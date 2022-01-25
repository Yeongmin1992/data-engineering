from datetime import datetime
import os
from airflow.models.variable import Variable
from airflow import DAG
from airflow.contrib.operators.ecs_operator import ECSOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.helpers import chain
from Cryptodome.Cipher import AES
import boto3
import ast
import base64

default_args = {
    'start_date': datetime(2021, 1, 1),
}

dag = DAG(
    dag_id="pipeline-test",
    default_args=default_args,
    schedule_interval='01 22  * * *',
    tags=["image"],
    catchup=False
)

start_task = DummyOperator(task_id='start', dag=dag)
end_task = DummyOperator(task_id='end', dag=dag)

# generate dag documentation
dag.doc_md = __doc__

def aes_key():
    session = boto3.session.Session()
    client = session.client(
        service_name="secretsmanager",
        region_name="ap-southeast-2"
    )

    secret_key = client.get_secret_value(
        SecretId="dev/aeskey"
    )
    secret_dict = ast.literal_eval(secret_key.get('secret'))

    aes_key = secret_dict.get("AESKey")
    aes_key_utf = aes_key.encode('utf-8')


    iv = aes_key_utf
    pad = lambda s: s + (16 - len(s) % 16) * chr(16 - len(s) % 16)

    request = '''
        select
            refresh_tkn,
            clnt_id,
            end_point
        from
            media.api
        where
            md = 'kakao'
    '''
    mysql_hook = MySqlHook(mysql_conn_id = 'api')
    df = mysql_hook.get_pandas_df(sql=request)

    # refresh token 암호화 및 airflow 변수 set
    aes = AES.new(aes_key_utf, AES.MODE, iv)
    refresh_tkn = df['refresh_tkn'].loc[0]
    enc_refresh_token = aes.encrypt(pad(refresh_tkn).encode('utf-8'))
    tkn_byte64 = base64.b64encode(enc_refresh_token)
    final_tkn = tkn_byte64.decode('utf-8')
    print("aes_refresh_tocken : {}".format(final_tkn))
    Variable.set("kakao_refresh_token", final_tkn)

    # refresh token으로 access token 발급
    url = "https://kauth.kakao.com/oauth/token"

    body = {
        "grant_type" : "refresh_token",
        "client_id" : clnt_id,
        "refresh_token" : refresh_tkn,
    }

    res = requests.post(url=url, data=body)

    # response에 refresh token이 있는 경우 refresh token 업데이트
    if("refresh_token" in res):
        update_sql='''
        update
            media.api
        set
            refresh_tkn = {}
        where
            md = 'kakao'           
        '''.format(res["refresh_token"])

        mysql_hook.run(update_sql)
        print("update access token success")

    access_tkn = res["access_token"]

def fetch_records():
    request = '''
        select
            acnt_id
        from
            (select
                use_acnt.acnt_id,
            from
                media.api_use_ty use_ty
            left join
                media.api_use_acnt use_acnt
            on
                (use_ty.acnt_id=use_acnt.acnt_id)
            where
                use_ty.use_at = 1
                ) acnt
        '''
    mysql_hook = MySqlHook(mysql_conn_id = 'api')
    df = mysql_hook.get_pandas_df(sql=request)]
    js = df.to_json(orient='records')
    Variable.set("fb_images", js)

    print("query json result : {}".format(js))


facebook_api_db_call = PythonOperator(
    task_id = 'facebook_api_db_call',
    python_callable = fetch_records,
    dag = dag
)

fb_enc = PythonOperator(
    task_id = 'fb_enc',
    python_callable=aes_key,
    dag=dag
)

image_collector_facebook_tasks = []
fb_accounts = Variable.get("fb_images", deserialize_json=True)
for val in fb_accounts:
    image_collector_facebook = ECSOperator(
        task_id="image_collector_{}".format(val.get('acnt_id')),
        dag=dag,
        cluster="Collector-Batch",
        task_definition="facebook-image-api",
        launch_type="FARGATE",
        overrides={
            "containerOverrides": [
                {
                    "name": "facebook-image-api",
                    "environment": [
                        {"name":"accountId","value":val.get('acnt_id')},
                        {"name":"SPRING_PROFILES_ACTIVE","value":"dev"},
                        {"name":"agoDate","value":"0"}
                    ],
                },  
            ],
        },
        network_configuration={
            'awsvpcConfiguration': {
                'subnets': [
                    'subnet-0wfdfacdaaef022ffatjd32c'
                ],
            'securityGroups': [
                    'sg-0afefagee7c7tjdrdj6fd81676'
                ],
                'assignPublicIp': 'ENABLED'
            }
        },
        awslogs_group="/ecs/facebook-image-api",
        awslogs_region="ap-southeast-2",
        awslogs_stream_prefix="ecs/facebook-image-api",  # replace with your container name
    )
    image_collector_facebook_tasks.append(image_collector_facebook)

start_task >> fb_enc >> facebook_api_db_call >> image_collector_facebook_tasks >> end_task

