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
    dag_id="crea-pipeline-test",
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
        region_name="ap-northeast-2"
    )

    secret_key = client.get_secret_value(
        SecretId="dev/aeskey"
    )
    secret_dict = ast.literal_eval(secret_key.get('SecretString'))

    aes_key = secret_dict.get("AES256Key")
    aes_key_utf = aes_key.encode('utf-8')


    iv = aes_key_utf
    pad = lambda s: s + (16 - len(s) % 16) * chr(16 - len(s) % 16)

    
    
    request = '''
        select
            access_tkn,
            app_secret
        from
            media_api.tb_md_api_info
        where
            md_nm = 'facebook'
            and clctr_ty = 'image'
            and use_at = true
    '''
    mysql_hook = MySqlHook(mysql_conn_id = 'api_info')
    df = mysql_hook.get_pandas_df(sql=request)
    aes = AES.new(aes_key_utf, AES.MODE_CBC, iv)
    enc_access_token = aes.encrypt(pad(df['access_tkn'].loc[0]).encode('utf-8'))
    tkn_byte64 = base64.b64encode(enc_access_token)
    final_tkn = tkn_byte64.decode('utf-8')
    print("access_tocken : {}".format(final_tkn))
    Variable.set("fb_access_token", final_tkn)

    print("formal_app_secret : {}".format(df['app_secret'].loc[0]))
    aes = AES.new(aes_key_utf, AES.MODE_CBC, iv)
    enc_app_secret = aes.encrypt(pad(df['app_secret'].loc[0]).encode('utf-8'))
    app_byte64 = base64.b64encode(enc_app_secret)
    final_app = app_byte64.decode('utf-8')
    print("app_secret : {}".format(final_app))
    Variable.set("fb_app_secret", final_app)

def fetch_records():
    request = '''
        select
            acnt_id
        from
            (select
                use_acnt.acnt_id,
            from
                media.pi_use_ty use_ty
            left join
                media.api_use_acnt use_acnt
            on
                (use_ty.acnt_id=use_acnt.acnt_id)
            where
                use_ty.use_at = 1
                and use_ty.api_ty = 'image'
                and use_acnt.use_at = 1 ) acnt
        '''
    mysql_hook = MySqlHook(mysql_conn_id = 'api_info')
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
                        {"name":"accessToken", "value":Variable.get('fb_access_token')},
                        {"name":"appSecret","value":Variable.get('fb_app_secret')},
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
                    'subnet-0wfdfacdaaef022ffa32c'
                ],
            'securityGroups': [
                    'sg-0afefagee7c76fd81676'
                ],
                'assignPublicIp': 'ENABLED'
            }
        },
        awslogs_group="/ecs/facebook-image-api",
        awslogs_region="ap-northeast-2",
        awslogs_stream_prefix="ecs/facebook-image-api",  # replace with your container name
    )
    image_collector_facebook_tasks.append(image_collector_facebook)

start_task >> fb_enc >> facebook_api_db_call >> image_collector_facebook_tasks >> end_task

