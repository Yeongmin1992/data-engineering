from datetime import datetime
import json
from airflow import DAG
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from pandas import json_normalize

default_args = {
    'start_date': datetime(2021, 1, 1),
}

def _processing_nft(ti):
    # task instance에 tasks_id에 해당하는 결과값 가져오기 
    assets = ti.xcom_pull(task_ids=['extract_nft'])
    asstets = None
    if not len(assets):
        raise ValueError("assets is empty")
    nft = assets[0]['assets'][0]

    processed_nft = json_normalize({
        'token_id': nft['token_id'],
        'name': nft['name'],
        'image_url': nft['image_url'],
    })
    processed_nft.to_csv('/tmp/processed_nft.csv', index=None, header=False)


with DAG(dag_id='nft-pipeline',
         schedule_interval='@daily',
         default_args=default_args,
         # 찾기 쉽도록 tag 사용
         tags=['nft'],
         # dag를 멈췄다가 다시돌리면 catchup이 false일 경우 당일 날짜의 dag만 돌리고, true로 하면 start_date 이후 멈춘날 부터 당일날 까지의 dag를 돌리게 됨 
         catchup=False) as dag:
    
    #테이블을 만드는 task
    #test : airflow tasks test nft-pipeline creating_table 2021-01-01
    creating_table = SqliteOperator(
        task_id='creating_table',
        sqlite_conn_id='db_sqlite',
        sql='''
            CREATE TABLE IF NOT EXISTS nfts (
                token_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                image_url TEXT NOT NULL
            )
        '''
    )

    # 해당 api가 존재하는지 확인하는 http sensor
    # test : airflow tasks test nft-pipeline is_api_available 2021-01-01
    is_api_available = HttpSensor(
        task_id='is_api_available',
        http_conn_id='opensea_api',
        endpoint='api/v1/assets?collection=doodles-official&limit=1'
    )

    # http에서 데이터를 가져와서 추출하기
    extract_nft = SimpleHttpOperator(
        task_id='extract_nft',
        http_conn_id='opensea_api',
        endpoint='api/v1/assets?collection=doodles-official&limit=1',
        method='GET',
        # json을 pythons으로 변경
        response_filter=lambda res: json.loads(res.text),
        log_response=True
    )

    # 위에서 불러온 데이터를 가공하기
    process_nft = PythonOperator(
        task_id='process_nft',
        python_callable=_processing_nft
    )

    # bash 커맨드를 입력하여 커맨드라인 실행
    store_nft = BashOperator(
        task_id='store_nft',
        # csv를 쪼개서 aiflow.db에 있는 nfts table로 import 하겠다.
        bash_command='echo -e ".separator ","\n.import /tmp/processed_nft.csv nfts" | sqlite3 ~/airflow/airflow.db'
    )

    #의존성 만들기
    creating_table >> is_api_available >> extract_nft >> process_nft >> store_nft


