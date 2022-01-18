from airflow.contrib.hooks.aws_sqs_hook import SQSHook

def sqs_poke():
    sqs_hook = SQSHook(aws_conn_id='aws_default')
    sqs_conn = sqs_hook.get_conn()
    sqs_queue = "sqs_url"

    messages = sqs_conn.receive_message(QueueUrl=sqs_queue,
                                        MaxNumberOfMessages=10,
                                        WaitTimeSeconds=1,
                                        )
    print(messages)
    
 fb_sqs = PythonOperator(
    task_id = 'fb_sqs',
    python_callable=sqs_poke,
    dag=dag
)