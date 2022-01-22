from airflow.contrib.hooks.aws_sqs_hook import SQSHook

def sqs_poke():
    sqs_hook = SQSHook(aws_conn_id='aws_default')
    sqs_conn = sqs_hook.get_conn()
    sqs_queue = "sqs_url"

    path_list = []
    # sqs에 있는 모든 메세지를 불러오기 위해 while문 사용
    # MaxNumberOfMessages를 1보다 크게 넣으면 중복 메세지를 가져오는 경우도 있어 1로 설정
    while True:
        message_call = sqs_conn.receive_message(QueueUrl=sqs_queue, MaxNumberOfMessages=1, WaitTimeSeconds=1)
        if 'Messages' in message_call and len(message_call['Messages']) > 0:
            message = message_call['Messages']

            if message[0]['Body']:
                try:
                    body = json.loads(message[0]['Body'])
                    path_list.append(body['Records'][0]['s3']['object']['key'])
                except KeyError:
                    print("no records")

            delete_result = sqs_conn.delete_message_batch(
                    QueueUrl=sqs_queue,
                    Entries=[{'Id': message[0]['MessageId'], 'ReceiptHandle': message[0]['ReceiptHandle']}])

            if 'Successful' in delete_result:
                print("load and delete SQS success" + message[0]['MessageId'])
            else:
                raise AirflowException(
                    'Delete SQS Messages failed ' + str(delete_result) + ' for messages ' + str(message))
        else:
            break     