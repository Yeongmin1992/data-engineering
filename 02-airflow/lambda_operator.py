from airflow.providers.amazon.aws.hooks.lambda_function import AwsLambdaHook

def call_lambda(event):

    json_event = json.dumps({"paths":event})
    print(json_event)

    hook = AwsLambdaHook(function_name="image_downloader",
                      qualifier='$LATEST',
                      config=None,
                      aws_conn_id='aws_default')
    response = hook.invoke_lambda(payload=json_event)
    print ('Lambada Response--->' , response)

# dynamic task
flambda_tasks = []
lambda_events = Variable.get("result_path_by_accounts", deserialize_json=True)
for account in lambda_events:
    lambda_task = PythonOperator(
        task_id = "lambda-{}".format(account),
        op_kwargs={"event": lambda_events[account]},
        python_callable=call_lambda,
        dag=dag
    )
    fb_lambda_tasks.append(lambda_task)