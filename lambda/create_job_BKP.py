import boto3
import datetime
import json
import uuid

s3 = boto3.client("s3")
glue = boto3.client("glue")
job_datetime=datetime.datetime.now().strftime('%Y%m%d%H%M%S')


def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))
    # Get the object from the event and show its content type
    s3_bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    file_name = event["Records"][0]["s3"]["object"]["key"]
    s3_file_name=file_name
    balance_sheet="BalanceSheet"
    invoices="Invoices"
    profitAndLoss="profitAndLoss"
    payment="Payment"
    TPS_Claims="TPSClaims"
    TPSPayments="TPSPayments"
    supplier="Supplier"
    yaml_config_bucket='chello'
    yaml_config_file=''
    if file_name is not None and balance_sheet.lower() in file_name.lower():
        s3_file_name=balance_sheet
        yaml_config_file='ingestion/dq_config_file/bronze/balance_sheet.yaml'
    elif file_name is not None and TPS_Claims.lower() in file_name.lower():
        s3_file_name=TPS_Claims
        yaml_config_file='ingestion/dq_config_file/bronze/trizetto_claims.yaml'
    elif file_name is not None and TPSPayments.lower() in file_name.lower():
        s3_file_name=TPSPayments
        yaml_config_file='ingestion/dq_config_file/bronze/trizetto_payments.yaml'
    elif file_name is not None and profitAndLoss.lower() in file_name.lower():
        s3_file_name=profitAndLoss
        yaml_config_file='ingestion/dq_config_file/bronze/p_and_l.yaml'
    elif file_name is not None and invoices.lower() in file_name.lower():
        s3_file_name=invoices
        yaml_config_file='ingestion/dq_config_file/bronze/invoice.yaml'
    elif file_name is not None and payment.lower() in file_name.lower():
        s3_file_name=payment
        yaml_config_file='ingestion/dq_config_file/bronze/payments.yaml'
    elif file_name is not None and supplier.lower() in file_name.lower():
        s3_file_name=supplier
        yaml_config_file='ingestion/dq_config_file/bronze/supplier.yaml'
    else:
        print("Not found!")
    source_data="s3://"+s3_bucket_name+"/"+file_name
    uid = uuid.uuid4()
    job_name=s3_file_name+"_"+job_datetime+"_"+uid.hex
    script_location = f"s3://chello/ingestion/dataIngestion.py"
    if s3_file_name in (TPS_Claims , TPSPayments):
        script_location = f"s3://chello/ingestion/dataIngestion_tps.py"
        s3_file_name_split=(file_name.split('_'))
        job_name=s3_file_name_split[0]+'_'+s3_file_name_split[1]+"_"+job_datetime
    print(script_location)
    default_args = {
        "--extra-py-files": f"s3://chello/ingestion/external_library/src.zip,s3://chello/ingestion/external_library/PyMySQL-1.1.1-py3-none-any.whl",
        "--TempDir": f"s3://chello/temporary/"
    }
    glue_job = glue.create_job(
        Name=job_name,
        Role="lambda-full",
        Command={
            "Name": "glueetl",
            "ScriptLocation": script_location,
            "PythonVersion": "3",
        },
        DefaultArguments=default_args,
        Timeout=15,
        GlueVersion="4.0",
    )
    print("my_job:", glue_job)

    try:
        response = s3.get_object(Bucket=s3_bucket_name, Key=file_name)
        job_metadata = glue.start_job_run(JobName=glue_job["Name"], Arguments = {'--raw_s3_path' : source_data, '--conf_s3_location' : yaml_config_bucket, '--conf_s3_file' : yaml_config_file})
        status = glue.get_job_run(JobName=glue_job["Name"], RunId=job_metadata["JobRunId"])
        print(status["JobRun"]["JobRunState"])

        print("CONTENT TYPE: " + response["ContentType"])
        return response["ContentType"]
    except Exception as e:
        print(e)
        print(
            "Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.".format(
                s3_file_name, s3_bucket_name
            )
        )
        raise e
