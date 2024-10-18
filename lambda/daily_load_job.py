import boto3
import datetime
import json
import uuid

s3 = boto3.client("s3")
glue = boto3.client("glue")
job_datetime=datetime.datetime.now().strftime('%Y%m%d%H%M%S')


def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))
    uid = uuid.uuid4()
    script_location = f"s3://chello/ingestion/dataIngestion_daily.py"
    yaml_config_bucket='chello'
    yaml_config_file='ingestion/dq_config_file/daily/daily_balance_sheet.yaml'
    job_name="Daily_Job_"+job_datetime+"_"+uid.hex
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
    print("default_arg ", default_args)
    print("args passed are  ", yaml_config_bucket+yaml_config_file)

    try:
        job_metadata = glue.start_job_run(JobName=glue_job["Name"], Arguments = {'--conf_s3_location' : yaml_config_bucket, '--conf_s3_file' : yaml_config_file})
        status = glue.get_job_run(JobName=glue_job["Name"], RunId=job_metadata["JobRunId"])
        print(status["JobRun"]["JobRunState"])
    except Exception as e:
        print(e)
        raise e
