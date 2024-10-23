import boto3
from botocore.exceptions import ClientError
import datetime
import json
import uuid

s3 = boto3.client("s3")
#glue = boto3.client("glue")
glue = boto3.client('glue', region_name='us-east-2')
job_datetime=datetime.datetime.now().strftime('%Y%m%d%H%M%S')

def check_or_create_glue_job(job_name):
    print("inside the check_or_create_glue_job function and job name is ", job_name)
    #session = boto3.session.Session()
    #glue_client = session.client('glue')
    try:
        response = glue_client.get_job(JobName=job_name)
        print("check glue job resp is ", response)
        print("Already Job exists in Glue for ",response['Job']['Name'])
        return response['Job']['Name']
    finally:
        print("job isn't exist and lambda is creating Glue job: ", job_name)

def create_glue_job_if_not_exists(job_name, script_location, default_args):
    glue_client = boto3.client('glue')
    try:
        # Check if the job already exists
        response = glue_client.get_job(JobName=job_name)
        print(f"Job '{job_name}' already exists.")
        return response['Job']

    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityNotFoundException':
            # Job does not exist, so create it
            try:
                response = glue_client.create_job(
                    Name=job_name,Role="lambda-full",
                    Command={"Name": "glueetl", "ScriptLocation": script_location,"PythonVersion": "3",},
                    ExecutionProperty={'MaxConcurrentRuns': 1000 },
                    DefaultArguments=default_args,Timeout=15,GlueVersion="4.0",)
                print(f"Created job '{job_name}' successfully.")
                return response
            except ClientError as create_err:
                print(f"Failed to create job: {create_err}")
        else:
            print(f"Unexpected error: {e}")
            raise

def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))
    # Get the object from the event and show its content type
    s3_bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    print("bucket name: ", s3_bucket_name)
    file_name = event["Records"][0]["s3"]["object"]["key"]
    s3_file_name=file_name
    print("S3 File Name: ", s3_file_name)
    balance_sheet="BalanceSheet"
    invoices="Invoices"
    profitAndLoss="profitAndLoss"
    paymnt="Paymnt"
    TPS_Claims="TPSClaims"
    TPSPayments="TPSPayments"
    supplier="Supplier"
    transaction="transaction"
    codat_bills="Codat_bills"
    codat_billPayments="codat_billPayments"
    yaml_config_bucket='chello'
    yaml_config_file=''
    print("s3 file name is ", s3_file_name)
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
    elif file_name is not None and paymnt.lower() in file_name.lower():
        s3_file_name=payment
        yaml_config_file='ingestion/dq_config_file/bronze/payments.yaml'
    elif file_name is not None and supplier.lower() in file_name.lower():
        s3_file_name=supplier
        yaml_config_file='ingestion/dq_config_file/bronze/supplier.yaml'
    elif file_name is not None and transaction.lower() in file_name.lower():
        s3_file_name=transaction
        yaml_config_file='ingestion/dq_config_file/bronze/Transactions.yaml'
    elif file_name is not None and codat_bills.lower() in file_name.lower():
        s3_file_name=codat_bills
        yaml_config_file='ingestion/dq_config_file/bronze/codat_bills.yaml'
    elif file_name is not None and codat_billPayments.lower() in file_name.lower():
        s3_file_name=codat_billPayments
        yaml_config_file='ingestion/dq_config_file/bronze/codat_billPayments.yaml'
    else:
        print("Not found!")
    source_data="s3://"+s3_bucket_name+"/"+file_name
    #uid = uuid.uuid4()
    uid = str(uuid.uuid4().hex)[:6]
    job_name=s3_file_name
    job_id=s3_file_name+"_"+job_datetime+"_"+uid
    script_location = f"s3://chello/ingestion/dataIngestion.py"
    if s3_file_name in (TPS_Claims , TPSPayments):
        script_location = f"s3://chello/ingestion/dataIngestion_tps.py"
        s3_file_name_split=(file_name.split('_'))
        #job_name=s3_file_name_split[0]+'_'+s3_file_name_split[1]+"_"+job_datetime
        #job_name=s3_file_name+"_"+job_datetime+"_"+uid
        #job_id=s3_file_name+"_"+job_datetime+"_"+uid
        #job_name=s3_file_name
        trigger_name=s3_file_name+"_"+job_datetime+"_"+uid
    print(script_location)
    default_args = {
        "--extra-py-files": f"s3://chello/ingestion/external_library/src.zip,s3://chello/ingestion/external_library/PyMySQL-1.1.1-py3-none-any.whl",
        "--TempDir": f"s3://chello/temporary/"}

    try:
        create_glue_job_if_not_exists(job_name, script_location, default_args)
        print("nextline to check_or_create_glue_job function call")
        response = s3.get_object(Bucket=s3_bucket_name, Key=file_name)
        job_metadata = glue.start_job_run(JobName=job_name, Arguments = {'--raw_s3_path' : source_data, '--conf_s3_location' : yaml_config_bucket, '--conf_s3_file' : yaml_config_file, '--job_id' : job_id})
        status = glue.get_job_run(JobName=job_name, RunId=job_metadata["JobRunId"])
        print("The Glue job status is ", status["JobRun"]["JobRunState"])
        print("The arguments passed are ", status["JobRun"]["Arguments"])

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

