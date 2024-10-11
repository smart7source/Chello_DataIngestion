import sys
from awsglue.utils import getResolvedOptions
from src.data_pipeline.bronze.raw_to_bronze import process_ingestion

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'raw_s3_path','conf_s3_location', 'conf_s3_file'])
job_id=args['JOB_NAME']
s3_location=args['raw_s3_path']
conf_s3_location=args['conf_s3_location']
conf_s3_file=args['conf_s3_file']


print("Data Ingestion - Start")
print("***********************************************")
process_ingestion(s3_location, job_id, conf_s3_location, conf_s3_file)
print("Data Ingestion - End ")
print("***********************************************")