import sys
from awsglue.utils import getResolvedOptions
from src.data_pipeline.daily.daily_load import process_daily_load

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'conf_s3_location', 'conf_s3_file'])
job_id=args['JOB_NAME']
s3_location=args['raw_s3_path']
conf_s3_location=args['conf_s3_location']
conf_s3_file=args['conf_s3_file']


print("Daily data ingestion - Start")
print("***********************************************")
process_daily_load(job_id, conf_s3_location, conf_s3_file)
print("Daily data ingestion - END")
print("***********************************************")
