from datetime import datetime

JOB_START_TIME = datetime.now().strftime('%Y-%m-%dT%H-%M-%S-%f')
PARTITION_KEYS = ['import_year', 'import_month', 'import_day', 'import_date']
PARTITION_KEYS_SNAPSHOT = ['snapshot_year', 'snapshot_month', 'snapshot_day', 'snapshot_date']
