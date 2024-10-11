
INSERT_SQL="""insert into `job_tracking` (`job_id`,`job_description`,`total_record_count`,`load_record_count`,
                    `error_record_count`,`job_start_time`,`job_end_time`,
                    `source_path`, `dq_file_path`, `status`)
         values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
    """

UPDATE_SQL = """UPDATE job_tracking SET total_record_count=%s, load_record_count=%s, error_record_count=%s, job_end_time=%s, dq_file_path=%s, status=%s WHERE job_id=%s """
