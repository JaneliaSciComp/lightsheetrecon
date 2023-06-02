
def get_spark_config_filepath(spark_work_dir) {
    return "${spark_work_dir}/spark-defaults.conf"
}

def get_spark_master_log(spark_work_dir) {
    return "${spark_work_dir}/sparkmaster.log"
}

def get_spark_worker_log(spark_work_dir, worker) {
    return "${spark_work_dir}/sparkworker-${worker}.log"
}

def get_terminate_file_name(spark_work_dir) {
    return "${spark_work_dir}/terminate-spark"
}
