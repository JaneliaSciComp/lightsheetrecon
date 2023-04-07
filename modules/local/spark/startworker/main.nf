include {
    get_terminate_file_name;
    get_spark_worker_log;
    get_spark_config_filepath;
} from '../utils'

process SPARK_STARTWORKER {
    container 'multifish/biocontainers-spark:3.1.3'
    cpus { worker_cores }
    // 1 GB of overhead for Spark, the rest for executors
    memory "${worker_mem_in_gb+1} GB"

    input:
    tuple val(spark_uri), path(spark_work_dir), val(worker_id)
    val(worker_cores)
    val(worker_mem_in_gb)

    output:
    tuple val(spark_uri), path(spark_work_dir), val(worker_id)

    when:
    task.ext.when == null || task.ext.when

    shell:
    args = task.ext.args ?: ''
    sleep_secs = task.ext.sleep_secs ?: '1'
    spark_worker_log_file = get_spark_worker_log(spark_work_dir, worker_id)
    spark_config_filepath = get_spark_config_filepath(spark_work_dir)
    terminate_file_name = get_terminate_file_name(spark_work_dir)
    template 'startworker.sh'
}
