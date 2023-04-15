include {
    get_terminate_file_name;
    get_spark_worker_log;
    get_spark_config_filepath;
} from '../utils'

process SPARK_STARTWORKER {
    container 'multifish/biocontainers-spark:3.1.3'
    // scratch { spark_local_dir }
    cpus { worker_cores }
    // 1 GB of overhead for Spark, the rest for executors
    memory "${worker_mem_in_gb+1} GB"

    input:
    val(spark_local_dir)
    tuple val(spark_uri), path(cluster_work_dir), val(worker_id)
    path(input_dir)
    path(output_dir)
    val(worker_cores)
    val(worker_mem_in_gb)

    output:
    val(spark_uri)

    when:
    task.ext.when == null || task.ext.when

    shell:
    args = task.ext.args ?: ''
    sleep_secs = task.ext.sleep_secs ?: '1'
    spark_worker_log_file = get_spark_worker_log(cluster_work_dir, worker_id)
    spark_config_filepath = get_spark_config_filepath(cluster_work_dir)
    terminate_file_name = get_terminate_file_name(cluster_work_dir)
    template 'startworker.sh'
}
