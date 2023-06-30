include {
    get_spark_config_filepath;
    get_spark_master_log;
    get_terminate_file_name;
} from '../utils'

process SPARK_STARTMANAGER {
    container 'multifish/biocontainers-spark:3.1.3'
    label 'process_single'

    input:
    val(spark_local_dir)
    path(cluster_work_dir)

    output:
    val(cluster_work_fullpath)

    when:
    task.ext.when == null || task.ext.when

    script:
    args = task.ext.args ?: ''
    sleep_secs = task.ext.sleep_secs ?: '1'
    spark_config_filepath = get_spark_config_filepath(cluster_work_dir)
    spark_master_log_file = get_spark_master_log(cluster_work_dir)
    terminate_file_name = get_terminate_file_name(cluster_work_dir)
    container_engine = workflow.containerEngine
    cluster_work_fullpath = cluster_work_dir.resolveSymLink().toString()
    """
    /opt/scripts/startmanager.sh "$spark_local_dir" "$cluster_work_dir" "$spark_master_log_file" \
        "$spark_config_filepath" "$terminate_file_name" "$args" $sleep_secs $container_engine
    """
}
