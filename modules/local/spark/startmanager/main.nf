include {
    get_spark_config_filepath;
    get_spark_master_log;
    get_terminate_file_name;
} from '../utils'

process SPARK_STARTMANAGER {
    label 'process_single'
    container 'multifish/biocontainers-spark:3.1.3'

    input:
    tuple val(spark_work_dir), val(spark_local_dir)

    output:
    tuple val(spark_work_dir), val(spark_local_dir)

    when:
    task.ext.when == null || task.ext.when

    shell:
    args = task.ext.args ?: ''
    spark_config_filepath = get_spark_config_filepath(spark_work_dir)
    spark_master_log_file = get_spark_master_log(spark_work_dir)
    terminate_file_name = get_terminate_file_name(spark_work_dir)
    template 'startmanager.sh'
}
