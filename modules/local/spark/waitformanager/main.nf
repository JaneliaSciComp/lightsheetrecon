include {
    get_spark_master_log;
    get_terminate_file_name;
    create_check_session_id_script;
} from '../utils'

process SPARK_WAITFORMANAGER {
    label 'process_single'
    container 'multifish/biocontainers-spark:3.1.3'
    errorStrategy { task.exitStatus == 2
        ? 'retry' // retry on a timeout to prevent the case when the waiter is started before the master and master never gets its chance
        : 'terminate' }
    maxRetries 20

    input:
    val(spark_work_dir)

    output:
    tuple val(spark_work_dir), env(spark_uri)

    when:
    task.ext.when == null || task.ext.when

    shell:
    sleep_secs = task.ext.sleep_secs ?: '1'
    max_wait_secs = task.ext.max_wait_secs ?: '3600'
    spark_master_log_name = get_spark_master_log(spark_work_dir)
    terminate_file_name = get_terminate_file_name(spark_work_dir)
    template 'waitformanager.sh'
}
