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

    script:
    def args = task.ext.args ?: ''
    def spark_master_log_name = get_spark_master_log(spark_work_dir)
    def terminate_file_name = get_terminate_file_name(spark_work_dir)
    def check_session_id_script = create_check_session_id_script(spark_work_dir)
    """
    ${check_session_id_script}

    while true; do

        if [[ -e ${spark_master_log_name} ]]; then
            test_uri=`grep -o "\\(spark://.*\$\\)" ${spark_master_log_name} || true`
            if [[ ! -z \${test_uri} ]]; then
                echo "Spark master started at \${test_uri}"
                break
            fi
        fi

        if [[ -e "${terminate_file_name}" ]]; then
            echo "Terminate file ${terminate_file_name} found"
            exit 1
        fi

        if (( \${SECONDS} > \${MAX_WAIT_SECS} )); then
            echo "Timed out after \${SECONDS} seconds while waiting for spark master <- ${spark_master_log_name}"
            cat ${spark_master_log_name} >&2
            exit 2
        fi

        sleep \${SLEEP_SECS}
        SECONDS=\$(( \${SECONDS} + \${SLEEP_SECS} ))


    done
    spark_uri=\${test_uri}
    """
}
