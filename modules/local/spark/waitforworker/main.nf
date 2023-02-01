include {
    get_terminate_file_name;
    get_spark_worker_log;
    create_check_session_id_script;
} from '../utils'

process SPARK_WAITFORWORKER {
    label 'process_single'
    container 'multifish/biocontainers-spark:3.1.3'
    errorStrategy { task.exitStatus == 2
        ? 'retry' // retry on a timeout to prevent the case when the waiter is started before the worker and the worker never gets its chance
        : 'terminate' }
    maxRetries 20

    input:
    tuple val(spark_work_dir), val(spark_uri), val(worker_id)

    output:
    tuple val(spark_work_dir), val(spark_uri), val(worker_id)

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: ''
    def terminate_file_name = get_terminate_file_name(spark_work_dir)
    def spark_worker_log_file = get_spark_worker_log(spark_work_dir, worker_id)
    def check_session_id_script = create_check_session_id_script(spark_work_dir)
    """
    ${check_session_id_script}

    while true; do

        if [[ -e "${spark_worker_log_file}" ]]; then
            found=`grep -o "\\(Worker: Successfully registered with master ${spark_uri}\\)" ${spark_worker_log_file} || true`

            if [[ ! -z \${found} ]]; then
                echo "\${found}"
                break
            fi
        fi

        if [[ -e "${terminate_file_name}" ]]; then
            echo "Terminate file ${terminate_file_name} found"
            exit 1
        fi

        if (( \${SECONDS} > \${MAX_WAIT_SECS} )); then
            echo "Spark worker ${worker_id} timed out after \${SECONDS} seconds while waiting for master ${spark_uri}"
            cat ${spark_worker_log_file} >&2
            exit 2
        fi

        sleep \${SLEEP_SECS}
        SECONDS=\$(( \${SECONDS} + \${SLEEP_SECS} ))

    done
    """
}
