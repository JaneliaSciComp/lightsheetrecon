
def get_spark_config_filepath(spark_work_dir) {
    return "${spark_work_dir}/spark-defaults.conf"
}

def get_spark_master_log(spark_work_dir) {
    return "${spark_work_dir}/sparkmaster.log"
}

def get_spark_worker_log(spark_work_dir, worker) {
    return "${spark_work_dir}/sparkworker-${worker}.log"
}

def get_spark_driver_log(spark_work_dir, log_name) {
    def log_file_name = log_name == null || log_name == "" ? "sparkdriver.log" : log_name
    return "${spark_work_dir}/${log_file_name}"
}

def get_terminate_file_name(spark_work_dir) {
    return "${spark_work_dir}/terminate-spark"
}

def create_spark_env(spark_work_dir, spark_env_extra) {
    return """
    export SPARK_ENV_LOADED=
    export SPARK_HOME=/opt/spark
    export PYSPARK_PYTHONPATH_SET=
    export PYTHONPATH="/opt/spark/python"
    export SPARK_LOG_DIR="${spark_work_dir}"
    ${spark_env_extra}
    set +u
    . "/opt/spark/sbin/spark-config.sh"
    . "/opt/spark/bin/load-spark-env.sh"
    set -u
    """
}

def create_default_spark_config(config_filepath, spark_local_dir) {
    def config_file = file(config_filepath)
    return """
        mkdir -p ${config_file.parent}
        cat <<EOF > ${config_filepath}
        spark.rpc.askTimeout=300s
        spark.storage.blockManagerHeartBeatMs=30000
        spark.rpc.retry.wait=30s
        spark.kryoserializer.buffer.max=1024m
        spark.core.connection.ack.wait.timeout=600s
        spark.driver.maxResultSize=0
        spark.worker.cleanup.enabled=true
        spark.local.dir=${spark_local_dir}
        EOF
        """.stripIndent()
}

def remove_log_file(log_file) {
    try {
        File f = new File(log_file)
        f.delete()
    }
    catch (Throwable t) {
        log.error "Problem deleting log file ${log_file}"
        t.printStackTrace()
    }
}

def create_lookup_ip_script() {
    if (workflow.containerEngine == "docker") {
        return lookup_ip_inside_docker_script()
    } else {
        return lookup_local_ip_script()
    }
}

def lookup_local_ip_script() {
    // Take the last IP that's listed by hostname -i.
    // This hack works on Janelia Cluster and AWS EC2.
    // It won't be necessary at all once we add a local option for Spark apps.
    """
    SPARK_LOCAL_IP=`hostname -i | rev | cut -d' ' -f1 | rev`
    echo "Use Spark IP: \$SPARK_LOCAL_IP"
    if [[ -z "\${SPARK_LOCAL_IP}" ]] ; then
        echo "Could not determine local IP: SPARK_LOCAL_IP is empty"
        exit 1
    fi
    """
}

def lookup_ip_inside_docker_script() {
    """
    SPARK_LOCAL_IP=
    for interface in /sys/class/net/{eth*,en*,em*}; do
        [ -e \$interface ] && \
        [ `cat \$interface/operstate` == "up" ] && \
        SPARK_LOCAL_IP=\$(ifconfig `basename \$interface` | grep "inet " | awk '\$1=="inet" {print \$2; exit}' | sed s/addr://g)
        if [[ "\$SPARK_LOCAL_IP" != "" ]]; then
            echo "Use Spark IP: \$SPARK_LOCAL_IP"
            break
        fi
    done
    if [[ -z "\${SPARK_LOCAL_IP}" ]] ; then
        echo "Could not determine local IP: SPARK_LOCAL_IP is empty"
        exit 1
    fi
    """
}

def create_write_session_id_script(spark_work_dir) {
    """
    echo "Writing ${workflow.sessionId} to ${spark_work_dir}/.sessionId"
    echo "${workflow.sessionId}" > "${spark_work_dir}/.sessionId"
    """
}

def create_check_session_id_script(spark_work_dir) {
    """
    SESSION_FILE="$spark_work_dir/.sessionId"
    echo "Checking for \$SESSION_FILE"
    SLEEP_SECS=${params.sleep_between_timeout_checks_seconds}
    MAX_WAIT_SECS=${params.wait_for_spark_timeout_seconds}
    SECONDS=0

    while ! test -e "\$SESSION_FILE"; do
        sleep \${SLEEP_SECS}
        if (( \${SECONDS} < \${MAX_WAIT_SECS} )); then
            echo "Waiting for \$SESSION_FILE"
            SECONDS=\$(( \${SECONDS} + \${SLEEP_SECS} ))
        else
            echo "-------------------------------------------------------------------------------"
            echo "ERROR: Timed out after \${SECONDS} seconds while waiting for \$SESSION_FILE    "
            echo "Make sure that your --spark_work_dir is accessible to all nodes in the cluster "
            echo "-------------------------------------------------------------------------------"
            exit 1
        fi
    done

    echo "Found \$SESSION_FILE in \$SECONDS s"
    # reset SECONDS for the next wait
    SECONDS=0

    if ! grep -F -x -q "${workflow.sessionId}" \$SESSION_FILE
    then
        echo "------------------------------------------------------------------------------"
        echo "ERROR: session id in \$SESSION_FILE does not match current session            "
        echo "Make sure that your --spark_work_dir is accessible to all nodes in the cluster"
        echo "and that you are not running multiple pipelines with the same --spark_work_dir"
        echo "------------------------------------------------------------------------------"
        exit 1
    fi
    """
}

def wait_to_terminate(pid_var, terminate_file_name, log_file) {
    """
    trap "kill -9 \$${pid_var} &>/dev/null" EXIT

    while true; do

        if ! kill -0 \$${pid_var} >/dev/null 2>&1; then
            echo "Process \$${pid_var} died"
            cat ${log_file} >&2
            exit 1
        fi

        if [[ -e "${terminate_file_name}" ]] ; then
            cat ${log_file}
            break
        fi

        sleep 1
    done
    """
}

def calc_executor_memory(cores, mem_per_core_in_gb) {
    return cores * mem_per_core_in_gb
}
