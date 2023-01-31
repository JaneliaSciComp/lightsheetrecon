task.ext.spark_container = "multifish/biocontainers-spark:3.1.3"

process SPARK_WORKER {
    label 'process_single'
    container "${task.ext.spark_container}"
    cpus { task.ext.worker_cores }
    // 1 GB of overhead for Spark, the rest for executors
    memory "${task.ext.worker_mem_in_gb+1} GB"

    input:
    val spark_master_uri
    val worker_id

    output:

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: ''

    def terminate_file_name = get_terminate_file_name(spark_work_dir, terminate_name)
    def spark_worker_log_file = get_spark_worker_log(spark_work_dir, worker_id)
    def spark_config_name = get_spark_config_name(spark_conf, spark_work_dir)
    def spark_config_env
    def spark_config_arg
    def spark_worker_opts_list = []
    spark_worker_opts_list << "spark.worker.cleanup.enabled=true"
    spark_worker_opts_list << "spark.worker.cleanup.interval=30"
    spark_worker_opts_list << "spark.worker.cleanup.appDataTtl=1"
    spark_worker_opts_list << "spark.port.maxRetries=${params.max_connect_retries}"
    def spark_worker_opts_string = spark_worker_opts_list.inject('') {
        arg, item -> "${arg} -D${item}"
    }
    def spark_worker_opts="export SPARK_WORKER_OPTS=\"${spark_worker_opts_string}\""
    if (spark_config_name != '') {
        spark_config_arg = "--properties-file ${spark_config_name}"
        spark_config_env = """
        ${spark_worker_opts}
        """
    } else {
        spark_config_arg = ""
        spark_config_env = """
        ${spark_worker_opts}
        export SPARK_CONF_DIR=${spark_conf}
        """
    }

    def spark_env = create_spark_env(spark_work_dir, spark_config_env, task.ext.sparkLocation)
    def lookup_ip_script = create_lookup_ip_script()
    def check_session_id = create_check_session_id_script(spark_work_dir)
    """
    SPARK_VERSION=`ls /opt/spark/jars/spark-core* | sed -e "s/.\(.*\)-\(.*\)\.jar/\2/"`
    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        spark: ${task.ext.spark_version}
    END_VERSIONS

    echo "Starting spark worker ${worker_id} - logging to ${spark_worker_log_file}"
    ${check_session_id}

    rm -f ${spark_worker_log_file} || true
    ${spark_env}
    ${lookup_ip_script}

    echo "\
    ${task.ext.sparkLocation}/bin/spark-class org.apache.spark.deploy.worker.Worker \
    ${spark_master_uri} \
    -c ${worker_cores} \
    -m ${worker_mem_in_gb}G \
    -d ${spark_work_dir} \
    -h \$SPARK_LOCAL_IP \
    ${spark_config_arg} \
    "

    ${task.ext.sparkLocation}/bin/spark-class org.apache.spark.deploy.worker.Worker \
    ${spark_master_uri} \
    -c ${worker_cores} \
    -m ${worker_mem_in_gb}G \
    -d ${spark_work_dir} \
    -h \$SPARK_LOCAL_IP \
    ${spark_config_arg} \
    &> ${spark_worker_log_file} &
    spid=\$!
    ${wait_to_terminate('spid', terminate_file_name, spark_worker_log_file)}
    """
}

def get_terminate_file_name(working_dir, terminate_name) {
    return terminate_name == null || terminate_name == ''
        ? "${working_dir}/terminate-spark"
        : "${working_dir}/${terminate_name}"
}

def get_spark_worker_log(spark_work_dir, worker) {
    return "${spark_work_dir}/sparkworker-${worker}.log"
}


