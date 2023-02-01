include {
    get_terminate_file_name;
    get_spark_worker_log;
    get_spark_config_filepath;
    create_spark_env;
    create_lookup_ip_script;
    create_check_session_id_script;
    wait_to_terminate;
} from '../utils'

process SPARK_STARTWORKER {
    container 'multifish/biocontainers-spark:3.1.3'
    cpus { worker_cores }
    // 1 GB of overhead for Spark, the rest for executors
    memory "${worker_mem_in_gb+1} GB"

    input:
    tuple val(spark_work_dir), val(spark_uri), val(worker_id)
    val(worker_cores)
    val(worker_mem_in_gb)

    output:
    tuple val(spark_work_dir), val(spark_uri), val(worker_id)

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: ''
    def terminate_file_name = get_terminate_file_name(spark_work_dir)
    def spark_worker_log_file = get_spark_worker_log(spark_work_dir, worker_id)
    def spark_config_filepath = get_spark_config_filepath(spark_work_dir)
    def spark_worker_opts_list = []
    spark_worker_opts_list << "spark.worker.cleanup.enabled=true"
    spark_worker_opts_list << "spark.worker.cleanup.interval=30"
    spark_worker_opts_list << "spark.worker.cleanup.appDataTtl=1"
    spark_worker_opts_list << "spark.port.maxRetries=${params.spark_max_connect_retries}"
    def spark_worker_opts_string = spark_worker_opts_list.inject('') {
        arg, item -> "${arg} -D${item}"
    }
    def spark_worker_opts="export SPARK_WORKER_OPTS=\"${spark_worker_opts_string}\""
    def spark_env_script = create_spark_env(spark_work_dir, spark_worker_opts)
    def lookup_ip_script = create_lookup_ip_script()
    def check_session_id_script = create_check_session_id_script(spark_work_dir)
    """
    echo "Starting spark worker ${worker_id} - logging to ${spark_worker_log_file}"
    ${check_session_id_script}

    rm -f ${spark_worker_log_file} || true

    ${spark_env_script}
    ${lookup_ip_script}

    echo "\
    /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
    ${spark_uri} \
    -c ${worker_cores} \
    -m ${worker_mem_in_gb}G \
    -d ${spark_work_dir} \
    -h \$SPARK_LOCAL_IP \
    --properties-file ${spark_config_filepath} \
    "

    /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
    ${spark_uri} \
    -c ${worker_cores} \
    -m ${worker_mem_in_gb}G \
    -d ${spark_work_dir} \
    -h \$SPARK_LOCAL_IP \
    --properties-file ${spark_config_filepath} \
    &> ${spark_worker_log_file} &
    spid=\$!
    ${wait_to_terminate('spid', terminate_file_name, spark_worker_log_file)}
    """
}
