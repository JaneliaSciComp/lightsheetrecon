task.ext.spark_container = "multifish/biocontainers-spark:3.1.3"

process SPARK_MASTER {
    label 'process_single'
    container "${task.ext.spark_container}"

    input:
    val spark_conf
    val spark_work_dir
    val terminate_name

    output:

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: ''
    def spark_master_log_file = get_spark_master_log(spark_work_dir)
    def spark_config_name = get_spark_config_name(spark_conf, spark_work_dir)
    def terminate_file_name = get_terminate_file_name(spark_work_dir, terminate_name)
    def create_spark_config
    def spark_config_env
    def spark_config_arg
    if (spark_config_name != '') {
        create_spark_config = create_default_spark_config(spark_config_name)
        spark_config_arg = "--properties-file ${spark_config_name}"
        spark_config_env = ""
    } else {
        create_spark_config = ""
        spark_config_arg = ""
        spark_config_env = "export SPARK_CONF_DIR=${spark_conf}"
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

    echo "Starting spark master - logging to ${spark_master_log_file}"
    ${check_session_id}

    rm -f ${spark_master_log_file} || true

    ${create_spark_config}
    ${spark_env}
    ${lookup_ip_script}

    ${task.ext.sparkLocation}/bin/spark-class org.apache.spark.deploy.master.Master \
    -h \$SPARK_LOCAL_IP \
    ${spark_config_arg} \
    &> ${spark_master_log_file} &
    spid=\$!

    ${wait_to_terminate('spid', terminate_file_name, spark_master_log_file)}
    """
}
