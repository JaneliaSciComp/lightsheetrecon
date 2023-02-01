include {
    get_spark_master_log;
    get_spark_config_filepath;
    get_terminate_file_name;
    create_default_spark_config;
    create_spark_env;
    create_lookup_ip_script;
    create_check_session_id_script;
    wait_to_terminate;
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

    script:
    def args = task.ext.args ?: ''
    def spark_config_filepath = get_spark_config_filepath(spark_work_dir)
    def spark_master_log_file = get_spark_master_log(spark_work_dir)
    def terminate_file_name = get_terminate_file_name(spark_work_dir)
    def create_spark_config_script = create_default_spark_config(spark_config_filepath, spark_local_dir)
    def spark_env_script = create_spark_env(spark_work_dir, '')
    def lookup_ip_script = create_lookup_ip_script()
    def check_session_id_script = create_check_session_id_script(spark_work_dir)
    """
    echo "Starting spark master - logging to ${spark_master_log_file}"
    ${check_session_id_script}

    rm -f ${spark_master_log_file} || true

    ${create_spark_config_script}
    ${spark_env_script}
    ${lookup_ip_script}

    /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master \
    -h \$SPARK_LOCAL_IP \
    --properties-file ${spark_config_filepath} \
    &> ${spark_master_log_file} &
    spid=\$!

    ${wait_to_terminate('spid', terminate_file_name, spark_master_log_file)}
    """
}
