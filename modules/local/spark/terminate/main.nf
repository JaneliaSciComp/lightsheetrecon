include {
    get_terminate_file_name;
    create_check_session_id_script;
} from '../utils'

process SPARK_TERMINATE {
    label 'process_single'
    container 'multifish/biocontainers-spark:3.1.3'

    input:
    val(spark_work_dir)

    output:
    val(spark_work_dir)

    script:
    terminate_file_name = get_terminate_file_name(spark_work_dir)
    check_session_id = create_check_session_id_script(spark_work_dir)
    """
    ${check_session_id}

    echo "DONE" > ${terminate_file_name}
    cat ${terminate_file_name}
    """
}
