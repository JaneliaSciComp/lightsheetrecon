include {
    get_terminate_file_name;
} from '../utils'

process SPARK_TERMINATE {
    label 'process_single'
    container 'multifish/biocontainers-spark:3.1.3'

    input:
    tuple val(spark_uri), path(spark_work_dir)

    output:
    tuple val(spark_uri), path(spark_work_dir)

    script:
    terminate_file_name = get_terminate_file_name(spark_work_dir)
    """
    echo "DONE" > ${terminate_file_name}
    cat ${terminate_file_name}
    """
}
