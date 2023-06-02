include {
    get_terminate_file_name;
} from '../utils'

process SPARK_TERMINATE {
    label 'process_single'
    container 'multifish/biocontainers-spark:3.1.3'

    input:
    tuple val(spark_uri), path(cluster_work_dir)

    output:
    tuple val(spark_uri), val(cluster_work_fullpath)

    script:
    cluster_work_fullpath = cluster_work_dir.resolveSymLink().toString()
    terminate_file_name = get_terminate_file_name(cluster_work_dir)
    """
    /opt/scripts/terminate.sh "$terminate_file_name"
    """
}
