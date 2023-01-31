task.ext.spark_container = "multifish/biocontainers-spark:3.1.3"

process SPARK_PREPARE {
    label 'process_single'
    container "${task.ext.spark_container}"

    input:
    val spark_work_dir

    output:
    val spark_work_dir

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: ''
    log.debug "Spark local directory: ${params.spark_local_dir}"
    log.debug "Spark work directory: ${spark_work_dir}"

    """
    SPARK_VERSION=`ls /opt/spark/jars/spark-core* | sed -e "s/.\(.*\)-\(.*\)\.jar/\2/"`
    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        spark: ${task.ext.spark_version}
    END_VERSIONS

    if [[ ! -d "${spark_work_dir}" ]] ; then
        mkdir -p "${spark_work_dir}"
    else
        rm -f ${spark_work_dir}/* || true
    fi

    echo "Writing ${workflow.sessionId} to ${spark_work_dir}/.sessionId"
    echo "${workflow.sessionId}" > "${spark_work_dir}/.sessionId"
    """
}
