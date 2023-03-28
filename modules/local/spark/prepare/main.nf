process SPARK_PREPARE {
    label 'process_single'
    container 'multifish/biocontainers-spark:3.1.3'

    input:
    tuple val(spark_local_dir), val(spark_work_dir)

    output:
    tuple val(spark_local_dir), val(spark_work_dir)

    when:
    task.ext.when == null || task.ext.when

    shell:
    def args = task.ext.args ?: ''
    log.debug "Spark local directory: ${spark_local_dir}"
    log.debug "Spark work directory: ${spark_work_dir}"
    template 'prepare.sh'
}
