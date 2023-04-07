process SPARK_PREPARE {
    label 'process_single'
    container 'multifish/biocontainers-spark:3.1.3'
    scratch { spark_local_dir }

    input:
    // The parent spark dir and the dir name are passed separately so that parent
    // gets mounted and work dir can be created within it
    tuple val(spark_local_dir), path(spark_work_dir_parent), val(spark_work_dir_name)

    output:
    tuple val(spark_local_dir), path(spark_work_dir_parent), val(spark_work_dir_name)

    when:
    task.ext.when == null || task.ext.when

    shell:
    template 'prepare.sh'
}
