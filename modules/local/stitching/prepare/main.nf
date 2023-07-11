process STITCHING_PREPARE {
    tag "${meta.id}"
    label 'process_single'
    container 'multifish/biocontainers-spark:3.1.3'

    input:
    tuple val(meta), val(files)
    path(input_dir)
    path(output_dir)

    output:
    tuple val(meta), val(files), emit: acquisitions

    script:
    abs_output_dir = output_dir.resolveSymLink().toString()
    meta.stitching_dir = "${abs_output_dir}/stitching/${meta.id}"
    meta.spark_work_dir = "${abs_output_dir}/spark/${workflow.sessionId}/${meta.id}"
    """
    umask 0002
    mkdir -p ${meta.stitching_dir}
    mkdir -p ${meta.spark_work_dir}
    """
}
