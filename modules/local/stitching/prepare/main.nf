process STITCHING_PREPARE {
    label 'process_single'
    container 'multifish/biocontainers-spark:3.1.3'

    input:
    path(input_dir)
    path(output_dir)
    val(acq_name)

    output:
    tuple val(acq_name), val(acq_fullpath)

    script:
    abs_input_dir = input_dir.resolveSymLink().toString()
    abs_output_dir = output_dir.resolveSymLink().toString()
    acq_fullpath = "${abs_output_dir}/stitching/${acq_name}/"
    mvl = "${abs_input_dir}/${acq_name}.mvl"
    czi = "${abs_input_dir}/${acq_name}*.czi"
    """
    umask 0002
    mkdir -p $acq_fullpath
    ln -s $mvl $acq_fullpath || true
    ln -s $czi $acq_fullpath || true
    """
}
