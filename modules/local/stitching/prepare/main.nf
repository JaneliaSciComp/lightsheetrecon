process STITCHING_PREPARE {
    label 'process_single'
    container 'multifish/biocontainers-spark:3.1.3'

    input:
    path(input_dir)
    path(output_dir)
    val(acq_name)

    output:
    tuple val(acq_name), path(acq_output_dir)

    script:
    acq_output_dir = "${output_dir}/${acq_name}"
    mvl = "${input_dir}/${acq_name}.mvl"
    mvl_link_dir = "${acq_output_dir}"
    czi = "${input_dir}/${acq_name}*.czi"
    czi_link_dir = "${acq_output_dir}"
    """
    umask 0002
    mkdir -p ${acq_output_dir}
    ln -s ${mvl} ${mvl_link_dir} || true
    ln -s ${czi} ${czi_link_dir} || true
    """
}
