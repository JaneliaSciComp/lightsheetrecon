
process SPARK_PREPARE {
    label 'process_single'
    container "${task.ext.spark_container}"

    input:
    val(input_dir)
    val(output_dir)
    val(acq_name)
    val(stitching_output)

    output:
    tuple val(acq_name), val(stitching_dir)

    script:
    acq_output_dir = "${output_dir}/${acq_name}"
    stitching_dir = stitching_output == ''
        ? acq_output_dir
        : "${acq_output_dir}/${stitching_output}"
    mvl = "${input_dir}/${acq_name}.mvl"
    mvl_link_dir = "${stitching_dir}"
    czi = "${input_dir}/${acq_name}*.czi"
    czi_link_dir = "${stitching_dir}"
    """
    umask 0002
    mkdir -p ${stitching_dir}
    ln -s ${mvl} ${mvl_link_dir} || true
    ln -s ${czi} ${czi_link_dir} || true
    """
}

/**
 * Get the stitching ref channel, empty if "all" is provided.
 * Also extracts only the numeric part from the channel since that's
 * how the stitching pipeline expects it.
 */
def stitching_ref_param(stitching_ref) {
    if (stitching_ref=="all") {
        return ''
    }
    def ch_num_lookup = (stitching_ref =~ /(\d+)/)
    if (ch_num_lookup.find()) {
        return ch_num_lookup[0][1]
    } else {
        return ''
    }
}

def entries_inputs_args(data_dir, entries, flag, suffix, ext) {
    entries_inputs(data_dir, entries, "${suffix}${ext}")
        .inject('') {
            arg, item -> "${arg} ${flag} ${item}"
        }
}

def entries_inputs(data_dir, entries, suffix) {
    return entries.collect {
        if (data_dir != null && data_dir != '')
            "${data_dir}/${it}${suffix}"
        else
            "${it}${suffix}"
    }
}

def read_config(cf) {
    jsonSlurper = new groovy.json.JsonSlurper()
    return jsonSlurper.parse(cf)
}

def write_config(data, cf) {
    json_str = groovy.json.JsonOutput.toJson(data)
    json_beauty = groovy.json.JsonOutput.prettyPrint(json_str)
    cf.write(json_beauty)
}

/**
 * index_channel converts the original channel into
 * another channel that contains a tuple of with
 * the position of the element in the channel and element itself.
 * For example:
 * [e1, e2, e3, ..., en] -> [ [0, e1], [1, e2], [2, e3], ..., [n-1, en] ]
 *
 * This function is needed when we need to pair outputs from process, let's say P1,
 * with other inputs to be passed to another process, P2  in the pipeline,
 * because the asynchronous nature of the process execution
 */
def index_channel(c) {
    c.reduce([ 0, [] ]) { accum, elem ->
        def indexed_elem = [accum[0], elem]
        [ accum[0]+1, accum[1]+[indexed_elem] ]
    } | flatMap { it[1] }
}
