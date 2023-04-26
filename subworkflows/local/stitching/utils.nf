/**
 * Get the stitching ref channel, empty if "all" is provided.
 * Also extracts only the numeric part from the channel since that's
 * how the stitching pipeline expects it.
 */
def parse_stitching_channel(stitching_ref) {
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
        if (!StringUtils.isEmpty(data_dir))
            "${data_dir}/${it}${suffix}"
        else
            "${it}${suffix}"
    }
}

/**
 * Converts the original channel into another channel that contains a tuple of
 * the position of the element in the channel and element itself.
 * For example:
 * [e1, e2, e3, ..., en] -> [ [0, e1], [1, e2], [2, e3], ..., [n-1, en] ]
 *
 * This function is needed when we need to pair outputs from a process with other
 * inputs to be passed to another process because the asynchronous nature of the
 * process execution.
 */
def index_channel(c) {
    c.reduce([ 0, [] ]) { accum, elem ->
        def indexed_elem = [accum[0], elem]
        [ accum[0]+1, accum[1]+[indexed_elem] ]
    } | flatMap { it[1] }
}
