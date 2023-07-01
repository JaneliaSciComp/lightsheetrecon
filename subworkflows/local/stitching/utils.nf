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
