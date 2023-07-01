include { SPARK_START       } from '../../../subworkflows/local/spark/start/main'
include { SPARK_TERMINATE   } from '../../../modules/local/spark/terminate/main'
include { STITCHING_PREPARE } from '../../../modules/local/stitching/prepare/main'

include {
    STITCHING_PARSECZI;
} from '../../../modules/local/stitching/parseczi/main'

include {
    SPARK_RUNAPP as CZI2N5;
    SPARK_RUNAPP as FLATFIELD_CORRECTION;
    SPARK_RUNAPP as RETILE;
    SPARK_RUNAPP as STITCHING;
    SPARK_RUNAPP as FUSE;
} from '../../../modules/local/spark/runapp/main'

include {
    parse_stitching_channel;
    entries_inputs_args;
} from './utils'

/**
 * prepares the work and output directories and invoke stitching
 * for the aquisitions list - this is a value channel containing a list of acquistins
 *
 * @return tuple of <acq_name, acq_stitching_dir>
 */
workflow STITCH {
    take:
    acquisitions
    input_dir
    output_dir
    channels
    resolution
    axis_mapping
    stitching_block_size
    final_block_size_xy
    stitching_channel
    stitching_mode
    stitching_padding
    stitching_blur_sigma
    flatfield_correction
    spark_cluster
    spark_workers
    spark_worker_cores
    spark_gb_per_core
    spark_driver_cores
    spark_driver_memory

    main:
    ch_versions = Channel.empty()
    stitching_app_container = params.stitching_app_container

    spark_work_dir = "${output_dir}/spark/${workflow.sessionId}"
    if (!file(spark_work_dir).mkdirs()) {
        error "Could not create Spark working dir at $spark_work_dir"
    }
    // tuple: [acq_name, acq_stitching_output_dir]
    STITCHING_PREPARE(
        acquisitions,
        input_dir,
        output_dir,
    )
    // map: {acq_names:channel, stitching_dirs:channel, spark_work_dirs:channel}
    STITCHING_PREPARE.out.acquisitions.subscribe {
        meta = it[0]
        filenames = it[1]
        log.debug "Create stitching dir for ${meta.id}: ${meta.stitching_dir}"
    }

    // spark_local_dir = "/tmp/spark-${workflow.sessionId}"

    // def spark_cluster_res = acq_inputs.spark_work_dirs.map { [ 'local[*]', it ] }
    // driver_cores = spark_driver_cores
    // driver_memory = spark_driver_memory
    // workers = spark_workers
    // if (spark_cluster) {
    //     spark_cluster_res = SPARK_START(
    //         spark_local_dir,
    //         acq_inputs.spark_work_dirs,
    //         input_dir,
    //         output_dir,
    //         workers,
    //         spark_worker_cores,
    //         spark_gb_per_core
    //     )
    // }
    // else {
    //     // When running locally, the driver needs enough resources to run a spark worker
    //     workers = 1
    //     driver_cores = driver_cores + spark_worker_cores
    //     driver_memory = (2 + spark_worker_cores * spark_gb_per_core) + " GB"
    // }
    // log.debug "Setting driver_cores: $driver_cores"
    // log.debug "Setting driver_memory: $driver_memory"

    // // print spark cluster result
    // // channel: [ spark_uri, spark_work_dir ]

    // def indexed_spark_uris = spark_cluster_res
    //     .join(indexed_spark_work_dirs, by:1)
    //     .map {
    //         def indexed_uri = [ it[2], it[1] ]
    //         log.debug "Create indexed spark URI from $it -> ${indexed_uri}"
    //         return indexed_uri
    //     }

    // // TODO: get this from meta
    // def stitching_czi_pattern = "TODO"
    // // prepare parse czi tiles
    // parseczi_args = prepare_app_args(
    //     "parseczi",
    //     indexed_spark_work_dirs, // to start the chain we just need a tuple that has the working dir as the 2nd element
    //     indexed_spark_work_dirs,
    //     indexed_acq_data,
    //     { acq_name, stitching_dir ->
    //         def mvl_inputs = entries_inputs_args(stitching_dir, [ acq_name ], '-i', '', '.mvl')
    //         def czi_inputs = entries_inputs_args('', [ acq_name ], '-f', stitching_czi_pattern, '.czi')
    //         return "${mvl_inputs} ${czi_inputs} -r ${resolution} -a ${axis_mapping} -b ${stitching_dir}"
    //     }
    // )
    // parse_out = STITCHING_PARSECZI(
    //     parseczi_args.map { it[0..1] }, // tuple: [spark_uri, spark_work_dir]
    //     parseczi_args.map { it[2] }, // app args
    //     input_dir,
    //     output_dir,
    //     workers,
    //     spark_worker_cores,
    //     spark_gb_per_core,
    //     driver_cores,
    //     driver_memory
    // )
    // ch_versions = ch_versions.mix(STITCHING_PARSECZI.out.versions)

    // // prepare czi to n5
    // czi_to_n5_args = prepare_app_args(
    //     "czi2n5",
    //     STITCHING_PARSECZI.out.spark_context, // tuple: [spark_uri, spark_work_dir]
    //     indexed_spark_work_dirs,
    //     indexed_acq_data,
    //     { acq_name, stitching_dir ->
    //         def tiles_json = entries_inputs_args(stitching_dir, [ 'tiles' ], '-i', '', '.json')
    //         return "${tiles_json} --blockSize ${stitching_block_size}"
    //     }
    // )
    // CZI2N5(
    //     parse_out.spark_context, // tuple: [spark_uri, spark_work_dir]
    //     stitching_app_container,
    //     'org.janelia.stitching.ConvertCZITilesToN5Spark',
    //     czi_to_n5_args.map { it[2] }, // app args
    //     input_dir,
    //     output_dir,
    //     spark_workers,
    //     spark_worker_cores,
    //     spark_gb_per_core,
    //     driver_cores,
    //     driver_memory
    // )
    // ch_versions = ch_versions.mix(CZI2N5.out.versions)

    // flatfield_done = CZI2N5.out
    // if (flatfield_correction) {
    //     // prepare flatfield args
    //     def flatfield_args = prepare_app_args(
    //         "flatfield_correction",
    //         CZI2N5.out.spark_context,
    //         indexed_spark_work_dirs,
    //         indexed_acq_data,
    //         { acq_name, stitching_dir ->
    //             def n5_channels_args = entries_inputs_args(stitching_dir, channels, '-i', '-n5', '.json')
    //             return "${n5_channels_args} --2d --bins 256"
    //         }
    //     )
    //     flatfield_done = FLATFIELD_CORRECTION(
    //         CZI2N5.out.spark_context, // tuple: [spark_uri, spark_work_dir]
    //         stitching_app_container,
    //         'org.janelia.flatfield.FlatfieldCorrection',
    //         flatfield_args.map { it[2] }, // app args
    //         input_dir,
    //         output_dir,
    //         workers,
    //         spark_worker_cores,
    //         spark_gb_per_core,
    //         driver_cores,
    //         driver_memory
    //     )
    //     ch_versions = ch_versions.mix(flatfield_done.versions)
    // }

    // // prepare stitching args
    // stitching_args = prepare_app_args(
    //     "stitching",
    //     flatfield_done.spark_context,
    //     indexed_spark_work_dirs,
    //     indexed_acq_data,
    //     { acq_name, stitching_dir ->
    //         def retiled_n5_channels_args = entries_inputs_args(stitching_dir, channels, '-i', '-n5', '.json')
    //         def correction_args = entries_inputs_args(stitching_dir, channels, '--correction-images-paths', '-n5', '.json')
    //         def channel = parse_stitching_channel(stitching_channel)
    //         def ref_channel_arg = channel ? "-r ${channel}" : ''
    //         return "--stitch ${ref_channel_arg} ${retiled_n5_channels_args} ${correction_args} --mode ${stitching_mode} --padding ${stitching_padding} --blurSigma ${stitching_blur_sigma}"
    //     }
    // )
    // STITCHING(
    //     flatfield_done.spark_context, // tuple: [spark_uri, spark_work_dir]
    //     stitching_app_container,
    //     'org.janelia.stitching.StitchingSpark',
    //     stitching_args.map { it[2] }, // app args
    //     input_dir,
    //     output_dir,
    //     workers,
    //     spark_worker_cores,
    //     spark_gb_per_core,
    //     driver_cores,
    //     driver_memory
    // )
    // ch_versions = ch_versions.mix(STITCHING.out.versions)

    // // prepare fuse args
    // fuse_args = prepare_app_args(
    //     "fuse",
    //     STITCHING.out.spark_context,
    //     indexed_spark_work_dirs,
    //     indexed_acq_data,
    //     { acq_name, stitching_dir ->
    //         def stitched_n5_channels_args = entries_inputs_args(stitching_dir, channels, '-i', '-n5-final', '.json')
    //         def correction_args = entries_inputs_args(stitching_dir, channels, '--correction-images-paths', '-n5', '.json')
    //         return "--fuse ${stitched_n5_channels_args} ${correction_args} --blending --fill --n5BlockSize ${final_block_size_xy}"
    //     }
    // )
    // FUSE(
    //     FUSE.out.spark_context, // tuple: [spark_uri, spark_work_dir]
    //     stitching_app_container,
    //     'org.janelia.stitching.StitchingSpark',
    //     fuse_args.map { it[2] }, // app args
    //     input_dir,
    //     output_dir,
    //     workers,
    //     spark_worker_cores,
    //     spark_gb_per_core,
    //     driver_cores,
    //     driver_memory
    // )
    // ch_versions = ch_versions.mix(FUSE.out.versions)

    // // terminate stitching cluster
    // if (spark_cluster) {
    //     done = SPARK_TERMINATE(
    //         FUSE.out.spark_context,
    //     ) | join(indexed_spark_work_dirs, by:1) | map {
    //         [ it[2], it[0] ]
    //     } | join(indexed_acq_data) | map {
    //         log.debug "Completed stitching for ${it}"
    //         [ it[1], it[4] ] // spark_work_dir, stitching_dir
    //     }
    // }
    // else {
    //     // [index, spark_uri, acq, stitching_dir, spark_work_dir]
    //     done = indexed_acq_data.map {
    //         log.debug "Completed stitching for ${it}"
    //         [ it[4], it[3] ] // spark_work_dir, stitching_dir
    //     }
    // }

    done = Channel.empty()

    emit:
    done
    versions = ch_versions
}

// Create the app arguments for a Spark App invocation
// app_name: app name for logging purposes
// spark_context: [ spark_uri, cluster_work_dir ]
//
//
def prepare_app_args(app_name,
                     spark_context,
                     indexed_working_dirs,
                     indexed_acq_data,
                     app_args_closure) {
    return spark_context | map {
        // reverse the order in the tuple because the join key is the working dir
        def r = [ it[2], it[0] ]
        log.debug "Indexed result from: $it -> $r"
        return r
    } | join(indexed_acq_data) | map {
        log.debug "Create ${app_name} inputs from ${it}"
        def idx = it[0]
        def spark_work_dir = it[1] // spark work dir comes from previous result
        def acq_name = it[2]
        def spark_uri = it[3]
        def stitching_dir = it[4]
        //log.debug "Get ${app_name} args using: (${acq_name}, ${stitching_dir})"
        def app_args = app_args_closure.call(acq_name, stitching_dir)
        def app_inputs = [ spark_uri, spark_work_dir, app_args ]
        log.debug "${app_name} app input ${idx}: ${app_inputs}"
        return app_inputs
    }
}
