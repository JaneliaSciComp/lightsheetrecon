include { SPARK_START       } from '../../../subworkflows/local/spark/start/main'
include { SPARK_TERMINATE   } from '../../../modules/local/spark/terminate/main'
include { STITCHING_PREPARE } from '../../../modules/local/stitching/prepare/main'

include {
    SPARK_RUNAPP as PARSECZI;
    SPARK_RUNAPP as CZI2N5;
    SPARK_RUNAPP as FLATFIELD_CORRECTION;
    SPARK_RUNAPP as RETILE;
    SPARK_RUNAPP as STITCHING;
    SPARK_RUNAPP as FUSE;
} from '../../../modules/local/spark/runapp/main'

include {
    parse_stitching_channel;
    entries_inputs_args;
    index_channel;
} from './utils'

/**
 * prepares the work and output directories and invoke stitching
 * for the aquisitions list - this is a value channel containing a list of acquistins
 *
 * @return tuple of <acq_name, acq_stitching_dir>
 */
workflow STITCH {
    take:
    ch_acq_names
    input_dir
    output_dir
    channels
    resolution
    axis_mapping
    stitching_block_size
    final_block_size_xy
    retile_z_size
    stitching_channel
    stitching_mode
    stitching_padding
    stitching_blur_sigma
    stitching_czi_pattern
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
        input_dir,
        output_dir,
        ch_acq_names
    )
    // map: {acq_names:channel, stitching_dirs:channel, spark_work_dirs:channel}
    acq_inputs = STITCHING_PREPARE.out
        .map {
            acq_name = it[0]
            acq_stitching_dir = it[1]
            acq_spark_work_dir = "${spark_work_dir}/${acq_name}"
            acq_input = [ acq_name, acq_stitching_dir, acq_spark_work_dir ]
            log.debug "Create acq input ${acq_input} from ${it} and ${spark_work_dir}"
            return acq_input
        }
        .multiMap { it ->
            log.debug "Put acq name '${it[0]}' into acq_names channel"
            log.debug "Put stitching dir '${it[1]}' into stitching_dirs channel"
            log.debug "Put spark work dir '${it[2]}' into spark_work_dirs channel"
            acq_names: it[0]
            stitching_dirs: it[1]
            spark_work_dirs: it[2]
        }

    spark_local_dir = "/tmp/spark-${workflow.sessionId}"

    // index inputs so that we can pair acq_name with the corresponding spark URI and/or spark working dir
    def indexed_acq_names = index_channel(acq_inputs.acq_names)
    def indexed_stitching_dirs = index_channel(acq_inputs.stitching_dirs)
    def indexed_spark_work_dirs = index_channel(acq_inputs.spark_work_dirs)

    indexed_acq_names.subscribe { log.debug "Indexed acq: $it" }
    indexed_stitching_dirs.subscribe { log.debug "Indexed stitching dir: $it" }
    indexed_spark_work_dirs.subscribe { log.debug "Indexed spark working dir: $it" }

    def spark_cluster_res = acq_inputs.spark_work_dirs.map { [ 'local[*]', it ] }
    driver_cores = spark_driver_cores
    driver_memory = spark_driver_memory
    workers = spark_workers
    if (spark_cluster) {
        spark_cluster_res = SPARK_START(
            spark_local_dir,
            acq_inputs.spark_work_dirs,
            input_dir,
            output_dir,
            workers,
            spark_worker_cores,
            spark_gb_per_core
        )
    }
    else {
        // When running locally, the driver needs enough resources to run a spark worker
        workers = 1
        driver_cores = driver_cores + spark_worker_cores
        driver_memory = (2 + spark_worker_cores * spark_gb_per_core) + " GB"
    }
    log.debug "Setting driver_cores: $driver_cores"
    log.debug "Setting driver_memory: $driver_memory"

    // print spark cluster result
    // channel: [ spark_uri, spark_work_dir ]

    def indexed_spark_uris = spark_cluster_res
        .join(indexed_spark_work_dirs, by:1)
        .map {
            def indexed_uri = [ it[2], it[1] ]
            log.debug "Create indexed spark URI from $it -> ${indexed_uri}"
            return indexed_uri
        }

    // create a channel of tuples: [index, spark_uri, acq, stitching_dir, spark_work_dir]
    def indexed_acq_data = indexed_acq_names
        | join(indexed_spark_uris)
        | join(indexed_stitching_dirs)
        | join(indexed_spark_work_dirs)

    // prepare parse czi tiles
    parseczi_args = prepare_app_args(
        "parseczi",
        indexed_spark_work_dirs, // to start the chain we just need a tuple that has the working dir as the 2nd element
        indexed_spark_work_dirs,
        indexed_acq_data,
        { acq_name, stitching_dir ->
            def mvl_inputs = entries_inputs_args(stitching_dir, [ acq_name ], '-i', '', '.mvl')
            def czi_inputs = entries_inputs_args('', [ acq_name ], '-f', stitching_czi_pattern, '.czi')
            return "${mvl_inputs} ${czi_inputs} -r ${resolution} -a ${axis_mapping} -b ${stitching_dir}"
        }
    )
    parse_out = PARSECZI(
        parseczi_args.map { it[0..1] }, // tuple: [spark_uri, spark_work_dir]
        stitching_app_container,
        'org.janelia.stitching.ParseCZITilesMetadata',
        parseczi_args.map { it[2] }, // app args
        input_dir,
        output_dir,
        workers,
        spark_worker_cores,
        spark_gb_per_core,
        driver_cores,
        driver_memory
    )
    ch_versions = ch_versions.mix(PARSECZI.out.versions)

    // prepare czi to n5
    czi_to_n5_args = prepare_app_args(
        "czi2n5",
        parse_out.spark_context, // tuple: [spark_uri, spark_work_dir]
        indexed_spark_work_dirs,
        indexed_acq_data,
        { acq_name, stitching_dir ->
            def tiles_json = entries_inputs_args(stitching_dir, [ 'tiles' ], '-i', '', '.json')
            return "${tiles_json} --blockSize ${stitching_block_size}"
        }
    )
    CZI2N5(
        czi_to_n5_args.map { it[0..1] }, // tuple: [spark_uri, spark_work_dir]
        stitching_app_container,
        'org.janelia.stitching.ConvertCZITilesToN5Spark',
        czi_to_n5_args.map { it[2] }, // app args
        input_dir,
        output_dir,
        spark_workers,
        spark_worker_cores,
        spark_gb_per_core,
        driver_cores,
        driver_memory
    )
    ch_versions = ch_versions.mix(CZI2N5.out.versions)

    flatfield_done = CZI2N5.out
    if (flatfield_correction) {
        // prepare flatfield args
        def flatfield_args = prepare_app_args(
            "flatfield_correction",
            CZI2N5.out.spark_context,
            indexed_spark_work_dirs,
            indexed_acq_data,
            { acq_name, stitching_dir ->
                def n5_channels_args = entries_inputs_args(stitching_dir, channels, '-i', '-n5', '.json')
                return "${n5_channels_args} --2d --bins 256"
            }
        )
        flatfield_done = FLATFIELD_CORRECTION(
            flatfield_args.map { it[0..1] }, // tuple: [spark_uri, spark_work_dir]
            stitching_app_container,
            'org.janelia.flatfield.FlatfieldCorrection',
            flatfield_args.map { it[2] }, // app args
            input_dir,
            output_dir,
            workers,
            spark_worker_cores,
            spark_gb_per_core,
            driver_cores,
            driver_memory
        )
        ch_versions = ch_versions.mix(flatfield_done.versions)
    }
    // // prepare retile args
    // retile_args = prepare_app_args(
    //     "retile",
    //     flatfield_done.spark_context,
    //     indexed_spark_work_dirs,
    //     indexed_acq_data,
    //     { acq_name, stitching_dir ->
    //         def retile_args = entries_inputs_args(stitching_dir, channels, '-i', '-n5', '.json')
    //         return "${retile_args} --size ${retile_z_size}"
    //     }
    // )
    // RETILE(
    //     retile_args.map { it[0..1] }, // tuple: [spark_uri, spark_work_dir]
    //     stitching_app_container,
    //     'org.janelia.stitching.ResaveAsSmallerTilesSpark',
    //     retile_args.map { it[2] }, // app args
    //     input_dir,
    //     output_dir,
    //     workers,
    //     spark_worker_cores,
    //     spark_gb_per_core,
    //     driver_cores,
    //     driver_memory
    // )
    // ch_versions = ch_versions.mix(RETILE.out.versions)

    // prepare stitching args
    stitching_args = prepare_app_args(
        "stitching",
        flatfield_done.spark_context,
        indexed_spark_work_dirs,
        indexed_acq_data,
        { acq_name, stitching_dir ->
            def retiled_n5_channels_args = entries_inputs_args(stitching_dir, channels, '-i', '-n5', '.json')
            def correction_args = entries_inputs_args(stitching_dir, channels, '--correction-images-paths', '-n5', '.json')
            def channel = parse_stitching_channel(stitching_channel)
            def ref_channel_arg = channel ? "-r ${channel}" : ''
            return "--stitch ${ref_channel_arg} ${retiled_n5_channels_args} ${correction_args} --mode ${stitching_mode} --padding ${stitching_padding} --blurSigma ${stitching_blur_sigma}"
        }
    )
    STITCHING(
        stitching_args.map { it[0..1] }, // tuple: [spark_uri, spark_work_dir]
        stitching_app_container,
        'org.janelia.stitching.StitchingSpark',
        stitching_args.map { it[2] }, // app args
        input_dir,
        output_dir,
        workers,
        spark_worker_cores,
        spark_gb_per_core,
        driver_cores,
        driver_memory
    )
    ch_versions = ch_versions.mix(STITCHING.out.versions)

    // prepare fuse args
    fuse_args = prepare_app_args(
        "fuse",
        STITCHING.out.spark_context,
        indexed_spark_work_dirs,
        indexed_acq_data,
        { acq_name, stitching_dir ->
            def stitched_n5_channels_args = entries_inputs_args(stitching_dir, channels, '-i', '-n5-final', '.json')
            def correction_args = entries_inputs_args(stitching_dir, channels, '--correction-images-paths', '-n5', '.json')
            return "--fuse ${stitched_n5_channels_args} ${correction_args} --blending --fill --n5BlockSize ${final_block_size_xy}"
        }
    )
    FUSE(
        fuse_args.map { it[0..1] }, // tuple: [spark_uri, spark_work_dir]
        stitching_app_container,
        'org.janelia.stitching.StitchingSpark',
        fuse_args.map { it[2] }, // app args
        input_dir,
        output_dir,
        workers,
        spark_worker_cores,
        spark_gb_per_core,
        driver_cores,
        driver_memory
    )
    ch_versions = ch_versions.mix(FUSE.out.versions)

    // terminate stitching cluster
    if (spark_cluster) {
        done = SPARK_TERMINATE(
            FUSE.out.spark_context,
        ) | join(indexed_spark_work_dirs, by:1) | map {
            [ it[2], it[0] ]
        } | join(indexed_acq_data) | map {
            log.debug "Completed stitching for ${it}"
            [ it[1], it[4] ] // spark_work_dir, stitching_dir
        }
    }
    else {
        // [index, spark_uri, acq, stitching_dir, spark_work_dir]
        done = indexed_acq_data.map {
            log.debug "Completed stitching for ${it}"
            [ it[4], it[3] ] // spark_work_dir, stitching_dir
        }
    }

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
    return spark_context | join(indexed_working_dirs, by: 1) | map {
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
