include { SPARK_START } from '../../../subworkflows/local/spark/start/main'
include { SPARK_TERMINATE } from '../../../modules/local/spark/terminate/main'
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
    retile_z_size
    registration_channel
    stitching_mode
    stitching_padding
    stitching_blur_sigma
    stitching_czi_pattern
    spark_workers
    spark_worker_cores
    spark_gb_per_core
    spark_driver_cores
    spark_driver_memory

    main:
    ch_versions = Channel.empty()
    //def stitching_app_container = "multifish/biocontainers-spark:3.1.3"
    def stitching_app_container = "multifish/biocontainers-stitching-spark:1.9.0"

    def spark_work_dir = "${output_dir}/spark/${workflow.sessionId}"
    if (!file(spark_work_dir).mkdirs()) {
        error "Could not create Spark working dir at $spark_work_dir"
    }
    def acq_stitching_dir_pairs = STITCHING_PREPARE(
        input_dir,
        output_dir,
        ch_acq_names
    )
    def acq_inputs = acq_stitching_dir_pairs
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

    def spark_local_dir = "/tmp/spark-${workflow.sessionId}"

    // index inputs so that we can pair acq_name with the corresponding spark URI and/or spark working dir
    def indexed_acq_names = index_channel(acq_inputs.acq_names)
    def indexed_stitching_dirs = index_channel(acq_inputs.stitching_dirs)
    def indexed_spark_work_dirs = index_channel(acq_inputs.spark_work_dirs)

    indexed_acq_names.subscribe { log.debug "Indexed acq: $it" }
    indexed_stitching_dirs.subscribe { log.debug "Indexed stitching dir: $it" }
    indexed_spark_work_dirs.subscribe { log.debug "Indexed spark working dir: $it" }

    // start a spark cluster
    // channel: [ spark_uri, spark_work_dir ]
    def spark_cluster_res = SPARK_START(
        spark_local_dir,
        acq_inputs.spark_work_dirs,
        input_dir,
        output_dir,
        spark_workers,
        spark_worker_cores,
        spark_gb_per_core
    )

    // print spark cluster result
    // channel: [ spark_uri, spark_work_dir ]
    spark_cluster_res.subscribe {  log.debug "Spark cluster result: $it"  }

    def indexed_spark_uris = spark_cluster_res
        .join(indexed_spark_work_dirs, by:1)
        .map {
            def indexed_uri = [ it[2], it[1] ]
            log.debug "Create indexed spark URI from $it -> ${indexed_uri}"
            return indexed_uri
        }

    // create a channel of tuples:  [index, spark_uri, acq, stitching_dir, spark_work_dir]
    def indexed_acq_data = indexed_acq_names
        | join(indexed_spark_uris)
        | join(indexed_stitching_dirs)
        | join(indexed_spark_work_dirs)

    // prepare parse czi tiles
    def PARSECZI_args = prepare_app_args(
        "parseCZI",
        indexed_spark_work_dirs, // to start the chain we just need a tuple that has the working dir as the 2nd element
        indexed_spark_work_dirs,
        indexed_acq_data,
        { acq_name, stitching_dir ->
            def mvl_inputs = entries_inputs_args(stitching_dir, [ acq_name ], '-i', '', '.mvl')
            def czi_inputs = entries_inputs_args('', [ acq_name ], '-f', stitching_czi_pattern, '.czi')
            return "${mvl_inputs} ${czi_inputs} -r ${resolution} -a ${axis_mapping} -b ${stitching_dir}"
        }
    )
    PARSECZI(
        PARSECZI_args.map { it[0..1] }, // tuple: [spark_uri, spark_work_dir]
        stitching_app_container,
        'org.janelia.stitching.ParseCZITilesMetadata',
        PARSECZI_args.map { it[2] }, // app args
        input_dir,
        output_dir,
        spark_workers,
        spark_worker_cores,
        spark_gb_per_core,
        spark_driver_cores,
        spark_driver_memory
    )
    ch_versions = ch_versions.mix(PARSECZI.out.versions)

    // prepare czi to n5
    def czi_to_n5_args = prepare_app_args(
        "czi2N5",
        PARSECZI.out.cluster, // tuple: [spark_uri, spark_work_dir]
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
        spark_driver_cores,
        spark_driver_memory
    )
    ch_versions = ch_versions.mix(CZI2N5.out.versions)

    def flatfield_done = CZI2N5.out
    if (params.flatfield_correction) {
        // prepare flatfield args
        def flatfield_args = prepare_app_args(
            "flatfield",
            CZI2N5.out.cluster,
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
            spark_workers,
            spark_worker_cores,
            spark_gb_per_core,
            spark_driver_cores,
            spark_driver_memory
        )
        ch_versions = ch_versions.mix(flatfield_done.versions)
    }
    // prepare retile args
    def retile_args = prepare_app_args(
        "retile",
        flatfield_done.cluster,
        indexed_spark_work_dirs,
        indexed_acq_data,
        { acq_name, stitching_dir ->
            def retile_args = entries_inputs_args(stitching_dir, channels, '-i', '-n5', '.json')
            return "${retile_args} --size ${retile_z_size}"
        }
    )
    RETILE(
        retile_args.map { it[0..1] }, // tuple: [spark_uri, spark_work_dir]
        stitching_app_container,
        'org.janelia.stitching.ResaveAsSmallerTilesSpark',
        retile_args.map { it[2] }, // app args
        input_dir,
        output_dir,
        spark_workers,
        spark_worker_cores,
        spark_gb_per_core,
        spark_driver_cores,
        spark_driver_memory
    )
    ch_versions = ch_versions.mix(RETILE.out.versions)

    // prepare stitching args
    def stitching_args = prepare_app_args(
        "stitching",
        RETILE.out.cluster,
        indexed_spark_work_dirs,
        indexed_acq_data,
        { acq_name, stitching_dir ->
            def retiled_n5_channels_args = entries_inputs_args(stitching_dir, channels, '-i', '-n5-retiled', '.json')
            def correction_args = entries_inputs_args(stitching_dir, channels, '--correction-images-paths', '-n5', '.json')
            def ref_channel_arg = registration_channel ? "-r ${registration_channel}" : ''
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
        spark_workers,
        spark_worker_cores,
        spark_gb_per_core,
        spark_driver_cores,
        spark_driver_memory
    )
    ch_versions = ch_versions.mix(STITCHING.out.versions)

    // prepare fuse args
    def fuse_args = prepare_app_args(
        "fuse",
        STITCHING.out.cluster,
        indexed_spark_work_dirs,
        indexed_acq_data,
        { acq_name, stitching_dir ->
            def stitched_n5_channels_args = entries_inputs_args(stitching_dir, channels, '-i', '-n5-retiled-final', '.json')
            def correction_args = entries_inputs_args(stitching_dir, channels, '--correction-images-paths', '-n5', '.json')
            return "--fuse ${stitched_n5_channels_args} ${correction_args} --blending --fill"
        }
    )
    FUSE(
        fuse_args.map { it[0..1] }, // tuple: [spark_uri, spark_work_dir]
        stitching_app_container,
        'org.janelia.stitching.StitchingSpark',
        fuse_args.map { it[2] }, // app args
        input_dir,
        output_dir,
        spark_workers,
        spark_worker_cores,
        spark_gb_per_core,
        spark_driver_cores,
        spark_driver_memory
    )
    ch_versions = ch_versions.mix(FUSE.out.versions)

    // terminate stitching cluster
    done = SPARK_TERMINATE(
        FUSE.out.cluster,
    ) | join(indexed_spark_work_dirs, by:1) | map {
        [ it[2], it[0] ]
    } | join(indexed_acq_data) | map {
        log.debug "Completed stitching for ${it}"
        // acq_name, acq_stitching_dir
        [ it[2], it[4] ]
    }

    emit:
    done
    versions = ch_versions
}

def prepare_app_args(app_name,
                     previous_result_dirs,
                     indexed_working_dirs,
                     indexed_acq_data,
                     app_args_closure) {
    return previous_result_dirs | join(indexed_working_dirs, by: 1) | map {
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
        log.debug "Get ${app_name} args using: (${acq_name}, ${stitching_dir})"
        def app_args = app_args_closure.call(acq_name, stitching_dir)
        def app_inputs = [ spark_uri, spark_work_dir, app_args ]
        log.debug "${app_name} app input ${idx}: ${app_inputs}"
        return app_inputs
    }
}
