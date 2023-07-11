include { SPARK_START         } from '../../../subworkflows/local/spark/start/main'
include { SPARK_TERMINATE     } from '../../../modules/local/spark/terminate/main'
include { STITCHING_PREPARE   } from '../../../modules/local/stitching/prepare/main'
include { STITCHING_PARSECZI  } from '../../../modules/local/stitching/parseczi/main'
include { STITCHING_CZI2N5    } from '../../../modules/local/stitching/czi2n5/main'
include { STITCHING_FLATFIELD } from '../../../modules/local/stitching/flatfield/main'

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

    // tuple: [acq_name, acq_stitching_output_dir]
    prepare_out = STITCHING_PREPARE(
        acquisitions,
        input_dir,
        output_dir
    )
    // map: {acq_names:channel, stitching_dirs:channel, spark_work_dirs:channel}
    prepare_out.acquisitions.subscribe {
        log.debug "Create stitching dir for ${it[0].id}: ${it[0].stitching_dir}"
    }

    spark_local_dir = "/tmp/spark-${workflow.sessionId}"
    spark_work_dirs = prepare_out.acquisitions.map { it[0].spark_work_dir }

    // When running locally, the driver needs enough resources to run a spark worker
    spark_cluster_res = spark_work_dirs.map { [ 'local[*]', it ] }
    workers = 1
    driver_cores = spark_driver_cores + spark_worker_cores
    driver_memory = (2 + spark_worker_cores * spark_gb_per_core) + " GB"

    if (spark_cluster) {
        workers = spark_workers
        driver_cores = spark_driver_cores
        driver_memory = spark_driver_memory
        spark_cluster_res = SPARK_START(
            spark_local_dir,
            spark_work_dirs,
            input_dir,
            output_dir,
            workers,
            spark_worker_cores,
            spark_gb_per_core
        )
    }

    log.debug "Setting workers: $workers"
    log.debug "Setting driver_cores: $driver_cores"
    log.debug "Setting driver_memory: $driver_memory"

    // Rejoin Spark clusters to metas
    // channel: [ meta, spark_work_dir, spark_uri ]
    prepare_out_meta = prepare_out.acquisitions.map {
        def (meta, files) = it
        log.debug "Prepared $meta.id"
        [meta, meta.spark_work_dir, files]
    }
    .join(spark_cluster_res, by:1)
    .map {
        def (spark_work_dir, meta, files, spark_uri) = it
        meta.spark_uri = spark_uri
        log.debug "Assigned Spark cluster ${spark_uri} for {$meta.id}"
        [meta, files]
    }

    STITCHING_PARSECZI(
        prepare_out_meta,
        input_dir,
        output_dir,
        workers,
        spark_worker_cores,
        spark_gb_per_core,
        driver_cores,
        driver_memory
    )
    ch_versions = ch_versions.mix(STITCHING_PARSECZI.out.versions)

    STITCHING_CZI2N5(
        STITCHING_PARSECZI.out.acquisitions,
        input_dir,
        output_dir,
        workers,
        spark_worker_cores,
        spark_gb_per_core,
        driver_cores,
        driver_memory
    )
    ch_versions = ch_versions.mix(STITCHING_CZI2N5.out.versions)

    flatfield_done = STITCHING_CZI2N5.out
    if (flatfield_correction) {
        flatfield_done = STITCHING_FLATFIELD(
            STITCHING_CZI2N5.out.acquisitions,
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
