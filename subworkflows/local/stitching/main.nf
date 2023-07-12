include { SPARK_START         } from '../../../subworkflows/local/spark/start/main'
include { SPARK_TERMINATE     } from '../../../modules/local/spark/terminate/main'
include { STITCHING_PREPARE   } from '../../../modules/local/stitching/prepare/main'
include { STITCHING_PARSECZI  } from '../../../modules/local/stitching/parseczi/main'
include { STITCHING_CZI2N5    } from '../../../modules/local/stitching/czi2n5/main'
include { STITCHING_FLATFIELD } from '../../../modules/local/stitching/flatfield/main'
include { STITCHING_STITCH    } from '../../../modules/local/stitching/stitch/main'
include { STITCHING_FUSE      } from '../../../modules/local/stitching/fuse/main'

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
        def spark = [:]
        spark.uri = spark_uri
        spark.work_dir = spark_work_dir
        spark.workers = workers ?: 1
        spark.worker_cores = spark_worker_cores
        spark.driver_cores = driver_cores ?: 1
        spark.driver_memory = driver_memory
        spark.parallelism = (workers * spark_worker_cores)
        spark.executor_memory = (spark_worker_cores * spark_gb_per_core)+" GB"
        log.debug "Assigned Spark context for {$meta.id}: "+spark
        [meta, files, spark]
    }

    STITCHING_PARSECZI(
        prepare_out_meta,
        input_dir,
        output_dir
    )
    ch_versions = ch_versions.mix(STITCHING_PARSECZI.out.versions)

    STITCHING_CZI2N5(
        STITCHING_PARSECZI.out.acquisitions,
        input_dir,
        output_dir
    )
    ch_versions = ch_versions.mix(STITCHING_CZI2N5.out.versions)

    flatfield_done = STITCHING_CZI2N5.out
    if (flatfield_correction) {
        flatfield_done = STITCHING_FLATFIELD(
            STITCHING_CZI2N5.out.acquisitions,
            input_dir,
            output_dir
        )
        ch_versions = ch_versions.mix(flatfield_done.versions)
    }

    STITCHING_STITCH(
        flatfield_done.acquisitions,
        input_dir,
        output_dir
    )
    ch_versions = ch_versions.mix(STITCHING_STITCH.out.versions)

    STITCHING_FUSE(
        STITCHING_STITCH.out.acquisitions,
        input_dir,
        output_dir
    )
    ch_versions = ch_versions.mix(STITCHING_FUSE.out.versions)

    // terminate stitching cluster
    if (spark_cluster) {
        done_cluster = STITCHING_FUSE.out.acquisitions.map { [it[2].uri, it[2].work_dir] }
        done = SPARK_TERMINATE(done_cluster) | map { it[1] }
    }
    else {
        done = STITCHING_FUSE.out.acquisitions.map { it[2].work_dir }
    }

    emit:
    done
    versions = ch_versions
}
