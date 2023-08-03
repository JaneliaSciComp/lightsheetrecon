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
    spark_cluster
    spark_workers
    spark_worker_cores
    spark_gb_per_core
    spark_driver_cores
    spark_driver_memory

    main:

    spark_work_dirs = acquisitions.map { it[0].spark_work_dir }

    if (spark_cluster) {
        workers = spark_workers
        driver_cores = spark_driver_cores
        driver_memory = spark_driver_memory
        spark_cluster_res = SPARK_START(
            spark_work_dirs,
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
        driver_cores = spark_driver_cores + spark_worker_cores
        driver_memory = (2 + spark_worker_cores * spark_gb_per_core) + " GB"
        spark_cluster_res = spark_work_dirs.map { [ 'local[*]', it ] }
    }

    log.debug "Setting workers: $workers"
    log.debug "Setting driver_cores: $driver_cores"
    log.debug "Setting driver_memory: $driver_memory"

    // Rejoin Spark clusters to metas
    // channel: [ meta, spark_work_dir, spark_uri ]
    spark_context = acquisitions.map {
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
        log.debug "Assigned Spark context for ${meta.id}: "+spark
        [meta, files, spark]
    }


    emit:
    spark_context
}
