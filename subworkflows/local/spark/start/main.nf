include { SPARK_PREPARE } from '../../../../modules/local/spark/prepare/main'
include { SPARK_STARTMANAGER } from '../../../../modules/local/spark/startmanager/main'
include { SPARK_WAITFORMANAGER } from '../../../../modules/local/spark/waitformanager/main'
include { SPARK_STARTWORKER } from '../../../../modules/local/spark/startworker/main'
include { SPARK_WAITFORWORKER } from '../../../../modules/local/spark/waitforworker/main'

workflow SPARK_START {
    take:
    spark_local_dir
    spark_work_dir
    spark_workers
    spark_worker_cores
    spark_gbmem_per_core

    main:
    // prepare spark cluster params
    SPARK_PREPARE([spark_local_dir, spark_work_dir])

    // start cluster manager and wait for it to be ready
    SPARK_STARTMANAGER(SPARK_PREPARE.out)
    SPARK_WAITFORMANAGER(SPARK_PREPARE.out) // channel: [val(spark_uri, val(spark_work_dir))]

    // cross product all workers with all work dirs
    // so that we can start all needed spark workers with the proper worker directory
    def workers_list = 1..spark_workers

    // cross product all worker directories with all worker numbers
    // channel: [val(spark_uri), val(spark_work_dir), val(worker_id)]
    def workers_with_work_dirs = SPARK_WAITFORMANAGER.out.combine(workers_list)

    // start workers
    SPARK_STARTWORKER(
        workers_with_work_dirs,
        spark_worker_cores,
        spark_worker_cores * spark_gbmem_per_core,
    )

    // wait for cluster to start
    def final_out = SPARK_WAITFORWORKER(workers_with_work_dirs)
    | map {
        def worker_id = it[2]
        log.debug "Spark worker $worker_id - started"
        it
    }
    | groupTuple(by: [0,1]) // wait for all workers to start
    | map {
        log.debug "Spark cluster started:"
        log.debug "  Spark work directory: ${it[1]}"
        log.debug "  Number of workers: ${spark_workers}"
        log.debug "  Cores per worker: ${spark_worker_cores}"
        log.debug "  GB per worker core: ${spark_gbmem_per_core}"
        it[0..1]
    }

    emit:
    done = final_out // channel: [ spark_uri, spark_work_dir ]
}
