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
    def prepare_input = spark_work_dir.map { [spark_local_dir, file(it).parent, file(it).name] }
    SPARK_PREPARE(prepare_input)

    // start cluster manager and wait for it to be ready
    def manager_input = SPARK_PREPARE.out.map {
        spark_local_dir = it[0]
        spark_work_dir = it[1].toString()+File.separator+it[2]
        log.debug "Spark local directory: ${spark_local_dir}"
        log.debug "Spark work directory: ${spark_work_dir}"
        [spark_local_dir, spark_work_dir]
    }

    // start the Spark manager
    // this runs indefinitely until SPARK_TERMINATE is called
    SPARK_STARTMANAGER(manager_input)

    // start a watcher that waits for the manager to be ready
    SPARK_WAITFORMANAGER(manager_input) // channel: [val(spark_uri, val(spark_work_dir))]

    // cross product all workers with all work dirs
    // so that we can start all needed spark workers with the proper worker directory
    def workers_list = 1..spark_workers

    // cross product all worker directories with all worker numbers
    // channel: [val(spark_uri), val(spark_work_dir), val(worker_id)]
    def workers_with_work_dirs = SPARK_WAITFORMANAGER.out.combine(workers_list)

    // start workers
    // these run indefinitely until SPARK_TERMINATE is called
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
