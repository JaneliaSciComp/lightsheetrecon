include {
    prepare_spark_work_dir;
    spark_master;
    spark_worker;
    wait_for_master;
    wait_for_worker;
} from './processes'

workflow SPARK_START {
    take:
    spark_conf
    spark_work_dir
    spark_workers
    spark_worker_cores
    spark_gbmem_per_core
    spark_app_terminate_name

    main:
    // prepare spark cluster params
    def work_dir = prepare_spark_work_dir(spark_work_dir, spark_app_terminate_name)

    // start master
    spark_master(
        spark_conf,
        work_dir,
        spark_app_terminate_name
    )

    def spark_master_res = wait_for_master(work_dir, spark_app_terminate_name)

    // cross product all workers with all work dirs and
    // then push them to different channels
    // so that we can start all needed spark workers with the proper worker directory
    def workers_list = create_workers_list(spark_workers)
    // cross product all worker directories with all worker numbers
    def workers_with_work_dirs = spark_master_res[0].combine(workers_list)

    // start workers
    spark_worker(
        workers_with_work_dirs.map { it[2] }, // spark uri
        workers_with_work_dirs.map { it[3] }, // worker number
        spark_conf,
        workers_with_work_dirs.map { it[0] }, // spark work dir
        spark_worker_cores,
        spark_worker_cores * spark_gbmem_per_core,
        workers_with_work_dirs.map { it[1] }, // spark app terminate name
    )

    // wait for cluster to start
    def spark_cluster_res = wait_for_worker(
        workers_with_work_dirs.map { it[2] }, // spark uri
        workers_with_work_dirs.map { it[0] }, // spark work dir
        workers_with_work_dirs.map { it[1] }, // spark app terminate name
        workers_with_work_dirs.map { it[3] } // worker number
    )
    | map {
        log.debug "Spark worker $it - started"
        it
    }
    | groupTuple(by: [0,1,2]) // wait for all workers to start
    | map {
        log.debug "Spark cluster started:"
        log.debug "  Spark work directory: ${it[1]}"
        log.debug "  Number of workers: ${spark_workers}"
        log.debug "  Cores per worker: ${spark_worker_cores}"
        log.debug "  GB per worker core: ${spark_gbmem_per_core}"
        it[0..1]
    } // [ spark_uri, spark_work_dir ]

    emit:
    done = spark_cluster_res
}

def create_workers_list(nworkers) {
    log.debug "Prepare $nworkers workers"
    return 1..nworkers
}
