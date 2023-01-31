include { spark_start_app } from './processes'

workflow SPARK_APP {
    take:
    spark_uri
    spark_app
    spark_app_entrypoint
    spark_app_args
    spark_app_log
    spark_app_terminate_name
    spark_conf
    spark_work_dir
    spark_workers
    spark_executor_cores
    spark_gbmem_per_core
    spark_driver_cores
    spark_driver_memory
    spark_driver_stack_size
    spark_driver_logconfig
    spark_driver_deploy_mode

    main:
    done = spark_start_app(
        spark_uri,
        spark_conf,
        spark_work_dir,
        spark_workers,
        spark_executor_cores,
        spark_gbmem_per_core,
        spark_driver_cores,
        spark_driver_memory,
        spark_driver_stack_size,
        spark_driver_logconfig,
        spark_driver_deploy_mode,
        spark_app,
        spark_app_entrypoint,
        spark_app_args,
        spark_app_log
    )

    emit:
    done
}
