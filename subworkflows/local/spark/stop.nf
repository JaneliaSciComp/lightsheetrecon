
include { terminate_spark } from './processes'

workflow SPARK_STOP {
    take:
    spark_work_dir
    spark_app_terminate_name

    main:
    done = terminate_spark(
        spark_work_dir,
        spark_app_terminate_name
    )

    emit:
    done
}
