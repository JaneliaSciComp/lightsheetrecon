include { SPARK_TERMINATE } from '../../../../modules/local/spark/terminate/main'

/**
 * Terminate Spark clusters specified by the meta_tuples.
 */
workflow SPARK_STOP {
    take:
    meta_tuples // channel: [ val(meta), [ files ], val(spark_context) ]

    main:
    if (params.spark_cluster) {
        done_cluster = meta_tuples.map { [it[2].uri, it[2].work_dir] }
        done = SPARK_TERMINATE(done_cluster) | map { it[1] }
    }
    else {
        done = meta_tuples.map { it[2].work_dir }
    }

    emit:
    done
}
