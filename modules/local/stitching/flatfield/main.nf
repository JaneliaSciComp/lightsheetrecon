process STITCHING_FLATFIELD {
    tag "${meta.id}"
    container 'multifish/biocontainers-stitching-spark:1.9.0'
    cpus { spark.driver_cores }
    memory { spark.driver_memory }

    input:
    tuple val(meta), val(files), val(spark)
    path(input_dir)
    path(output_dir)

    output:
    tuple val(meta), val(files), val(spark), emit: acquisitions
    path "versions.yml", emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    extra_args = task.ext.args ?: ''
    executor_memory = spark.executor_memory.replace(" KB",'k').replace(" MB",'m').replace(" GB",'g').replace(" TB",'t')
    driver_memory = spark.driver_memory.replace(" KB",'k').replace(" MB",'m').replace(" GB",'g').replace(" TB",'t')
    """
    # Remove previous flatfield results because the process will fail if it exists
    rm -r ${meta.stitching_dir}/*flatfield
    # Create command line parameters
    declare -a app_args
    for file in ${meta.stitching_dir}/*-n5.json
    do
        app_args+=( -i "\$file" )
    done
    /opt/scripts/runapp.sh "$workflow.containerEngine" "$spark.work_dir" "$spark.uri" \
        /app/app.jar org.janelia.flatfield.FlatfieldCorrection \
        $spark.parallelism $spark.worker_cores "$executor_memory" $spark.driver_cores "$driver_memory" \
        \${app_args[@]} $extra_args

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        spark: \$(cat /opt/spark/VERSION)
        stitching-spark: \$(cat /app/VERSION)
    END_VERSIONS
    """
}
