process STITCHING_FLATFIELD {
    container 'multifish/biocontainers-stitching-spark:1.9.0'
    cpus { driver_cores == 0 ? 1 : driver_cores }
    memory { driver_memory }

    input:
    tuple val(meta), val(files)
    path(input_dir)
    path(output_dir)
    val(workers)
    val(worker_cores)
    val(mem_per_core_in_gb)
    val(driver_cores)
    val(driver_memory)

    output:
    tuple val(meta), val(files), emit: acquisitions
    path "versions.yml", emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    extra_args = task.ext.args ?: ''
    executor_memory_in_gb = worker_cores * mem_per_core_in_gb
    executor_memory = executor_memory_in_gb+"g"
    parallelism = workers * worker_cores
    driver_cores_sh = driver_cores ?: 1
    driver_memory_sh = driver_memory.replace(" KB",'k').replace(" MB",'m').replace(" GB",'g').replace(" TB",'t')
    container_engine = workflow.containerEngine
    """
    # Remove previous flatfield results because the process will fail if it exists
    rm -r ${meta.stitching_dir}/*flatfield
    declare -a app_args
    for file in ${meta.stitching_dir}/*n5.json
    do
        app_args+=( -i "\$file" )
    done
    /opt/scripts/runapp.sh "$container_engine" "$meta.spark_work_dir" "$meta.spark_uri" \
        /app/app.jar org.janelia.flatfield.FlatfieldCorrection \
        $parallelism $worker_cores "$executor_memory" $driver_cores_sh $driver_memory_sh \
        \${app_args[@]} $extra_args

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        spark: \$(cat /opt/spark/VERSION)
        stitching-spark: \$(cat /app/VERSION)
    END_VERSIONS
    """
}
