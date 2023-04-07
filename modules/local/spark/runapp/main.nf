include {
    get_spark_config_filepath;
} from '../utils'

process SPARK_RUNAPP {
    container { spark_app_container }
    cpus { driver_cores == 0 ? 1 : driver_cores }
    memory { driver_memory.replace('k'," KB").replace('m'," MB").replace('g'," GB").replace('t'," TB") }

    input:
    tuple val(spark_uri), path(spark_work_dir)
    val(spark_app_container)
    val(spark_app_main_class)
    val(spark_app_args)
    val(workers)
    val(executor_cores)
    val(mem_per_core_in_gb)
    val(driver_cores)
    val(driver_memory)

    output:
    tuple val(spark_uri), path(spark_work_dir)

    when:
    task.ext.when == null || task.ext.when

    shell:
    args = task.ext.args ?: ''
    sleep_secs = task.ext.sleep_secs ?: '1'
    spark_config_filepath = get_spark_config_filepath(spark_work_dir)
    executor_cores = executor_cores_param as int
    executor_memory_in_gb = cores * mem_per_core_in_gb
    executor_memory = executor_memory_in_gb+"g"
    parallelism = workers * executor_cores
    driver_cores_sh = driver_cores ?: 1
    driver_memory_sh = driver_memory.replace(" KB",'k').replace(" MB",'m').replace(" GB",'g').replace(" TB",'t')
    template 'runapp.sh'
}
