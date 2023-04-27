#!/usr/bin/env bash -ue

cluster_work_dir="!{cluster_work_dir}"
spark_uri="!{spark_uri}"
spark_config_filepath="!{spark_config_filepath}"
main_class="!{spark_app_main_class}"
spark_app_args="!{spark_app_args}"
args="!{args}"
parallelism="!{parallelism}"
worker_cores="!{worker_cores}"
executor_memory="!{executor_memory}"
driver_cores="!{driver_cores_sh}"
driver_memory="!{driver_memory_sh}"

echo "Starting the Spark driver"

# Initialize the environment for Spark (same as startmanager.sh)
export SPARK_ENV_LOADED=
export SPARK_HOME=/opt/spark
export PYSPARK_PYTHONPATH_SET=
export PYTHONPATH="/opt/spark/python"
export SPARK_LOG_DIR="${cluster_work_dir}"
set +u
. "/opt/spark/sbin/spark-config.sh"
. "/opt/spark/bin/load-spark-env.sh"
set -u

if [ "${spark_uri}" = "local[*]" ]; then
    spark_cluster_params=
else
    # Determine the IP address of the current host (same as startmanager.sh)
    local_ip=
    if [ "!{workflow.containerEngine}" = "docker" ]; then
        for interface in /sys/class/net/{eth*,en*,em*}; do
            [ -e $interface ] && \
            [ `cat $interface/operstate` == "up" ] && \
            local_ip=$(ifconfig `basename $interface` | grep "inet " | awk '$1=="inet" {print $2; exit}' | sed s/addr://g)
            if [[ "$local_ip" != "" ]]; then
                echo "Use Spark IP: $local_ip"
                break
            fi
        done
        if [[ -z "${local_ip}" ]] ; then
            echo "Could not determine local IP: local_ip is empty"
            exit 1
        fi
    else
        # Take the last IP that's listed by hostname -i.
        # This hack works on Janelia Cluster and AWS EC2.
        # It won't be necessary at all once we add a local option for Spark apps.
        local_ip=`hostname -i | rev | cut -d' ' -f1 | rev`
        echo "Use Spark IP: $local_ip"
        if [[ -z "${local_ip}" ]] ; then
            echo "Could not determine local IP: local_ip is empty"
            exit 1
        fi
    fi
    spark_cluster_params=" \
    --properties-file ${spark_config_filepath} \
    --conf spark.driver.host=${local_ip} \
    --conf spark.driver.bindAddress=${local_ip} \
    "
fi

spark_version=`cat /opt/spark/VERSION`
sitching_spark_version=`cat /app/VERSION`
cat <<-END_VERSIONS > versions.yml
"!{task.process}":
    spark: $spark_version
    $main_class: $sitching_spark_version
END_VERSIONS

/opt/spark/bin/spark-class org.apache.spark.deploy.SparkSubmit \
    $spark_cluster_params \
    --master ${spark_uri} \
    --class ${main_class} \
    --conf spark.files.openCostInBytes=0 \
    --conf spark.default.parallelism=${parallelism} \
    --conf spark.executor.cores=${worker_cores} \
    --executor-memory ${executor_memory} \
    --conf spark.driver.cores=${driver_cores} \
    --driver-memory ${driver_memory} \
    /app/app.jar \
    ${spark_app_args} ${args}
