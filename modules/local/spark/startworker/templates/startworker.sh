#!/usr/bin/env bash -ue

spark_work_dir="!{spark_work_dir}"
spark_uri="!{spark_uri}"
worker_id="!{worker_id}"
worker_cores="!{worker_cores}"
worker_mem_in_gb="!{worker_mem_in_gb}"
spark_worker_log_file="!{spark_worker_log_file}"
spark_config_filepath="!{spark_config_filepath}"
terminate_file_name="!{terminate_file_name}"
sleep_secs=!{sleep_secs}

echo "Starting spark worker ${worker_id} - logging to ${spark_worker_log_file}"
rm -f ${spark_worker_log_file} || true

# Initialize the environment for Spark (same as startmanager.sh)
export SPARK_ENV_LOADED=
export SPARK_HOME=/opt/spark
export PYSPARK_PYTHONPATH_SET=
export PYTHONPATH="/opt/spark/python"
export SPARK_LOG_DIR="${spark_work_dir}"
export SPARK_WORKER_OPTS="-Dspark.worker.cleanup.enabled=true -Dspark.worker.cleanup.interval=30 -Dspark.worker.cleanup.appDataTtl=1 -Dspark.port.maxRetries=64"
set +u
. "/opt/spark/sbin/spark-config.sh"
. "/opt/spark/bin/load-spark-env.sh"
set -u

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

/opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
    ${spark_uri} \
    -c ${worker_cores} \
    -m ${worker_mem_in_gb}G \
    -d ${spark_work_dir} \
    -h ${local_ip} \
    --properties-file ${spark_config_filepath} \
    !{args} &> ${spark_worker_log_file} &
spid=$!

# Ensure that Spark process dies if this script is interrupted
function cleanup() {
    echo "Killing background processes"
    [[ $spid ]] && kill "$spid"
    exit 0
}
trap cleanup INT TERM EXIT

while true; do
    if ! kill -0 $spid >/dev/null 2>&1; then
        echo "Process $spid died"
        cat ${spark_worker_log_file} >&2
        exit 1
    fi
    if [[ -e "${terminate_file_name}" ]] ; then
        cat ${spark_worker_log_file}
        break
    fi
    sleep ${sleep_secs}
done
