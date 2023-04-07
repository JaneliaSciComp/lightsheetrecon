#!/usr/bin/env bash -ue

spark_work_dir="!{spark_work_dir}"
spark_local_dir="!{spark_local_dir}"
spark_master_log_file="!{spark_master_log_file}"
spark_config_filepath="!{spark_config_filepath}"
terminate_file_name="!{terminate_file_name}"
sleep_secs=!{sleep_secs}
container_engine=!{container_engine}

echo "Starting spark master - logging to ${spark_master_log_file}"
rm -f ${spark_master_log_file} || true

# Create Spark configuration
mkdir -p `dirname ${spark_config_filepath}`
cat <<EOF > ${spark_config_filepath}
spark.rpc.askTimeout=300s
spark.storage.blockManagerHeartBeatMs=30000
spark.rpc.retry.wait=30s
spark.kryoserializer.buffer.max=1024m
spark.core.connection.ack.wait.timeout=600s
spark.driver.maxResultSize=0
spark.worker.cleanup.enabled=true
spark.local.dir=${spark_local_dir}
EOF

# Initialize the environment for Spark
export SPARK_ENV_LOADED=
export SPARK_HOME=/opt/spark
export PYSPARK_PYTHONPATH_SET=
export PYTHONPATH="/opt/spark/python"
export SPARK_LOG_DIR="${spark_work_dir}"
set +u
. "/opt/spark/sbin/spark-config.sh"
. "/opt/spark/bin/load-spark-env.sh"
set -u

# Determine the IP address of the current host
local_ip=
if [ "!{container_engine}" = "docker" ]; then
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

# Start the Spark manager
/opt/spark/bin/spark-class org.apache.spark.deploy.master.Master \
    -h $local_ip \
    --properties-file ${spark_config_filepath} \
    !{args} &> ${spark_master_log_file} &
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
        cat ${spark_master_log_file} >&2
        exit 1
    fi
    if [[ -e "${terminate_file_name}" ]] ; then
        cat ${spark_master_log_file}
        break
    fi
    sleep ${sleep_secs}
done
