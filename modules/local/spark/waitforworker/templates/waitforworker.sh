#!/usr/bin/env bash -u

spark_uri="!{spark_uri}"
spark_worker_log_file="!{spark_worker_log_file}"
terminate_file_name="!{terminate_file_name}"
sleep_secs=!{sleep_secs}
max_wait_secs=!{max_wait_secs}

elapsed_secs=0
while true; do

    if [[ -e ${spark_worker_log_file} ]]; then
        found=`grep -o "\(Worker: Successfully registered with master ${spark_uri}\)" ${spark_worker_log_file} || true`
        if [[ ! -z ${found} ]]; then
            echo "${found}"
            break
        fi
    fi

    if [[ -e "${terminate_file_name}" ]]; then
        echo "Terminate file ${terminate_file_name} found"
        exit 1
    fi

    if (( ${elapsed_secs} > ${max_wait_secs} )); then
        echo "Timed out after ${elapsed_secs} seconds while waiting for Spark manager <- ${spark_uri}"
        cat ${spark_worker_log_file} >&2
        exit 2
    fi

    sleep ${sleep_secs}
    elapsed_secs=$(( ${elapsed_secs} + ${sleep_secs} ))
done
