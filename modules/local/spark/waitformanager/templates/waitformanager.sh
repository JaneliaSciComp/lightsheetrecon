#!/usr/bin/env bash -ue

spark_master_log_name="!{spark_master_log_name}"
terminate_file_name="!{terminate_file_name}"
sleep_secs=!{sleep_secs}
max_wait_secs=!{max_wait_secs}

elapsed_secs=0
while true; do

    if [[ -e ${spark_master_log_name} ]]; then
        test_uri=`grep -o "\(spark://.*$\)" ${spark_master_log_name} || true`
        if [[ ! -z ${test_uri} ]]; then
            echo "Spark master started at ${test_uri}"
            break
        fi
    fi

    if [[ -e "${terminate_file_name}" ]]; then
        echo "Terminate file ${terminate_file_name} found"
        exit 1
    fi

    if (( ${elapsed_secs} > ${max_wait_secs} )); then
        echo "Timed out after ${elapsed_secs} seconds while waiting for Spark master <- ${spark_master_log_name}"
        cat ${spark_master_log_name} >&2
        exit 2
    fi

    sleep ${sleep_secs}
    elapsed_secs=$(( ${elapsed_secs} + ${sleep_secs} ))
done

export spark_uri=${test_uri}
