#!/usr/bin/env bash -ue

work_dir="!{spark_work_dir}/!{spark_work_dir_name}"

if [[ ! -d "${work_dir}" ]] ; then
    echo "Creating work directory: ${work_dir}"
    mkdir -p "${work_dir}"
else
    echo "Cleaning existing work directory: ${work_dir}"
    rm -f ${work_dir}/spark* || true
fi
