#!/usr/bin/env bash -ue

work_dir="!{spark_work_dir_parent}/!{spark_work_dir_name}"

if [[ ! -d "${work_dir}" ]] ; then
    echo "Creating work directory: ${work_dir}"
    mkdir -p "${work_dir}"
else
    echo "Cleaning existing work directory: ${work_dir}"
    rm -f ${work_dir}/spark* || true
fi

spark_version=`ls /opt/spark/jars/spark-core* | sed -e "s/.\(.*\)-\(.*\)\.jar/\2/"`
cat <<-END_VERSIONS > versions.yml
"!{task.process}":
    spark: ${spark_version}
END_VERSIONS
