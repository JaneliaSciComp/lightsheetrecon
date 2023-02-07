#!/usr/bin/env bash -ue

work_dir="!{spark_work_dir}"

if [[ ! -d "${work_dir}" ]] ; then
    mkdir -p "${work_dir}"
else
    rm -f ${work_dir}/* || true
fi

spark_version=`ls /opt/spark/jars/spark-core* | sed -e "s/.\(.*\)-\(.*\)\.jar/\2/"`
cat <<-END_VERSIONS > versions.yml
"!{task.process}":
    spark: ${spark_version}
END_VERSIONS
