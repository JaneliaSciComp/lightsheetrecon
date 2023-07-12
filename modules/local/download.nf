process DOWNLOAD {
    tag "${samplesheet_row.filename}"
    container "multifish/downloader:1.1.0"

    input:
    path download_dir
    val samplesheet_row

    output:
    val samplesheet_row, emit: tiles
    path "versions.yml", emit: versions

    when:
    task.ext.when == null || task.ext.when

    script: // This script is bundled with the pipeline, in ./bin
    filepath = "${download_dir}/${samplesheet_row.filename}"
    """
    download.sh ${samplesheet_row.uri} ${filepath} ${samplesheet_row.checksum}

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        python: \$(python --version | sed 's/Python //g')
    END_VERSIONS
    """
}


