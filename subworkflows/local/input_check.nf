//
// Check input samplesheet and download images if necessary
//

include { SAMPLESHEET_CHECK } from '../../modules/local/samplesheet_check'
include { DOWNLOAD } from '../../modules/local/download'

workflow INPUT_CHECK {
    take:
    imagedir
    samplesheet // file: /path/to/samplesheet.csv

    main:
    SAMPLESHEET_CHECK(samplesheet)
        .csv
        .splitCsv ( header:true, sep:',' )
        .branch {
            row ->
                remote: row.containsKey('uri')
                    return row
                local: true
                    return row
    }
    .set { tiles }

    DOWNLOAD(imagedir, tiles.remote)
        .tiles
        .mix(tiles.local)
        .map { create_acq_channel(it, imagedir) }
        // Group by acquisition
        .groupTuple()
        .map {
            // Set acquisition's filename pattern to the meta map
            def (meta, files, patterns) = it
            meta.pattern = patterns.findAll({ it?.trim() }).first()
            [meta, files]
        }
        .set { acquisitions }

    emit:
    acquisitions                              // channel: [ val(meta), [ filenames ] ]
    versions = SAMPLESHEET_CHECK.out.versions // channel: [ versions.yml ]
}

def create_acq_channel(LinkedHashMap samplesheet_row, imagedir) {
    def meta = [:]
    meta.id = samplesheet_row.id
    filepath = "${imagedir}/${samplesheet_row.filename}"
    return [meta, file(filepath), samplesheet_row.pattern]
}
