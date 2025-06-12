#!/usr/bin/env nextflow
nextflow.enable.dsl = 2

// Parameter validation
if (!params.ftp_urls) {
    error "Please provide --ftp_urls parameter pointing to your URLs file"
}

if (!params.outdir) {
    error "Please provide --outdir parameter pointing to your output directory"
}

process DOWNLOAD_FTP_DATA {
    conda "conda-forge::wget=1.21.3"
    
    publishDir "${params.outdir}/raw_data", mode: 'copy'
    
    input:
    val ftp_url
    
    output:
    path "*.{fastq.gz,fq.gz,txt,csv,tsv}", emit: downloaded_files
    path "versions.yml", emit: versions
    
    script:
    def filename = ftp_url.split('/').last()
    """
    # Download file from FTP
    wget -t 3 -T 30 "${ftp_url}" -O "${filename}"
    
    # Verify download
    if [ ! -s "${filename}" ]; then
        echo "Error: Failed to download ${ftp_url}"
        exit 1
    fi
    
    # Create versions file
    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        wget: \$(wget --version | head -n1 | cut -d' ' -f3)
    END_VERSIONS
    """
}

process VALIDATE_FILES {
    conda "conda-forge::md5sum=8.32"
    
    publishDir "${params.outdir}/validation", mode: 'copy'
    
    input:
    path files
    
    output:
    path "file_checksums.txt", emit: checksums
    path "file_sizes.txt", emit: sizes
    
    script:
    """
    # Generate checksums
    md5sum ${files} > file_checksums.txt
    
    # Get file sizes
    ls -lh ${files} > file_sizes.txt
    """
}

workflow {
    // Read FTP URLs from file
    ftp_urls_ch = Channel
        .fromPath(params.ftp_urls)
        .splitText()
        .map { it.trim() }
        .filter { it && !it.startsWith('#') }
    
    // Download files
    DOWNLOAD_FTP_DATA(ftp_urls_ch)
    
    // Validate downloaded files
    VALIDATE_FILES(DOWNLOAD_FTP_DATA.out.downloaded_files.collect())
    
    // Emit results
    DOWNLOAD_FTP_DATA.out.downloaded_files.view { "Downloaded: $it" }
}