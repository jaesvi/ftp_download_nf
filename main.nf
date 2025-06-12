#!/usr/bin/env nextflow
nextflow.enable.dsl = 2

// Parameter validation
if (!params.ftp_urls) {
    error "Please provide --ftp_urls parameter pointing to your URLs file"
}

if (!params.outdir) {
    error "Please provide --outdir parameter pointing to your output directory"
}

pprocess DOWNLOAD_FTP_DATA {
    conda "conda-forge::aria2=1.37.0"
    
    publishDir "${params.outdir}/raw_data", mode: 'copy'
    
    input:
    val ftp_url
    
    output:
    path "*.{fastq.gz,fq.gz,txt,csv,tsv}", emit: downloaded_files
    path "versions.yml", emit: versions
    
    script:
    def filename = ftp_url.split('/').last()
    """
    # Use aria2c for parallel, resumable downloads
    aria2c \\
        --max-connection-per-server=4 \\
        --split=4 \\
        --min-split-size=1M \\
        --continue=true \\
        --retry-wait=3 \\
        --max-tries=5 \\
        --timeout=60 \\
        --connect-timeout=30 \\
        "${ftp_url}" \\
        --out="${filename}"
    
    # Verify download
    if [ ! -s "${filename}" ]; then
        echo "Error: Failed to download ${ftp_url}"
        exit 1
    fi
    
    echo "Successfully downloaded: ${filename} (\$(du -h ${filename} | cut -f1))"
    
    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        aria2: \$(aria2c --version | head -n1 | cut -d' ' -f3)
    END_VERSIONS
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
        
    // Emit results
    DOWNLOAD_FTP_DATA.out.downloaded_files.view { "Downloaded: $it" }
}