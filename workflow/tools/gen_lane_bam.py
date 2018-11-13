#!/usr/bin/python
import time

"""
Example input metadata YAML: https://github.com/icgc-dcc/pcawg2pilot/blob/master/tests/preprocessing/20181007/PEME-CA/WGS/CGP_donor_1199131/PD3851a/metadata.yaml

dcc_project_code: PEME-CA
submitter_donor_id: CGP_donor_1199131
submitter_specimen_id: CGP_specimen_1142534
submitter_sample_id: PD3851a
dcc_specimen_type: Normal - blood derived
library_strategy: WGS
read_groups:  # read group IDs must match what's in the BAM file
- read_group_id: WTSI-9399_7
  sequencing_center: WTSI
  sequencing_platform: ILLUMINA
  platform_model: Illumina HiSeq 2000
  platform_unit: WTSI-9399
  library_name: WGS:WTSI:28085
  insert_size: 453
  sequencing_date: 2013-03-17T20:00:00-04:00
  files:
  - name: 7a60604579c5874c480836b3a6d2f9ef.180288_icgc.bam
    size: 23243533
    rg_id_in_file: foo  # optional, if populated it must match what's in the BAM
    md5sum: d41d8cd98f00b204e9800998ecf8427e
    path: file:///absolute/path/to/file  # or relative from this current dir
    reference_genome:  # required for CRAM
    format: BAM  # BAM or CRAM
- read_group_id: WTSI-9399_8
  sequencing_center: WTSI
  sequencing_platform: ILLUMINA
  platform_model: Illumina HiSeq 2000
  platform_unit: WTSI-9399
  library_name: WGS:WTSI:28085
  insert_size: 453
  sequencing_date: 2013-03-17T20:00:00-04:00
  files:
  - name: 7a60604579c5874c480836b3a6d2f9ef.180288_icgc.bam
    size: 23243533
    rg_id_in_file: bar
    md5sum: d41d8cd98f00b204e9800998ecf8427e
    path: song://collaboratory/efcf90ee-53ae-4f9f-b29a-e0a83ca70272/6329334b-dcd5-53c8-98fd-9812ac386d30
    reference_genome:  # required for CRAM
    format: BAM  # BAM or CRAM


Major steps:
- identify all input files and their associated read group(s), pairing of paired-end FASTQ
  files must be properly recorded
- convert BAM to FASTQ for starting files are BAM
- convert FASTQ to BAM for each read group
"""




# just to enable testing for now

time.sleep(10)

with open("output.json", "w") as o:
  o.write('{"bams": ["a.bam", "b.bam"]}')
