#!/usr/bin/env python3
import yaml
import os
import subprocess
import sys
import json

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

def reshape_metadata(input_metadata):
    output_metadata = {}
    for key, value in input_metadata.items():
        if not key == 'read_groups':
            output_metadata[key] = value
            continue
        output_files = {}
        for rg in input_metadata['read_groups']:
            output_rg = {}
            for k, v in rg.items():
                if not k == 'files':
                    output_rg[k] = v
                    continue
                for fn in rg['files']:
                    if not fn['name'] in output_files:
                        output_files[fn['name']] = {}
                        output_files[fn['name']]['read_groups'] = []
                    for fk, fv in fn.items():
                        if not fk == 'rg_id_in_file':
                            output_files[fn['name']][fk] = fv
                            continue
                        output_rg[fk] = fv
            output_files[fn['name']]['read_groups'].append(output_rg)
    output_metadata['files'] = []
    for key, value in output_files.items():
        output_metadata['files'].append(value)
    return output_metadata

picard=os.environ.get('PICARD')
cwd = os.getcwd()

if len(sys.argv) != 2:
    sys.exit('Usage: %s <metadata.yaml>' % sys.argv[0])

if not sys.argv[1]:
    sys.exit('Must specify metadata.yaml file')

# read the yaml file
with open(sys.argv[1], 'r') as f:
    input_metadata=yaml.load(f)

# reshape the metadata
metadata=reshape_metadata(input_metadata)


# download the file
mapping = {
    'collaboratory': 'collab',
    'amazon': 'aws'
}

files = metadata.get('files')
for _file in files:
    file_path = _file.get('path')
    file_name = _file.get('name')
    storage_site, analysis_id, object_id = file_path.strip('song://').split('/')

    try:
        subprocess.run(['score-client',
                                 '--profile', mapping.get(storage_site),
                                 'download',
                                 '--object-id', object_id,
                                 '--output-dir', cwd,
                                 '--index', 'false',
                                 '--force'], check=True)
    except Exception as e:
        sys.exit('\n%s: Download object failed: %s' % (e, object_id))

    # get all the rg for the _file
    rg_yaml = set()
    rg_replace = {}
    for rg in _file.get('read_groups'):
        rg_yaml.add(rg.get('rg_id_in_file'))
        if not rg.get('rg_id_in_file') == rg.get('read_group_id'):
            rg_replace[rg.get('rg_id_in_file')] = {'ID': rg.get('read_group_id'),
                                                   'LB': rg.get('library_name'),
                                                   'PL': rg.get('sequencing_platform'),
                                                   'PU': rg.get('platform_unit'),
                                                   'SM': metadata.get('submitter_sample_id'),
                                                   'PM': rg.get('platform_model'),
                                                   'CN': rg.get('sequencing_center'),
                                                   'PI': rg.get('insert_size'),
                                                   'DT': rg.get('sequencing_date')}

    # retrieve the @RG from BAM header
    try:
        header = subprocess.check_output(['samtools', 'view', '-H', os.path.join(cwd, file_name)])

    except Exception as e:
        sys.exit('\n%s: Retrieve BAM header failed: %s' % (e, os.path.join(cwd, file_name)))

    # get @RG
    header_array = header.decode('utf-8').rstrip().split('\n')
    rg_bam = set()
    for line in header_array:
        if not line.startswith("@RG"): continue
        rg_array = line.rstrip().split('\t')[1:]
        for element in rg_array:
            if not element.startswith('ID'): continue
            rg_bam.add(':'.join(element.rstrip().split(':')[1:]))

    # compare the RG ids
    if not rg_yaml == rg_bam: sys.exit('\nThe read groups in metadata do not match with those in BAM!')  # die fast

    # Revert the bam to unaligned and lane level bam sorted by query name
    output_dir = os.path.join(cwd, object_id, 'lane_unaligned')
    if not os.path.isdir(output_dir): os.makedirs(output_dir)
    try:
        subprocess.run(['java', '-jar', picard,
                        'RevertSam', 'I=%s' % os.path.join(cwd, file_name),
                        'OUTPUT_BY_READGROUP=true', 'O=%s' % output_dir], check=True)
    except Exception as e:
        sys.exit('\n%s: RevertSam failed: %s' %(e, os.path.join(cwd, file_name)))


    # detect if read_group replacement are needed
    if rg_replace: # need to replace
        for rg_old, rg_new in rg_replace.items():
            try:
                subprocess.run(['java', '-jar', picard,
                                'AddOrReplaceReadGroups', 'I=%s' % os.path.join(output_dir, rg_old+'.bam'),
                                'O=%s' % os.path.join(output_dir, rg_new.get('ID')+'.bam'),
                                'RGID=%s' % rg_new.get('ID'), 'RGLB=%s' % rg_new.get('LB'), 'RGPL=%s' % rg_new.get('PL'),
                                'RGPU=%s' % rg_new.get('PU'), 'RGSM=%s' % rg_new.get('SM'), 'RGPM=%s' % rg_new.get('PM'),
                                'RGCN=%s' % rg_new.get('CN'), 'RGPI=%s' % rg_new.get('PI'), 'RGDT=%s' % rg_new.get('DT')], check=True)
            except Exception as e:
                sys.exit('\n%s: ReplaceReadGroups failed: %s' % (e, os.path.join(output_dir, rg_old+'.bam')))


    # no need to replace
    # add comments to lane-level bams
    output_bams = {'bams': []}
    for rg in _file.get('read_groups'):
        try:
            subprocess.run(['java', '-jar', picard,
                            'AddCommentsToBam', 'I=%s' % os.path.join(output_dir, rg.get('read_group_id')+'.bam'),
                            'O=%s' % os.path.join(output_dir, rg.get('read_group_id')+'.reheader.bam'),
                            'C=dcc_project_code:%s' % metadata.get('dcc_project_code'),
                            'C=submitter_donor_id:%s' % metadata.get('submitter_donor_id'),
                            'C=submitter_specimen_id:%s' % metadata.get('submitter_specimen_id'),
                            'C=submitter_sample_id:%s' % metadata.get('submitter_sample_id'),
                            'C=dcc_specimen_type:%s' % metadata.get('dcc_specimen_type'),
                            'C=library_strategy:%s' % metadata.get('library_strategy'),
                            'C=use_cntl:%s' % metadata.get('use_cntl', None)], check=True)
        except Exception as e:
            sys.exit('\n%s: AddCommentsToBam failed: %s' %(e, os.path.join(output_dir, rg.get('read_group_id')+'.bam')))

        output_bams['bams'].append(os.path.join(output_dir, rg.get('read_group_id')+'.reheader.bam'))


    with open("output.json", "w") as o:
      o.write(json.dumps(output_bams))
