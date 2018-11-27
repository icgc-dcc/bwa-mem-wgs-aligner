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



if len(sys.argv) != 3:
    sys.exit('\nUsage: %s <metadata.yaml> <picard_jar>' % sys.argv[0])

if not sys.argv[1]:
    sys.exit('\nMust specify metadata.yaml file')

if not sys.argv[2]:
    sys.exit('\nMust specify picard jar')


cwd = os.getcwd()
picard=sys.argv[2]

# read the yaml file
with open(sys.argv[1], 'r') as f:
    input_metadata=yaml.load(f)

mapping = {
    'collaboratory': 'collab',
    'amazon': 'aws'
}

output_bams = {'bams': []}

# detect the input format
input_format=set()
for rg in input_metadata['read_groups']:
    for rg_file in rg['files']:
        input_format.add(rg_file.get('format'))

if not len(input_format) == 1: sys.exit('\n%s: The input files should have the same format.')

# the inputs are BAM
input_format = input_format.pop()
if input_format == 'BAM':
    # reshape the metadata
    metadata=reshape_metadata(input_metadata)

    # download the file
    files = metadata.get('files')
    for _file in files:
        file_path = _file.get('path')
        file_name = _file.get('name')

        if file_path.startswith('song://'):
            storage_site, analysis_id, object_id = file_path.replace('song://', '').split('/')

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

            file_with_path = os.path.join(cwd, file_name)

        elif file_path.startswith('file://'): # file_path provides file_name not only the path information
            file_path = os.path.dirname(file_path.replace('file://', ''))
            if file_path.startswith('/'):
                file_with_path = os.path.join(file_path, file_name)
            else:
                file_with_path = os.path.join(cwd, file_path, file_name)

        else:
            sys.exit('\n Unrecognized file path!')

        # check whether the download files exist
        if not os.path.isfile(file_with_path): sys.exit('\n The downloaded file: %s do not exist!' % file_with_path)

        # get all the rg for the _file
        rg_yaml = set()
        rg_replace = {}
        for rg in _file.get('read_groups'):
            rg_yaml.add(rg.get('rg_id_in_file'))
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
            header = subprocess.check_output(['samtools', 'view', '-H', file_with_path])

        except Exception as e:
            sys.exit('\n%s: Retrieve BAM header failed: %s' % (e, file_with_path))

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
        output_dir = os.path.join(cwd, 'lane_unaligned')
        if not os.path.isdir(output_dir): os.makedirs(output_dir)
        try:
            subprocess.run(['java', '-jar', picard,
                            'RevertSam', 'I=%s' % file_with_path,
                            'OUTPUT_BY_READGROUP=true', 'O=%s' % output_dir], check=True)
        except Exception as e:
            sys.exit('\n%s: RevertSam failed: %s' %(e, file_with_path))


        # do the replacement for all read_groups
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

            try:
                os.remove(os.path.join(output_dir, rg_old+'.bam'))
            except:
                sys.exit('\n%s: Delete file failed: %s' % (e, os.path.join(output_dir, rg_old + '.bam')))

        # add comments to lane-level bams
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
                                'C=use_cntl:%s' % metadata.get('use_cntl', 'N/A')], check=True)
            except Exception as e:
                sys.exit('\n%s: AddCommentsToBam failed: %s' %(e, os.path.join(output_dir, rg.get('read_group_id')+'.bam')))

            try:
                os.remove(os.path.join(output_dir, rg.get('read_group_id')+'.bam'))
            except:
                sys.exit('\n%s: Delete file failed: %s' % (e, os.path.join(output_dir, rg.get('read_group_id')+'.bam')))


            output_bams['bams'].append(os.path.join(output_dir, rg.get('read_group_id')+'.reheader.bam'))

elif input_format == 'FASTQ':
    metadata = input_metadata
    read_groups = metadata.get('read_groups')
    for rg in read_groups:
        read_group_id = rg.get('read_group_id')
        files = rg.get('files')
        file_with_path = []
        for _file in files:
            file_path = _file.get('path')

            if file_path.startswith('song://'):
                storage_site, analysis_id, object_id = file_path.replace('song://', '').split('/')

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

                file_with_path.append(os.path.join(cwd, _file.get('name')))

            elif file_path.startswith('file://'):
                file_path = os.path.dirname(file_path.replace('file://', ''))
                if file_path.startswith('/'):
                    file_with_path.append(os.path.join(file_path, _file.get('name')))
                else:
                    file_with_path.append(os.path.join(cwd, file_path, _file.get('name')))


            else:
                sys.exit('\n Unrecognized file path!')


        # detect whether there are more than two fastq files for each read_group
        if not len(file_with_path) == 2:
            sys.exit('\nThe number of fastq files is not equal to 2 for %s' % read_group_id)

        # check whether the download files exist
        for f in file_with_path:
            if not os.path.isfile(f): sys.exit('\n The downloaded file: %s do not exist!' % f)

        # convert pair end fastq to unaligned and lane level bam sorted by query name
        output_dir = os.path.join(cwd, 'lane_unaligned')
        if not os.path.isdir(output_dir): os.makedirs(output_dir)

        try:
            subprocess.run(['java', '-jar', picard,
                            'FastqToSam', 'FASTQ=%s' % file_with_path[0],
                            'FASTQ2=%s' % file_with_path[1],
                            'OUTPUT=%s' % os.path.join(output_dir, read_group_id + '.bam'),
                            'READ_GROUP_NAME=%s' % read_group_id,
                            'SAMPLE_NAME=%s' % metadata.get('submitter_sample_id'),
                            'LIBRARY_NAME=%s' % rg.get('library_name'),
                            'PLATFORM_UNIT=%s' % rg.get('platform_unit'),
                            'PLATFORM=%s' % rg.get('sequencing_platform'),
                            'SEQUENCING_CENTER=%s' % rg.get('sequencing_center'),
                            'PREDICTED_INSERT_SIZE=%s' % rg.get('insert_size'),
                            'PLATFORM_MODEL=%s' % rg.get('platform_model'),
                            'RUN_DATE=%s' % rg.get('sequencing_date'),
                            'COMMENT=dcc_project_code:%s' % metadata.get('dcc_project_code'),
                            'COMMENT=submitter_donor_id:%s' % metadata.get('submitter_donor_id'),
                            'COMMENT=submitter_specimen_id:%s' % metadata.get('submitter_specimen_id'),
                            'COMMENT=submitter_sample_id:%s' % metadata.get('submitter_sample_id'),
                            'COMMENT=dcc_specimen_type:%s' % metadata.get('dcc_specimen_type'),
                            'COMMENT=library_strategy:%s' % metadata.get('library_strategy'),
                            'COMMENT=use_cntl:%s' % metadata.get('use_cntl', 'N/A')], check=True)
        except Exception as e:
            sys.exit('\n%s: FastqToSam failed: %s and %s' % (e, file_with_path[0], file_with_path[1]))

        output_bams['bams'].append(os.path.join(output_dir, read_group_id + '.bam'))

else:
    sys.exit('\n%s: Input files format are not FASTQ or BAM')


with open("output.json", "w") as o:
    o.write(json.dumps(output_bams))
