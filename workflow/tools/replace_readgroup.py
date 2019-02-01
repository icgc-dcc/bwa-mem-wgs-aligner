#!/usr/bin/env python3
import os
import subprocess
import sys
import json
import time
import shutil

"""
Major steps:
- Assigns all the reads in a file to a single new read-group
"""

task_dict = json.loads(sys.argv[1])

cwd = os.getcwd()

picard = task_dict['input'].get('picard_jar')
input_format = task_dict['input'].get('input_format')
download_files = task_dict['input'].get('download_files')
unaligned_by_rg_dir = task_dict['input'].get('unaligned_by_rg_dir')

with open(task_dict['input'].get('metadata_json'), 'r') as f:
    metadata = json.load(f)

output = {
    'unaligned_rg_replace_dir': None
}

RG_map = { 'ID': 'readGroupId',
           'LB': 'libraryName',
           'PL': 'sequencingPlatform',
           'PU': 'platformUnit',
           'SM': 'aliquotId',
           'PM': 'platformModel',
           'CN': 'sequencingCenter',
           'PI': 'insertSize'}


# the inputs are BAM
if input_format == 'BAM':
    files = metadata.get('files')
    output_dir = os.path.join(cwd, 'unaligned_rg_replace')
    if not os.path.isdir(output_dir): os.makedirs(output_dir)
    output['unaligned_rg_replace_dir'] = output_dir

    for _file in files:
        file_path = _file.get('path')
        file_name = _file.get('name')

        # get all the rg for the _file
        rg_yaml = set()
        rg_replace = {}
        for rg in _file.get('readGroups'):
            rg_yaml.add(rg.get('readGroupIdInFile'))
            rg_replace[rg.get('readGroupIdInFile')] = {}
            for key, value in RG_map.items():
                to_update = {}
                if key == 'SM' and metadata.get(value):
                    to_update = {key: metadata.get(value)}
                if key in ['ID', 'LB', 'PL', 'PU', 'PM', 'CN'] and rg.get(value) or key == 'PI' and isinstance(rg.get(value), int):
                    to_update = {key: rg.get(value)}
                if to_update:
                    rg_replace[rg.get('readGroupIdInFile')].update(to_update)

        for bam_dict in download_files:
            if bam_dict.get('path') == file_path and bam_dict.get('name') == file_name:
                file_with_path = bam_dict.get('local_path')
                break
            sys.exit('\n Error: can not find the downloaded file with matched information in the YAML!')

        # check whether the download files exist
        if not os.path.isfile(file_with_path): sys.exit('\n The downloaded file: %s do not exist!' % file_with_path)

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


        # do the replacement for all readGroups
        for rg_old, rg_new in rg_replace.items():
            rg_args = []
            for key, value in rg_new.items():
                rg_args.append('RG%s=%s' % (key, value))

            try:
                subprocess.run(['java', '-jar', picard,
                                'AddOrReplaceReadGroups', 'I=%s' % os.path.join(unaligned_by_rg_dir, rg_old+'.bam'),
                                'O=%s' % os.path.join(output_dir, rg_new.get('ID')+'.new.bam')] + rg_args, check=True)
            except Exception as e:
                sys.exit('\n%s: ReplaceReadGroups failed: %s' % (e, os.path.join(unaligned_by_rg_dir, rg_old+'.bam')))


elif input_format == 'FASTQ':
    # sleep 60 seconds and pass through the parameters
    time.sleep(60)
    pass

else:
    sys.exit('\n%s: Input files format are not FASTQ or BAM')


with open("output.json", "w") as o:
    o.write(json.dumps(output))

# delete files at the very last moment
if os.path.isdir(unaligned_by_rg_dir): shutil.rmtree(unaligned_by_rg_dir)

for f in download_files:
    if not os.path.isfile(f): continue
    try:
        os.remove(f)
    except Exception as e:
        sys.exit('\n%s: Delete file failed: %s' % (e, f))
