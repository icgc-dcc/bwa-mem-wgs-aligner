#!/usr/bin/env python3
import os
import subprocess
import sys
import json
import time
import glob

"""
Major steps:
- Assigns all the reads in a file to a single new read-group
"""

task_dict = json.loads(sys.argv[1])

cwd = os.getcwd()

picard = task_dict['input'].get('picard_jar')
input_format = task_dict['input'].get('input_format')
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
           'PI': 'insertSize',
           'DT': 'sequencingDate'}


# the inputs are BAM
if input_format == 'BAM':
    files = metadata.get('files')
    output_dir = os.path.join(cwd, 'unaligned_rg_replace')
    if not os.path.isdir(output_dir): os.makedirs(output_dir)
    output['unaligned_rg_replace_dir'] = output_dir

    for _file in files:
        file_path = _file.get('path')
        file_name = _file.get('fileName')

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
                if key in ['ID', 'LB', 'PL', 'PU', 'PM', 'CN', 'DT'] and rg.get(value) or key == 'PI' and isinstance(rg.get(value), int):
                    to_update = {key: rg.get(value)}
                # if key == 'DT' and re.match('^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}$', str(rg.get(value))):
                #     to_update = {key: rg.get(value)}
                if to_update:
                    rg_replace[rg.get('readGroupIdInFile')].update(to_update)

        # do the replacement for all readGroups
        for rg_old, rg_new in rg_replace.items():
            rg_args = []
            for key, value in rg_new.items():
                rg_args.append('RG%s=%s' % (key, value))

            try:
                subprocess.run(['java', '-jar', picard,
                                'AddOrReplaceReadGroups',
                                'VALIDATION_STRINGENCY=LENIENT',
                                'I=%s' % os.path.join(unaligned_by_rg_dir, rg_old+'.bam'),
                                'O=%s' % os.path.join(output_dir, rg_new.get('ID')+'.new.bam')] + \
                                rg_args, check=True)
            except Exception as e:
                sys.exit('\n%s: ReplaceReadGroups failed: %s' % (e, os.path.join(unaligned_by_rg_dir, rg_old+'.bam')))

    # delete input bam files at the very last moment
    if os.path.isdir(unaligned_by_rg_dir):
        for f in glob.glob(os.path.join(unaligned_by_rg_dir, "*.bam")):
            os.remove(f)

elif input_format == 'FASTQ':
    pass

else:
    sys.exit('\n%s: Input files format are not FASTQ or BAM')


with open("output.json", "w") as o:
    o.write(json.dumps(output))