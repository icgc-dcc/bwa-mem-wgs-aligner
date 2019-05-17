#!/usr/bin/env python3
import os
import subprocess
import sys
import json
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
    'unaligned_rg_replace_dir': cwd
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

    for _file in files:
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
                if to_update:
                    rg_replace[rg.get('readGroupIdInFile')].update(to_update)

    # do the replacement for all readGroups
    for f in glob.glob(os.path.join(unaligned_by_rg_dir, '*.bam')):
        # retrieve the @RG from BAM header
        try:
            header = subprocess.check_output(['samtools', 'view', '-H', args.input_bam])

        except Exception as e:
            sys.exit('\n%s: Retrieve BAM header failed: %s' % (e, args.input_bam))

        # get @RG and RGID
        header_array = header.decode('utf-8').rstrip().split('\n')
        rg_array = []
        for line in header_array:
            if not line.startswith("@RG"): continue
            rg_array.append(line.rstrip())

        if not len(rg_array) == 1: sys.exit(
            '\n%s: The input bam should only contain one readgroup ID: %s' % args.input_bam)

        for element in rg_array[0].split("\t"):
            if not element.startswith('ID'): continue
            rg_old = element.replace("ID:", "")

        rg_args = []
        for key, value in rg_replace.get(rg_old):
            rg_args.append('RG%s=%s' % (key, value))

        try:
            subprocess.run(['java', '-jar', picard,
                            'AddOrReplaceReadGroups',
                            'VALIDATION_STRINGENCY=LENIENT',
                            'I=%s' % f,
                            'O=%s' % os.path.join(cwd, os.path.basename(f))] + \
                            rg_args, check=True)
        except Exception as e:
            sys.exit('\n%s: ReplaceReadGroups failed: %s' % (e, f))

    # delete input bam files at the very last moment
    for f in glob.glob(os.path.join(unaligned_by_rg_dir, "*.bam")):
        os.remove(f)

elif input_format == 'FASTQ':
    pass

else:
    sys.exit('\n%s: Input files format are not FASTQ or BAM')


with open("output.json", "w") as o:
    o.write(json.dumps(output))