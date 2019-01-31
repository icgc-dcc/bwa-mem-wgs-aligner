#!/usr/bin/env python3
import yaml
import os
import subprocess
import sys
import json
import time
import datetime

"""
Major steps:
- Adds comments to the header of a BAM file
"""

task_dict = json.loads(sys.argv[1])
input_format = task_dict['input'].get('input_format')

cwd = os.getcwd()

output = {
    'bams': []
}

with open(task_dict['input'].get('metadata_json'), 'r') as f:
    metadata = json.load(f)

# the inputs are BAM
if input_format == 'BAM':
    picard = task_dict['input'].get('picard_jar')
    unaligned_rg_replace_dir = task_dict['input'].get('unaligned_rg_replace_dir')

    rg_args = []
    for ct in ['dccProjectCode', 'submitterDonorId', 'submitterSpecimenId', 'submitterSampleId', 'dccSpecimenType', 'libraryStrategy', 'useCntl']:
        rg_args.append('C=%s:%s' % (ct, metadata.get(ct)))

    files = metadata.get('files')
    output_dir = os.path.join(cwd, 'lane_unaligned')
    if not os.path.isdir(output_dir): os.makedirs(output_dir)

    for _file in files:
        # add comments to lane-level bams
        for rg in _file.get('readGroups'):
            try:
                subprocess.run(['java', '-jar', picard,
                                'AddCommentsToBam', 'I=%s' % os.path.join(unaligned_rg_replace_dir, rg.get('readGroupId')+'.new.bam'),
                                'O=%s' % os.path.join(output_dir, rg.get('readGroupId').replace(':', '_')+'.lane.bam')] + rg_args, check=True)
            except Exception as e:
                sys.exit('\n%s: AddCommentsToBam failed: %s' %(e, os.path.join(unaligned_rg_replace_dir, rg.get('readGroupId')+'.new.bam')))

            try:
                os.remove(os.path.join(unaligned_rg_replace_dir, rg.get('readGroupId')+'.new.bam'))
            except Exception as e:
                sys.exit('\n%s: Delete file failed: %s' % (e, os.path.join(unaligned_rg_replace_dir, rg.get('readGroupId')+'.new.bam')))


            output['bams'].append(os.path.join(output_dir, rg.get('readGroupId').replace(':', '_')+'.lane.bam'))

elif input_format == 'FASTQ':
    time.sleep(60)
    output['bams'] = task_dict['input'].get('bams')

else:
    sys.exit('\n%s: Input files format are not FASTQ or BAM')

output['aligned_bam_basename'] = '.'.join([metadata.get('aliquotId'), str(len(output['bams'])), datetime.date.today().strftime("%Y%m%d"), 'wgs', 'grch37'])

with open("output.json", "w") as o:
    o.write(json.dumps(output))
