#!/usr/bin/env python3
import yaml
import os
import subprocess
import sys
import json
import time

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

# the inputs are BAM
if input_format == 'BAM':
    picard = task_dict['input'].get('picard_jar')
    unaligned_rg_replace_dir = task_dict['input'].get('unaligned_rg_replace_dir')

    with open(task_dict['input'].get('metadata_json'), 'r') as f:
        metadata = json.load(f)

    files = metadata.get('files')
    output_dir = os.path.join(cwd, 'lane_unaligned')
    if not os.path.isdir(output_dir): os.makedirs(output_dir)

    for _file in files:
        # add comments to lane-level bams
        for rg in _file.get('read_groups'):
            try:
                subprocess.run(['java', '-jar', picard,
                                'AddCommentsToBam', 'I=%s' % os.path.join(unaligned_rg_replace_dir, rg.get('read_group_id')+'.new.bam'),
                                'O=%s' % os.path.join(output_dir, rg.get('read_group_id').replace(':', '_')+'.lane.bam'),
                                'C=dcc_project_code:%s' % metadata.get('dcc_project_code'),
                                'C=submitter_donor_id:%s' % metadata.get('submitter_donor_id'),
                                'C=submitter_specimen_id:%s' % metadata.get('submitter_specimen_id'),
                                'C=submitter_sample_id:%s' % metadata.get('submitter_sample_id'),
                                'C=dcc_specimen_type:%s' % metadata.get('dcc_specimen_type'),
                                'C=library_strategy:%s' % metadata.get('library_strategy'),
                                'C=use_cntl:%s' % metadata.get('use_cntl', 'NA')], check=True)
            except Exception as e:
                sys.exit('\n%s: AddCommentsToBam failed: %s' %(e, os.path.join(unaligned_rg_replace_dir, rg.get('read_group_id')+'.new.bam')))

            try:
                os.remove(os.path.join(unaligned_rg_replace_dir, rg.get('read_group_id')+'.new.bam'))
            except Exception as e:
                sys.exit('\n%s: Delete file failed: %s' % (e, os.path.join(unaligned_rg_replace_dir, rg.get('read_group_id')+'.new.bam')))


            output['bams'].append(os.path.join(output_dir, rg.get('read_group_id').replace(':', '_')+'.lane.bam'))

elif input_format == 'FASTQ':
    output['bams'] = task_dict['input'].get('bams')

else:
    sys.exit('\n%s: Input files format are not FASTQ or BAM')


with open("output.json", "w") as o:
    o.write(json.dumps(output))
