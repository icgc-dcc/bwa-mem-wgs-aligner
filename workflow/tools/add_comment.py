#!/usr/bin/env python3
import yaml
import os
import subprocess
import sys
import json
import time
import datetime
import shutil

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

case_map = {
    'study': 'dcc_project_code',
    'donorSubmitterId': 'submitter_donor_id',
    'specimenSubmitterId': 'submitter_specimen_id',
    'sampleSubmitterId': 'submitter_sample_id',
    'specimenType': 'dcc_specimen_type',
    'libraryStrategy': 'library_strategy',
    'useCntl': 'use_cntl'
}

# the inputs are BAM
if input_format == 'BAM':
    picard = task_dict['input'].get('picard_jar')
    unaligned_rg_replace_dir = task_dict['input'].get('output_dir')

    rg_args = []
    for ct in ['study', 'donorSubmitterId', 'specimenSubmitterId', 'sampleSubmitterId', 'specimenType', 'libraryStrategy', 'useCntl']:
        rg_args.append('C=%s:%s' % (case_map.get(ct), metadata.get(ct)))

    for f in glob.glob(os.path.join(unaligned_rg_replace_dir, '*.bam')):
        # add comments to lane-level bams
        try:
            subprocess.run(['java', '-jar', picard,
                            'AddCommentsToBam',
                            'I=%s' % f),
                            'O=%s' % os.path.join(cwd, os.path.basename(f))] + rg_args, check=True)
        except Exception as e:
            sys.exit('\n%s: AddCommentsToBam failed: %s' %(e, f))

        output['bams'].append(os.path.join(cwd, os.path.basename(f)))

    # delete the files at the very last moment
    for f in glob.glob(os.path.join(unaligned_rg_replace_dir, "*.bam")):
        os.remove(f)

elif input_format == 'FASTQ':
    output['bams'] = task_dict['input'].get('bams')

else:
    sys.exit('\n%s: Input files format are not FASTQ or BAM')

output['aligned_bam_basename'] = '.'.join([metadata.get('aliquotId'), str(len(output['bams'])), datetime.date.today().strftime("%Y%m%d"), 'wgs', 'grch38'])

with open("output.json", "w") as o:
    o.write(json.dumps(output))
