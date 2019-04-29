#!/usr/bin/env python3

import os
import subprocess
import sys
import json

task_dict = json.loads(sys.argv[1])

cwd = os.getcwd()

bwa_mem_aligner_docker = task_dict['input'].get('bwa_mem_aligner_docker')
lane_bams = task_dict['input'].get('lane_bams')
reference_gz = task_dict['input'].get('reference_gz')
reference_gz_fai = task_dict['input'].get('reference_gz_fai')
reference_gz_alt = task_dict['input'].get('reference_gz_alt')
reference_gz_bwt = task_dict['input'].get('reference_gz_bwt')
reference_gz_ann = task_dict['input'].get('reference_gz_ann')
reference_gz_pac = task_dict['input'].get('reference_gz_pac')
reference_gz_sa = task_dict['input'].get('reference_gz_sa')
reference_gz_amb = task_dict['input'].get('reference_gz_amb')

# pull docker image
subprocess.run(['docker', 'pull', '%s' % bwa_mem_aligner_docker])

aligned_lane_bam_prefix = 'grch38-aligned'
output_bams = []
for bam in lane_bams:
    try:
        subprocess.run(['docker', 'run', '--rm',
                        '--user', '1000:1000',
                        '--workdir', '/output',
                        '-v', '%s:/output:rw' % cwd,
                        '-v', '%s:/data/%s:ro' % (reference_gz, os.path.basename(reference_gz)),
                        '-v', '%s:/data/%s:ro' % (reference_gz_fai, os.path.basename(reference_gz_fai)),
                        '-v', '%s:/data/%s:ro' % (reference_gz_alt, os.path.basename(reference_gz_alt)),
                        '-v', '%s:/data/%s:ro' % (reference_gz_bwt, os.path.basename(reference_gz_bwt)),
                        '-v', '%s:/data/%s:ro' % (reference_gz_ann, os.path.basename(reference_gz_ann)),
                        '-v', '%s:/data/%s:ro' % (reference_gz_pac, os.path.basename(reference_gz_pac)),
                        '-v', '%s:/data/%s:ro' % (reference_gz_sa, os.path.basename(reference_gz_sa)),
                        '-v', '%s:/data/%s:ro' % (reference_gz_amb, os.path.basename(reference_gz_amb)),
                        '-v', '%s:/data/%s:ro' % (bam, os.path.basename(bam)),
                        '%s' % bwa_mem_aligner_docker,
                        'bwa-mem-aligner.py',
                        '-i', '/data/%s' % os.path.basename(bam),
                        '-o', '/output/%s.%s' % (aligned_lane_bam_prefix, os.path.basename(bam)),
                        '-r', '/data/%s' % os.path.basename(reference_gz)
                        ], check=True)
    except Exception as e:
        sys.exit('BWA MEM failed, input: %s' % bam)

    output_bams.append('%s.%s' % (aligned_lane_bam_prefix, os.path.basename(bam)))

with open("output.json", "w") as o:
  json.dump({
    'output_dir': cwd,
    'aligned_lane_bam_names': output_bams
  }, o)
