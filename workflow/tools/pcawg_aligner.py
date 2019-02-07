#!/usr/bin/env python3

import argparse
import subprocess
import yaml
import json
import os
import sys

"""
This is fairly general approach, it could be a univeral tool that
runs all PCAWG workflows defined in CWL

1. generate input yaml template
cwltool --make-template pcawg-bwa-mem.cwl > input.yaml

2. populate input.yaml with parameters provided from the arguments

3. launch cwltool to run workflow

4. report output
"""

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('--cwl_file', dest='cwl_file', help='CWL file')
parser.add_argument('--output_dir', dest='output_dir', type=str, help='Output directory', required=True)
parser.add_argument('--reads', dest='reads', nargs='+', help='List of reads', required=True)
parser.add_argument('--output_file_basename', dest='output_file_basename', type=str, help='Output file basename', required=True)
parser.add_argument('--download_reference_files', dest='download_reference_files', action='store_true', help='Download reference files', default=False)
parser.add_argument('--reference_gz_amb', dest='reference_gz_amb', help='Reference genome amb', required=True)
parser.add_argument('--reference_gz_sa', dest='reference_gz_sa', help='Reference genome sa', required=True)
parser.add_argument('--reference_gz_pac', dest='reference_gz_pac', help='Reference genome pac', required=True)
parser.add_argument('--reference_gz_ann', dest='reference_gz_ann', help='Reference genome ann', required=True)
parser.add_argument('--reference_gz_bwt', dest='reference_gz_bwt', help='Reference genome bwt', required=True)
parser.add_argument('--reference_gz_fai', dest='reference_gz_fai', help='Reference genome fai', required=True)
parser.add_argument('--reference_gz', dest='reference_gz', help='Reference genome gz', required=True)


args = parser.parse_args()

with open('input.yaml', 'w') as f:
  subprocess.call(['cwltool', '--make-template', args.cwl_file], stdout=f)

input_json = yaml.load(open('input.yaml'))
input_json['reference_gz_sa']['path'] = args.reference_gz_sa
input_json['reference_gz_pac']['path'] = args.reference_gz_pac
input_json['reference_gz_fai']['path'] = args.reference_gz_fai
input_json['reference_gz_bwt']['path'] = args.reference_gz_bwt
input_json['reference_gz_ann']['path'] = args.reference_gz_ann
input_json['reference_gz_amb']['path'] = args.reference_gz_amb
input_json['reference_gz']['path'] = args.reference_gz
input_json['reads'] = []

for read in args.reads:
  input_json['reads'].append({'class':'File','path':read})

input_json['output_file_basename'] = args.output_file_basename
input_json['output_dir'] = args.output_dir
input_json['download_reference_files'] = str(args.download_reference_files).lower()

input_json['reference_gz_sa']['path'] = args.reference_gz_sa
with open('job.json', 'w') as fp:
  json.dump(input_json,fp, indent=4, sort_keys=True)

subprocess.check_output(['cwltool', '--non-strict', '--debug', args.cwl_file, 'job.json'])

output_path = os.path.join(os.getcwd(), args.output_file_basename)

with open("output.json", "w") as o:
  json.dump({
    'output_dir': os.getcwd(),
    'merged_output_bai': output_path+'.bam.bai',
    'merged_output_unmapped_metrics': output_path+'.unmapped.bam.metrics',
    'merged_output_bam': output_path+'.bam',
    'merged_output_metrics': output_path+'.bam.metrics',
    'merged_output_unmapped_bai': output_path+'.unmapped.bam.bai',
    'merged_output_unmapped_bam': output_path+'.unmapped.bam'
  },o)

#delete the lane level bams at the last moment
for read in args.reads:
  if not os.path.isfile(read): continue
  try:
    os.remove(read)
  except Exception as e:
    sys.exit('\n%s: Delete file failed: %s' % (e, read))
