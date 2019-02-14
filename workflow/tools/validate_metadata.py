#!/usr/bin/env python3
import yaml
import os
import sys
import json
import re

"""
Major steps:
- validate the input metadata YAML
"""

def reshape_metadata(input_metadata):
    output_metadata = {}
    output_files = {}
    for key, value in input_metadata.items():
        if not key == 'readGroups':
            output_metadata[key] = value
            continue
        # process readGroups
        for rg in input_metadata['readGroups']:
            output_rg = {}
            for k, v in rg.items():
                if not k == 'files':
                    output_rg[k] = v
                    continue
                for fn in rg['files']:
                    if not fn['fileName'] in output_files:
                        output_files[fn['fileName']] = {}
                        output_files[fn['fileName']]['readGroups'] = []
                    for fk, fv in fn.items():
                        if not fk == 'readGroupIdInFile':
                            output_files[fn['fileName']][fk] = fv
                            continue
                        output_rg[fk] = fv
            output_files[fn['fileName']]['readGroups'].append(output_rg)
    output_metadata['files'] = []
    for key, value in output_files.items():
        output_metadata['files'].append(value)
    return output_metadata


task_dict = json.loads(sys.argv[1])

cwd = os.getcwd()

# read the yaml file
with open(task_dict['input'].get('metadata_yaml'), 'r') as f:
    input_metadata=yaml.load(f)

# metadata validate
fields_to_check = ['study', 'donorSubmitterId', 'specimenSubmitterId', 'sampleSubmitterId', 'aliquotId', 'specimenType', 'libraryStrategy', 'useCntl', 'readGroups']
for field_name in fields_to_check:
    if field_name not in input_metadata.keys() or not input_metadata.get(field_name):
        sys.exit('The metadata YAML must contain and specify field: %s' % field_name)
    if field_name == 'useCntl':
        if 'normal' not in input_metadata.get('specimenType').lower() and \
                not re.match(r'^([a-f\d]{8}(-[a-f\d]{4}){3}-[a-f\d]{12})$', str(input_metadata['useCntl'])):
            sys.exit('Must specify useCntl for Tumor in metadata YAML file as UUID')
        elif 'normal' in input_metadata.get('specimenType').lower() and not input_metadata['useCntl'] == 'N/A':
            sys.exit('Must specify useCntl for Normal in metadata YAML file as N/A')
    if field_name == 'aliquotId' and not re.match(r'^([a-f\d]{8}(-[a-f\d]{4}){3}-[a-f\d]{12})$', str(input_metadata['aliquotId'])):
        sys.exit('Must specify aliquotId in UUID format!')

# readGroups fields validate
rg_fields_to_check = ['readGroupId', 'sequencingPlatform', 'platformUnit', 'libraryName', 'files']
# files fields validate
file_fields_to_check = ['fileName', 'fileSize', 'readGroupIdInFile', 'fileMd5sum', 'path', 'fileType']

for readGroup in input_metadata['readGroups']:
    for rg_field in rg_fields_to_check:
        if rg_field not in readGroup.keys() or not readGroup.get(rg_field):
            sys.exit('The metadata YAML must contain readGroup field: %s' % rg_field)
        elif rg_field == 'files':
            for fileInfo in readGroup['files']:
                for file_field in file_fields_to_check:
                    if file_field not in fileInfo.keys() or not fileInfo.get(file_field):
                        sys.exit('The metadata YAML must contain file field: %s' % file_field)


# detect the input format
input_format = set()
for rg in input_metadata['readGroups']:
    for rg_file in rg['files']:
        input_format.add(rg_file.get('fileType'))

if not len(input_format) == 1: sys.exit('\nError: The input files should have the same format.')

# the inputs are BAM
input_format = input_format.pop()

output = {'input_format': input_format}


if input_format == 'BAM':
    # reshape the metadata
    metadata=reshape_metadata(input_metadata)

elif input_format == 'FASTQ':
    metadata = input_metadata

else:
    sys.exit('\n%s: Input files format are not FASTQ or BAM')


aliquot_id = input_metadata['aliquotId']
number_of_lanes = len(input_metadata['readGroups'])
study = input_metadata['study']

aws_allowed_studies = { 'LIRI-JP', 'PACA-CA', 'PRAD-CA', 'RECA-EU', 'PAEN-AU',
                        'PACA-AU', 'BOCA-UK','OV-AU', 'MELA-AU', 'BRCA-UK',
                        'PRAD-UK', 'CMDI-UK', 'LINC-JP', 'ORCA-IN', 'BTCA-SG',
                        'LAML-KR', 'LICA-FR', 'CLLE-ES', 'ESAD-UK', 'PAEN-IT' }
aws_upload_allowed = study in aws_allowed_studies

cgc_allowed_studies = { 'LIRI-JP', 'PACA-CA', 'PRAD-CA', 'RECA-EU', 'PAEN-AU',
                        'PACA-AU', 'BOCA-UK','OV-AU', 'MELA-AU', 'BRCA-UK',
                        'PRAD-UK', 'CMDI-UK', 'LINC-JP', 'ORCA-IN', 'BTCA-SG',
                        'LAML-KR', 'LICA-FR', 'CLLE-ES', 'ESAD-UK', 'PAEN-IT' }
cgc_upload_allowed = study in cgc_allowed_studies

output.update({
        'aliquot_id': aliquot_id,
        'number_of_lanes': str(number_of_lanes),
        'study': study,
        'aws_upload_allowed': str(aws_upload_allowed),
        'cgc_upload_allowed': str(cgc_upload_allowed),
        'collab_upload_allowed': str(True)  # always true for collab
    })

# write to the metadata json file
with open('metadata.json', 'w') as f:
    f.write(json.dumps(metadata, indent=2))

output['metadata_json'] = os.path.join(cwd, 'metadata.json')

with open("output.json", "w") as o:
    o.write(json.dumps(output))
