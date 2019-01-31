#!/usr/bin/env python3
import yaml
import os
import subprocess
import sys
import json
import re

"""
Major steps:
- identify all input files and their associated read group(s), pairing of paired-end FASTQ
  files must be properly recorded
- download the files
"""

def reshape_metadata(input_metadata):
    output_metadata = {}
    for key, value in input_metadata.items():
        if not key == 'readGroups':
            output_metadata[key] = value
            continue
        output_files = {}
        for rg in input_metadata['readGroups']:
            output_rg = {}
            for k, v in rg.items():
                if not k == 'files':
                    output_rg[k] = v
                    continue
                for fn in rg['files']:
                    if not fn['name'] in output_files:
                        output_files[fn['name']] = {}
                        output_files[fn['name']]['readGroups'] = []
                    for fk, fv in fn.items():
                        if not fk == 'readGroupIdInFile':
                            output_files[fn['name']][fk] = fv
                            continue
                        output_rg[fk] = fv
            output_files[fn['name']]['readGroups'].append(output_rg)
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
fields_to_check = ['dccProjectCode', 'submitterDonorId', 'submitterSpecimenId', 'submitterSampleId', 'aliquotId', 'dccSpecimenType', 'libraryStrategy', 'useCntl', 'readGroups']
for field_name in fields_to_check:
    if field_name not in input_metadata.keys() or not input_metadata.get(field_name):
        sys.exit('The metadata YAML must contain and specify field: %s' % field_name)
    if field_name == 'useCntl':
        if 'normal' not in input_metadata.get('dccSpecimenType').lower() and \
                not re.match(r'^([a-f\d]{8}(-[a-f\d]{4}){3}-[a-f\d]{12})$', str(input_metadata['useCntl'])):
            sys.exit('Must specify useCntl for Tumor in metadata YAML file as UUID')
        elif 'normal' in input_metadata.get('dccSpecimenType').lower() and not input_metadata['useCntl'] == 'N/A':
            sys.exit('Must specify useCntl for Normal in metadata YAML file as N/A')
    if field_name == 'aliquotId' and not re.match(r'^([a-f\d]{8}(-[a-f\d]{4}){3}-[a-f\d]{12})$', str(input_metadata['aliquotId'])):
        sys.exit('Must specify aliquotId in UUID format!')

# readGroups fields validate
rg_fields_to_check = ['readGroupId', 'sequencingPlatform', 'platformUnit', 'libraryName', 'files']
# files fields validate
file_fields_to_check = ['name', 'size', 'readGroupIdInFile', 'md5sum', 'path', 'format']

for readGroup in input_metadata['readGroups']:
    for rg_field in rg_fields_to_check:
        if rg_field not in readGroup.keys() or not readGroup.get(rg_field):
            sys.exit('The metadata YAML must contain readGroup field: %s' % rg_field)
        elif rg_field == 'files':
            for fileInfo in readGroup['files']:
                for file_field in file_fields_to_check:
                    if file_field not in fileInfo.keys() or not fileInfo.get(file_field):
                        sys.exit('The metadata YAML must contain file field: %s' % file_field)



mapping = {
    'collaboratory': 'collab',
    'amazon': 'aws'
}

# detect the input format
input_format=set()
for rg in input_metadata['readGroups']:
    for rg_file in rg['files']:
        input_format.add(rg_file.get('format'))

if not len(input_format) == 1: sys.exit('\nError: The input files should have the same format.')

# the inputs are BAM
input_format = input_format.pop()

output = {
    'download_files': [],
    'input_format': input_format,
    'metadata_json': None
}


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
            file_path_dir = os.path.dirname(file_path.replace('file://', ''))
            if file_path_dir.startswith('/'):
                file_with_path = os.path.join(file_path_dir, file_name)
            else:
                file_with_path = os.path.join(cwd, file_path_dir, file_name)

        else:
            sys.exit('\n Unrecognized file path!')

        file_info = {
            'name': file_name,
            'path': file_path,
            'local_path': file_with_path
        }
        output['download_files'].append(file_info)


elif input_format == 'FASTQ':
    metadata = input_metadata

    readGroups = metadata.get('readGroups')
    for rg in readGroups:
        readGroupId = rg.get('readGroupId')
        files = rg.get('files')
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

                file_with_path = os.path.join(cwd, _file.get('name'))

            elif file_path.startswith('file://'):
                file_path = os.path.dirname(file_path.replace('file://', ''))
                if file_path.startswith('/'):
                    file_with_path = os.path.join(file_path, _file.get('name'))
                else:
                    file_with_path = os.path.join(cwd, file_path, _file.get('name'))

            else:
                sys.exit('\n Unrecognized file path!')

            file_info = {
                'name': file_name,
                'path': file_path,
                'local_path': file_with_path
            }
            output['download_files'].append(file_info)


else:
    sys.exit('\n%s: Input files format are not FASTQ or BAM')

# write to the metadata json file
with open('metadata.json', 'w') as f:
    f.write(json.dumps(metadata, indent=2))

output['metadata_json'] = os.path.join(cwd, 'metadata.json')

with open("output.json", "w") as o:
    o.write(json.dumps(output))
