#!/usr/bin/env python3
import yaml
import os
import subprocess
import sys
import json

"""
Major steps:
- identify all input files and their associated read group(s), pairing of paired-end FASTQ
  files must be properly recorded
- download the files
"""

def reshape_metadata(input_metadata):
    output_metadata = {}
    for key, value in input_metadata.items():
        if not key == 'read_groups':
            output_metadata[key] = value
            continue
        output_files = {}
        for rg in input_metadata['read_groups']:
            output_rg = {}
            for k, v in rg.items():
                if not k == 'files':
                    output_rg[k] = v
                    continue
                for fn in rg['files']:
                    if not fn['name'] in output_files:
                        output_files[fn['name']] = {}
                        output_files[fn['name']]['read_groups'] = []
                    for fk, fv in fn.items():
                        if not fk == 'rg_id_in_file':
                            output_files[fn['name']][fk] = fv
                            continue
                        output_rg[fk] = fv
            output_files[fn['name']]['read_groups'].append(output_rg)
    output_metadata['files'] = []
    for key, value in output_files.items():
        output_metadata['files'].append(value)
    return output_metadata


task_dict = json.loads(sys.argv[1])

cwd = os.getcwd()

# read the yaml file
with open(task_dict.get('metadata_yaml'), 'r') as f:
    input_metadata=yaml.load(f)

mapping = {
    'collaboratory': 'collab',
    'amazon': 'aws'
}

# detect the input format
input_format=set()
for rg in input_metadata['read_groups']:
    for rg_file in rg['files']:
        input_format.add(rg_file.get('format'))

if not len(input_format) == 1: sys.exit('\n%s: The input files should have the same format.')

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
            file_path = os.path.dirname(file_path.replace('file://', ''))
            if file_path.startswith('/'):
                file_with_path = os.path.join(file_path, file_name)
            else:
                file_with_path = os.path.join(cwd, file_path, file_name)

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

    read_groups = metadata.get('read_groups')
    for rg in read_groups:
        read_group_id = rg.get('read_group_id')
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
