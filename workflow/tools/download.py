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

task_dict = json.loads(sys.argv[1])

cwd = os.getcwd()

# read the json file
with open(task_dict['input'].get('metadata_json'), 'r') as f:
    metadata = json.load(f)

mapping = {
    'collaboratory': 'collab',
    'amazon': 'aws'
}

input_format = task_dict['input'].get('input_format')

output = {
    'download_files': []
}


if input_format == 'BAM':
    # download the file
    files = metadata.get('files')
    for _file in files:
        file_path = _file.get('path')
        file_name = _file.get('fileName')

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
    readGroups = metadata.get('readGroups')
    for rg in readGroups:
        readGroupId = rg.get('readGroupId')
        files = rg.get('files')
        for _file in files:
            file_path = _file.get('path')
            file_name = _file.get('fileName')

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

with open("output.json", "w") as o:
    o.write(json.dumps(output))
