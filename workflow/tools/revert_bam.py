#!/usr/bin/env python3

import os
import subprocess
import sys
import json
from multiprocessing import cpu_count

"""
Major steps:
- produce an unmapped BAM (uBAM) from a previously aligned BAM
"""

task_dict = json.loads(sys.argv[1])

cwd = os.getcwd()
ncpu = cpu_count()

input_format = task_dict['input'].get('input_format')
download_files = task_dict['input'].get('download_files')


with open(task_dict['input'].get('metadata_json'), 'r') as f:
    metadata = json.load(f)

output = {
    'output_dir': cwd
}

# the inputs are BAM
if input_format == 'BAM':
    files = metadata.get('files')

    for _file in files:
        file_path = _file.get('path')
        file_name = _file.get('fileName')

        for bam_dict in download_files:
            if bam_dict.get('path') == file_path and bam_dict.get('name') == file_name:
                file_with_path = bam_dict.get('local_path')
                break
            sys.exit('Error: can not find the input BAM file specified in metadata YAML: %s, %s' % (file_name, file_path))

        # check whether the download files exist
        if not os.path.isfile(file_with_path): sys.exit('\n The downloaded file: %s do not exist!' % file_with_path)

        cmd = 'samtools split -f "%*_%#.bam" -@ %s %s' % (ncpu, file_with_path)
        # Revert the bam to unaligned and lane level bam
        print('command: %s' % cmd)
        stdout, stderr, p, success = '', '', None, True
        try:
            p = subprocess.Popen([cmd],
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE,
                                 shell=True)
            stdout, stderr = p.communicate()
        except Exception as e:
            print('Execution failed: %s' % e, file=sys.stderr)
            success = False

        if p and p.returncode != 0:
            print('Execution failed, none zero code returned.', file=sys.stderr)
            success = False

        print(stdout.decode("utf-8"))
        print(stderr.decode("utf-8"), file=sys.stderr)

        if not success:
            sys.exit(p.returncode if p.returncode else 1)

    # delete the files at the very last step
    for file_dict in download_files:
        if not os.path.isfile(file_dict.get('local_path')):
            continue
        # remove only when the file is downloaded into another task dir of the same job
        if file_dict.get('local_path').split(os.sep)[:-2] != cwd.split(os.sep)[:-1]:
            continue
        try:
            os.remove(file_dict.get('local_path'))
        except Exception as e:
            sys.exit('\n%s: Delete file failed: %s' % (e, file_dict.get('local_path')))


elif input_format == 'FASTQ':
    pass

else:
    sys.exit('\n%s: Input files format are not FASTQ or BAM')


with open("output.json", "w") as o:
    o.write(json.dumps(output))
