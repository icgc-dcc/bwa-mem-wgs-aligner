#!/usr/bin/env python3

import os
import subprocess
import sys
import json
import time

"""
Major steps:
- produce an unmapped BAM (uBAM) from a previously aligned BAM
"""

task_dict = json.loads(sys.argv[1])

cwd = os.getcwd()

picard = task_dict['input'].get('picard_jar')
input_format = task_dict['input'].get('input_format')
download_files = task_dict['input'].get('download_files')

with open(task_dict['input'].get('metadata_json'), 'r') as f:
    metadata = json.load(f)

output = {
    'unaligned_by_rg_dir': None
}

# the inputs are BAM
if input_format == 'BAM':
    files = metadata.get('files')
    output_dir = os.path.join(cwd, 'unaligned_by_rg')
    if not os.path.isdir(output_dir): os.makedirs(output_dir)
    output['unaligned_by_rg_dir'] = output_dir

    for _file in files:
        file_path = _file.get('path')
        file_name = _file.get('fileName')

        for bam_dict in download_files:
            if bam_dict.get('path') == file_path and bam_dict.get('name') == file_name:
                file_with_path = bam_dict.get('local_path')
                break
            sys.exit('\n Error: can not find the downloaded file with matched information in the YAML!')

        # check whether the download files exist
        if not os.path.isfile(file_with_path): sys.exit('\n The downloaded file: %s do not exist!' % file_with_path)

        # Revert the bam to unaligned and lane level bam sorted by query name
        try:
            subprocess.run(['java', '-jar', picard,
                            'RevertSam', 'I=%s' % file_with_path,
                            'OUTPUT_BY_READGROUP=true', 'O=%s' % output_dir], check=True)
        except Exception as e:
            sys.exit('\n%s: RevertSam failed: %s' %(e, file_with_path))

    # delete the files at the very last step
    for f in download_files:
        if not os.path.isfile(f): continue
        try:
            os.remove(f)
        except Exception as e:
            sys.exit('\n%s: Delete file failed: %s' % (e, f))


elif input_format == 'FASTQ':
    # sleep 60 seconds and pass through the parameters
    time.sleep(60)
    pass

else:
    sys.exit('\n%s: Input files format are not FASTQ or BAM')


with open("output.json", "w") as o:
    o.write(json.dumps(output))
