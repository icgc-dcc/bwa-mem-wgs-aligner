#!/usr/bin/env python3

import os
import subprocess
import sys
import json
import time
import re

"""
Major steps:
- convert FASTQ to unaligned BAM for each read group
"""

task_dict = json.loads(sys.argv[1])

cwd = os.getcwd()

picard = task_dict['input'].get('picard_jar')
input_format = task_dict['input'].get('input_format')
download_files = task_dict['input'].get('download_files')

with open(task_dict['input'].get('metadata_json'), 'r') as f:
    metadata = json.load(f)

output = {
    'bams': []
}


if input_format == 'FASTQ':
    readGroups = metadata.get('readGroups')
    for rg in readGroups:
        readGroupId = rg.get('readGroupId')
        files = rg.get('files')
        file_with_path = []
        for _file in files:
            file_path = _file.get('path')
            file_name = _file.get('name')

            for fastq in download_files:
                if fastq.get('path') == file_path and fastq.get('name') == file_name:
                    file_with_path.append(fastq.get('local_path'))


        # detect whether there are more than two fastq files for each read_group
        if not len(file_with_path) == 2:
            sys.exit('\nThe number of fastq files is not equal to 2 for %s' % readGroupId)

        # check whether the download files exist
        for f in file_with_path:
            if not os.path.isfile(f): sys.exit('\n The downloaded file: %s do not exist!' % f)

        rg_args = ['READ_GROUP_NAME=%s' % readGroupId,
                   'SAMPLE_NAME=%s' % metadata.get('aliquotId'),
                   'LIBRARY_NAME=%s' % rg.get('libraryName'),
                   'PLATFORM_UNIT=%s' % rg.get('platformUnit'),
                   'PLATFORM=%s' % rg.get('sequencingPlatform')]
        if rg.get('sequencingCenter') and str(rg.get('sequencingCenter')) != '':
            rg_args.append('SEQUENCING_CENTER=%s' % rg.get('sequencingCenter'))
        if rg.get('insertSize') and isinstance(rg.get('insertSize'), int):
            rg_args.append('PREDICTED_INSERT_SIZE=%s' % rg.get('insertSize'))
        if rg.get('platformModel') and str(rg.get('platformModel')) != '':
            rg_args.append('PLATFORM_MODEL=%s' % rg.get('platformModel'))

        for ct in ['dccProjectCode', 'submitterDonorId', 'submitterSpecimenId', 'submitterSampleId', 'dccSpecimenType', 'libraryStrategy', 'useCntl']:
            rg_args.append('COMMENT=%s:%s' % (ct, metadata.get(ct)))


        # convert pair end fastq to unaligned and lane level bam sorted by query name
        output_dir = os.path.join(cwd, 'lane_unaligned')
        if not os.path.isdir(output_dir): os.makedirs(output_dir)

        try:
            subprocess.run(['java', '-jar', picard,
                            'FastqToSam', 'FASTQ=%s' % file_with_path[0],
                            'FASTQ2=%s' % file_with_path[1],
                            'OUTPUT=%s' % os.path.join(output_dir, readGroupId.replace(':', '_') + '.lane.bam')] + rg_args, check=True)
        except Exception as e:
            sys.exit('\n%s: FastqToSam failed: %s and %s' % (e, file_with_path[0], file_with_path[1]))

        output['bams'].append(os.path.join(output_dir, readGroupId.replace(':', '_') + '.lane.bam'))

# the inputs are BAM
elif input_format == 'BAM':
    # sleep 60 seconds and pass through
    time.sleep(60)
    pass

else:
    sys.exit('\n%s: Input files format are not FASTQ or BAM')


with open("output.json", "w") as o:
    o.write(json.dumps(output))
