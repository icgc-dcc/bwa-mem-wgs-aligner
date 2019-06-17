#!/usr/bin/env python3

import os
import subprocess
import sys
import json
import glob

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
    'bams': []
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

        # Revert the bam to unaligned and lane level bam sorted by query name
        # Suggested options from: https://github.com/broadinstitute/picard/issues/849#issuecomment-313128088
        try:
            subprocess.run(['java', '-jar', picard,
                            'RevertSam',
                            'I=%s' % file_with_path,
                            'SANITIZE=true',
                            'ATTRIBUTE_TO_CLEAR=XT',
                            'ATTRIBUTE_TO_CLEAR=XN',
                            'ATTRIBUTE_TO_CLEAR=AS',
                            'ATTRIBUTE_TO_CLEAR=OC',
                            'ATTRIBUTE_TO_CLEAR=OP',
                            'SORT_ORDER=queryname',
                            'RESTORE_ORIGINAL_QUALITIES=true',
                            'REMOVE_DUPLICATE_INFORMATION=true',
                            'REMOVE_ALIGNMENT_INFORMATION=true',
                            'OUTPUT_BY_READGROUP=true',
                            'VALIDATION_STRINGENCY=LENIENT',
                            'O=%s' % cwd], check=True)
        except Exception as e:
            sys.exit('\n%s: RevertSam failed: %s' %(e, file_with_path))

        for filename in glob.glob(os.path.join(cwd, "*.bam")):
            # convert readGroupId to filename friendly
            readGroupId = os.path.basename(filename).replace(".bam", "")
            rg_fname = "".join([c if re.match(r"[a-zA-Z0-9\-_]", c) else "_" for c in readGroupId])
            os.rename(filename, os.path.join(cwd, rg_fname+".lane.bam"))
            output['bams'].append(os.path.join(cwd, rg_fname+".lane.bam")


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
    output['bams'] = task_dict['input'].get('bams')

else:
    sys.exit('\n%s: Input files format are not FASTQ or BAM')


output['aligned_bam_basename'] = '.'.join([metadata.get('aliquotId'), str(len(output['bams'])), datetime.date.today().strftime("%Y%m%d"), 'wgs', 'grch38'])


with open("output.json", "w") as o:
    o.write(json.dumps(output))
