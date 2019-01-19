#!/usr/bin/env python3
import os
import subprocess
import sys
import json
import time

"""
Major steps:
- Assigns all the reads in a file to a single new read-group
"""

task_dict = json.loads(sys.argv[1])

cwd = os.getcwd()

picard = task_dict['input'].get('picard_jar')
input_format = task_dict['input'].get('input_format')
download_files = task_dict['input'].get('download_files')
unaligned_by_rg_dir = task_dict['input'].get('unaligned_by_rg_dir')

with open(task_dict['input'].get('metadata_json'), 'r') as f:
    metadata = json.load(f)

output = {
    'unaligned_rg_replace_dir': None
}

# the inputs are BAM
if input_format == 'BAM':
    files = metadata.get('files')
    output_dir = os.path.join(cwd, 'unaligned_rg_replace')
    if not os.path.isdir(output_dir): os.makedirs(output_dir)
    output['unaligned_rg_replace_dir'] = output_dir

    for _file in files:
        file_path = _file.get('path')
        file_name = _file.get('name')

        # get all the rg for the _file
        rg_yaml = set()
        rg_replace = {}
        for rg in _file.get('read_groups'):
            rg_yaml.add(rg.get('rg_id_in_file'))
            rg_replace[rg.get('rg_id_in_file')] = {'ID': rg.get('read_group_id'),
                                                   'LB': rg.get('library_name'),
                                                   'PL': rg.get('sequencing_platform'),
                                                   'PU': rg.get('platform_unit'),
                                                   'SM': metadata.get('aliquot_id'),
                                                   'PM': rg.get('platform_model'),
                                                   'CN': rg.get('sequencing_center'),
                                                   'PI': rg.get('insert_size'),
                                                   'DT': rg.get('sequencing_date')}

        for bam in download_files:
            bam_dict = json.loads(bam)
            if bam_dict.get('path') == file_path and bam_dict.get('name') == file_name:
                file_with_path = bam_dict.get('local_path')
                break

        # check whether the download files exist
        if not os.path.isfile(file_with_path): sys.exit('\n The downloaded file: %s do not exist!' % file_with_path)

        # retrieve the @RG from BAM header
        try:
            header = subprocess.check_output(['samtools', 'view', '-H', file_with_path])

        except Exception as e:
            sys.exit('\n%s: Retrieve BAM header failed: %s' % (e, file_with_path))

        # get @RG
        header_array = header.decode('utf-8').rstrip().split('\n')
        rg_bam = set()
        for line in header_array:
            if not line.startswith("@RG"): continue
            rg_array = line.rstrip().split('\t')[1:]
            for element in rg_array:
                if not element.startswith('ID'): continue
                rg_bam.add(':'.join(element.rstrip().split(':')[1:]))

        # compare the RG ids
        if not rg_yaml == rg_bam: sys.exit('\nThe read groups in metadata do not match with those in BAM!')  # die fast


        # do the replacement for all read_groups
        for rg_old, rg_new in rg_replace.items():
            if 'ID' not in rg_new or str(rg_new.get('ID')) == '':
                sys.exit('Must specify read_group_id for file: %s' % os.path.join(output_dir, rg_old+'.bam'))
            rg_args = ['RGID=%s' % rg_new.get('ID')]
            if 'LB' not in rg_new or str(rg_new.get('LB')) == '':
                sys.exit('Must specify library_name for file: %s' % os.path.join(output_dir, rg_old+'.bam'))
            rg_args.append('RGLB=%s' % rg_new.get('LB'))
            if 'PL' not in rg_new or str(rg_new.get('PL')) == '':
                sys.exit('Must specify sequencing_platform for file: %s' % os.path.join(output_dir, rg_old+'.bam'))
            rg_args.append('RGPL=%s' % rg_new.get('PL'))
            if 'PU' not in rg_new or str(rg_new.get('PU')) == '':
                sys.exit('Must specify platform_unit for file: %s' % os.path.join(output_dir, rg_old+'.bam'))
            rg_args.append('RGPU=%s' % rg_new.get('PU'))
            if 'SM' not in rg_new or str(rg_new.get('SM')) == '':
                sys.exit('Must specify aliquot_id for file: %s' % os.path.join(output_dir, rg_old+'.bam'))
            rg_args.append('RGSM=%s' % rg_new.get('SM'))
            if 'PM' in rg_new and str(rg_new.get('PM')) != '':
                rg_args.append('RGPM=%s' % rg_new.get('PM'))
            if 'CN' in rg_new and str(rg_new.get('CN')) != '':
                rg_args.append('RGCN=%s' % rg_new.get('CN'))
            if 'PI' in rg_new and isinstance(rg_new.get('PI'), int):
                rg_args.append('RGPI=%s' % rg_new.get('PI'))
            if 'DT' in rg_new and re.match('^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}$', str(rg_new.get('DT'))):
                rg_args.append('RGDT=%s' % str(rg_new.get('DT')))

            try:
                subprocess.run(['java', '-jar', picard,
                                'AddOrReplaceReadGroups', 'I=%s' % os.path.join(unaligned_by_rg_dir, rg_old+'.bam'),
                                'O=%s' % os.path.join(output_dir, rg_new.get('ID')+'.new.bam')] + rg_args, check=True)
            except Exception as e:
                sys.exit('\n%s: ReplaceReadGroups failed: %s' % (e, os.path.join(unaligned_by_rg_dir, rg_old+'.bam')))

            try:
                os.remove(os.path.join(unaligned_by_rg_dir, rg_old+'.bam'))
            except Exception as e:
                sys.exit('\n%s: Delete file failed: %s' % (e, os.path.join(unaligned_by_rg_dir, rg_old + '.bam')))

elif input_format == 'FASTQ':
    # sleep 60 seconds and pass through the parameters
    time.sleep(60)
    pass

else:
    sys.exit('\n%s: Input files format are not FASTQ or BAM')


with open("output.json", "w") as o:
    o.write(json.dumps(output))
