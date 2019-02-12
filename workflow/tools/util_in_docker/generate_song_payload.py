#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import yaml
from overture_song_payload import DonorPayload
from overture_song_payload import ExperimentPayload
from overture_song_payload import FilePayload
from overture_song_payload import SpecimenPayload
from overture_song_payload import SamplePayload
from overture_song_payload import SongPayload
import re, os
import uuid
import tarfile

def main():
    """ Main program """
    parser = argparse.ArgumentParser(description='Convert yaml file to song payload')
    parser.add_argument('yaml_file', type=argparse.FileType('r'))
    parser.add_argument('bam_file')
    parser.add_argument('bai_file')
    parser.add_argument('tar_file')
    parser.add_argument('lane_unaligned_dir')
    parser.add_argument('oxog_metrics_dir')
    parser.add_argument('multiple_metrics_dir')
    results = parser.parse_args()

    with open(os.path.join(results.multiple_metrics_dir,'multiple_metrics.insert_size_metrics'),'r') as fp:
        insert_size_metrics = load_insert_size_metrics(fp)
    quality_yield_metrics_dir = results.lane_unaligned_dir
    yaml_data = yaml.load(results.yaml_file)

    song_payload = SongPayload(
        analysis_id=None,
        analysis_type=get_analysis_type(yaml_data),
        info={
            'isPcawg': get_analysis_info_ispcawg(),
            'dataset': get_analysis_info_dataset()
        },
        experiment_payload=ExperimentPayload(
            aligned=get_experiment_aligned(yaml_data),
            reference_genome=get_experiment_reference_genome(yaml_data),
            library_strategy=get_experiment_library_strategy(yaml_data),
            paired_end=get_experiment_paired_end(open(os.path.join(results.multiple_metrics_dir, 'multiple_metrics.alignment_summary_metrics'),'r')),
            info={
                'MEDIAN_INSERT_SIZE': int(float(insert_size_metrics.get('MEDIAN_INSERT_SIZE'))),
                'MODE_INSERT_SIZE': int(float(insert_size_metrics.get('MODE_INSERT_SIZE'))),
                'MEDIAN_ABSOLUTE_DEVIATION': int(float(insert_size_metrics.get('MEDIAN_ABSOLUTE_DEVIATION'))),
                'MIN_INSERT_SIZE': int(float(insert_size_metrics.get('MIN_INSERT_SIZE'))),
                'MAX_INSERT_SIZE': int(float(insert_size_metrics.get('MAX_INSERT_SIZE'))),
                'STANDARD_DEVIATION': int(float(insert_size_metrics.get('STANDARD_DEVIATION'))),
                'MEAN_INSERT_SIZE': int(float(insert_size_metrics.get('MEAN_INSERT_SIZE'))),
                'READ_PAIRS': int(float(insert_size_metrics.get('READ_PAIRS'))),
                'PAIR_ORIENTATION': str(insert_size_metrics.get('PAIR_ORIENTATION')),
                'readGroups': get_experiment_read_groups(yaml_data, quality_yield_metrics_dir),
            }
        ),
        study=get_study(yaml_data),
        sample_payloads=[
            SamplePayload(
                donor_payload=DonorPayload(
                    donor_gender=get_donor_gender(yaml_data),
                    donor_submitter_id=get_donor_submitter_id(yaml_data),
                    study_id=get_donor_study_id(yaml_data),
                ),
                sample_submitter_id=get_sample_submitter_id(yaml_data),
                sample_type=get_sample_type(),
                specimen_payload=SpecimenPayload(
                    specimen_class=get_specimen_class(yaml_data),
                    specimen_submitter_id=get_specimen_submitter_id(yaml_data),
                    specimen_type=get_specimen_type(yaml_data),
                    info={}
                ),
                info={
                    'aliquot_id': get_sample_info_aliquot_id(yaml_data),
                    'matched_control_sample': get_sample_info_matched_control_sample(yaml_data),
                }
            )
        ],
        file_payloads=get_files(results.bam_file, results.bai_file, results.tar_file)
    )

    print(song_payload.to_json())

    return 0

def get_files(bam_file, bai_file, tar_file):
    file_payloads = []
    for file in [bam_file,bai_file]:
        file_payloads.append(FilePayload(
            file_access='controlled',
            file_name=os.path.basename(file),
            md5sum=FilePayload.calculate_md5(file),
            file_size=FilePayload.calculate_size(file),
            file_type=FilePayload.retrieve_file_type(file),
            info={}
        ))

    file_payloads.append(FilePayload(
        file_access='controlled',
        file_name=os.path.basename(tar_file),
        md5sum=FilePayload.calculate_md5(tar_file),
        file_size=FilePayload.calculate_size(tar_file),
        #TODO
        file_type=FilePayload.retrieve_file_type(tar_file),
        info={
            'description':"The tar.gz file contains various QC metrics",
            'tarContent': [
                {
                    'path': 'unaligned_seq_qc',
                    'files': filter_starts_with('unaligned_seq_qc',list_files_tar_gz(tarfile.open(tar_file,'r:gz'))),
                    'description': "Quality yield metrics from unaligned lane level sequences, reported by Picard tools"
                },
                {
                    'path': 'oxog_metrics',
                    'files': filter_starts_with('oxog_metrics',list_files_tar_gz(tarfile.open(tar_file,'r:gz'))),
                    'description': "OxoG metrics reported by Picard tools"
                },
                {
                    'path': 'aligned_bam_qc',
                    'files': filter_starts_with('aligned_bam_qc',list_files_tar_gz(tarfile.open(tar_file,'r:gz'))),
                    'description': 'Multiple metrics of aligned BAM reported by Picard tools'
                }
            ]

        }
    ))

    return file_payloads

def list_files_tar_gz(fp_gz):
    files = []
    for member in fp_gz.getmembers():
        f = fp_gz.extractfile(member)
        if f is not None:
            files.append(member.name)
    return list(set(files))

def filter_starts_with(prefix, needle):
    result = []
    for s in needle:
        if str(s).startswith(prefix):
            result.append(s)
    return result

def get_experiment_paired_end(metrics_fp):
    categories = retrieve_alignment_summary_metrics_pairs(metrics_fp)
    if 'FIRST_OF_PAIR' in categories and 'SECOND_OF_PAIR': return True
    return False

def retrieve_alignment_summary_metrics_pairs(metrics_fp):
    categories = []
    record = False
    for line in metrics_fp.readlines():
        if str(line).startswith('## METRICS CLASS'):
            record = True
            continue
        if record:
            categories.append(str(line).split('\t')[0])
    return categories

def load_insert_size_metrics(metrics_fp):
    return parse_insert_size_metrics(metrics_fp)

def parse_insert_size_metrics(metrics_fp):
    lines = []
    record = False
    for line in metrics_fp.readlines():
        if str(line).startswith("## METRICS CLASS"):
            record = True
            continue

        if str(line).startswith("## HISTOGRAM"):
            record = False
            continue

        if record:
            if not re.match(r'^\s*$', line):
                lines.append(line.strip('\n').split('\t'))
    for i in range(1,len(lines)):
        parsed_line = dict(zip(lines[0], lines[i]))
        if parsed_line['LIBRARY'] == '' and parsed_line['READ_GROUP']=='' and parsed_line['SAMPLE']=='':
            return parsed_line
    raise Exception("The metrics cloud not be found.")

def load_quality_yield_metrics(metrics_dir, read_group):
    metrics_file = os.path.join(metrics_dir,retrieve_metrics_file(metrics_dir,read_group))
    with open(metrics_file,'r') as fp:
        return parse_quality_yield_metrics(fp)

def parse_quality_yield_metrics(metrics_fp):
    lines = []
    for line in metrics_fp.readlines():
        if not str(line).startswith('#'):
            if not re.match(r'^\s*$', line):
                lines.append(line.rstrip().split('\t'))
    return dict(zip(lines[0], lines[1]))

def retrieve_metrics_file(metrics_directory, read_group):
    file_full_path = os.path.join(metrics_directory,read_group+'.lane.bam.quality_yield_metrics.txt')
    for file in os.listdir(metrics_directory):
        if file.startswith(read_group) and file.endswith('.lane.bam.quality_yield_metrics.txt'):
            return file
    raise Exception("The metrics file %s does not exist." % (file_full_path))

def get_study(yaml_data):
    return yaml_data.get('study')

def get_analysis_type(yaml_data):
    return "sequencingRead"

def get_analysis_info_ispcawg():
    return False

def get_analysis_info_dataset():
    return ['PCAWG2']

def get_sample_info_aliquot_id(yaml_data):
    return yaml_data.get('aliquotId')

def get_sample_info_matched_control_sample(yaml_data):
    try:
        uuid.UUID(yaml_data.get('useCntl'))
        return yaml_data.get('useCntl')
    except ValueError:
        return None

def get_sample_submitter_id(yaml_data):
    return yaml_data.get('sampleSubmitterId')

def get_sample_type():
    return "DNA"

def get_specimen_submitter_id(yaml_data):
    return yaml_data.get('specimenSubmitterId')

def get_specimen_class(yaml_data):
    if 'normal' in str(yaml_data.get('specimenType')).lower(): return 'Normal'
    if 'tumour' in str(yaml_data.get('specimenType')).lower(): return 'Tumour'
    raise Exception('Cannot determine if '+yaml_data.get('specimenType')+' is Tumour or Normal.')

def get_specimen_type(yaml_data):
    return yaml_data.get('specimenType')

def get_donor_submitter_id(yaml_data):
    return yaml_data.get('donorSubmitterId')

def get_donor_study_id(yaml_data):
    return yaml_data.get('study')

def get_donor_gender(yaml_data):
    return yaml_data.get('donorGender')

def get_experiment_aligned(yaml_data):
    return True

def get_experiment_alignment_tool(yaml_data):
    return "BWA MEM"

def get_experiment_insert_size(yaml_data):
    return -1

def get_experiment_library_strategy(yaml_data):
    return "WGS"

def get_experiment_reference_genome(yaml_data):
    return "GRCh37"

def get_experiment_read_groups(yaml_data, quality_yield_metrics_dir):
    read_groups = []
    for read_group in yaml_data.get('readGroups'):
        metrics = load_quality_yield_metrics(quality_yield_metrics_dir,read_group.get('readGroupId'))
        read_groups.append({
            'totalReads':int(metrics.get('TOTAL_READS')),
            'pfReads':int(metrics.get('PF_READS')),
            'readLength':int(metrics.get('READ_LENGTH')),
            'totalBases':int(metrics.get('TOTAL_BASES')),
            'pfBases':int(metrics.get('PF_BASES'))
        })
    return read_groups


if __name__ == "__main__":
    main()