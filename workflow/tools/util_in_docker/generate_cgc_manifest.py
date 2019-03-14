#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import pathlib
import os

def main():
    """ Main program """
    parser = argparse.ArgumentParser(description='Convert SONG payload to CGC manifest file')
    parser.add_argument('--filenames',dest='filenames', nargs='+',required=True)
    parser.add_argument('--song-payload',dest='song_payload', type=argparse.FileType('r'), required=True)
    parser.add_argument('--output',dest='output', default="manifest.txt")

    results = parser.parse_args()

    with open(results.output,'w') as output_fp:
        payload = json.load(results.song_payload)
        output_fp.write(','.join(['File name','experimental_strategy','case_id','aliquot_uuid','case_submitter_id','sample_class','study','ftype'])+'\n')
        for filename in results.filenames:
            output_fp.write(','.join([
                os.path.basename(filename),
                payload.get('experiment').get('libraryStrategy'),
                payload.get('sample')[0].get('donor').get('donorSubmitterId'),
                payload.get('sample')[0].get('info').get('aliquotId'),
                payload.get('sample')[0].get('donor').get('donorSubmitterId'),
                payload.get('sample')[0].get('specimen').get('specimenClass'),
                payload.get('study'),
                pathlib.Path(filename).suffix[1:]
            ])+'\n')

if __name__ == "__main__":
    main()