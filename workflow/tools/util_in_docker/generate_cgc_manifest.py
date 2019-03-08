#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json

def main():
    """ Main program """
    parser = argparse.ArgumentParser(description='Convert SONG payload to CGC manifest file')
    parser.add_argument('--filename',dest='filename', required=True)
    parser.add_argument('--song-payload',dest='song_payload', type=argparse.FileType('r'), required=True)
    parser.add_argument('--output',dest='output', required=True)

    results = parser.parse_args()

    with open(results.output,'w') as output_fp:
        payload = json.load(results.song_payload)
        output_fp.write(','.join(['File name','experimental_strategy','case_id','aliquot_uuid','case_submitter_id','sample_class','study'])+'\n')
        output_fp.write(','.join([
            results.filename,
            payload.get('experiment').get('libraryStrategy'),
            payload.get('sample').get('donor').get('donorId'),
            payload.get('sample').get('info').get('aliquotId'),
            payload.get('sample').get('donor').get('donorSubmitterId'),
            payload.get('sample').get('specimen').get('specimenClass'),
            payload.get('study'),
        ]))

if __name__ == "__main__":
    main()