#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import tarfile
import os
import fnmatch

def main():
    """ Main program """
    parser = argparse.ArgumentParser(description='Convert yaml file to song payload')
    parser.add_argument('output')
    parser.add_argument('dirs', nargs='+')
    results = parser.parse_args()

    with tarfile.open(results.output,'w:gz') as tar:
        for dir in results.dirs:
            outside_tar_name = dir.split(':')[0]
            inside_tar_name = dir.split(':')[1]
            tar.add(outside_tar_name, arcname=inside_tar_name)
            for file in os.listdir(outside_tar_name):
                if fnmatch.fnmatch(file,dir.split(':')[2]):
                    tar.add(os.path.join(outside_tar_name,file),arcname=os.path.join(inside_tar_name,file))

if __name__ == "__main__":
    main()