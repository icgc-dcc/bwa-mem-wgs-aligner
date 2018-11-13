#!/usr/bin/python
import time

"""
This is fairly general approach, it could be a univeral tool that
runs all PCAWG workflows defined in CWL

1. download CWL workflow definition from Dockstore
wget -O pcawg-bwa-mem.cwl dockstore_tool_url

2. generate input yaml template
cwltool --make-template pcawg-bwa-mem.cwl > input.yaml

3. populate input.yaml with parameters provided from the arguments

4. launch cwltool to run workflow

5. report output
"""


time.sleep(20)


with open("output.json", "w") as o:
  o.write('{"merged_output_bam": "aligned.bam"}')