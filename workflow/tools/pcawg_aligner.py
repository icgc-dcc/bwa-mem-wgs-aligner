#!/usr/bin/python
import time

# just to enable testing for now

time.sleep(20)

with open("output.json", "w") as o:
  o.write('{"merged_output_bam": "aligned.bam"}')