#!/usr/bin/python
import time

# just to enable testing for now

time.sleep(10)

with open("output.json", "w") as o:
  o.write('{"bams": ["a.bam", "b.bam"]}')