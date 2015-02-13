#!/bin/sh

y=2014
m=02

for d in {1..23}; do

d=`printf "%0*d" 2 $d`

./mm_trackTorqueJobs.py -l 1 -a -m -d ${y}${m}${d}

done
