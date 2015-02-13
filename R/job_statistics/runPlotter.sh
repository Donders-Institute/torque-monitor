#!/bin/bash
#
# Wrapper script for cron job to call "Rscript plotter.R"
#

p=`dirname $0`
db_topdir=$1
m=`date +%Y%m`

source /cvmfs/dccn.nl/setup.sh
module load R/3.0.3
Rscript $p/plotter.R $db_topdir $m 
