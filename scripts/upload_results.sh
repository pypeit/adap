#!/bin/bash
# Copy log file
cp $3 $1/$2/complete/reduce/

# Copy PypeIt results and scorecard
cd $1
for rel_path in $2/complete/reduce*
do
   s3_path="s3://pypeit/adap/raw_data_reorg/$rel_path/"
   aws --endpoint http://rook-ceph-rgw-nautiluss3.rook s3 cp --no-progress --recursive $rel_path/ $s3_path
done
