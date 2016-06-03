#!/bin/sh
set -x
set -e

echo "Building package"
time mvn package 2>mvn_package.stderr >mvn_package.stdout

echo "Deploying job to HDFS"
hdfs dfs -rm -r -skipTrash /apps/meta_point02/ || true
hdfs dfs -mkdir /apps/meta_point02/
hdfs dfs -chmod a+rwx /apps/meta_point02/
hdfs dfs -rm -skipTrash /apps/meta_point02/meta-point02-1.0.jar || true
hdfs dfs -copyFromLocal ../input1.csv /apps/meta_point02/input1.csv || true

echo "Running job"
time yarn jar target/meta-point02-1.0.jar org.tkorostelev.meta_point02.MetaPoint02 hdfs:///apps/meta_point02/input1.csv col1,col2,col3 hdfs:///apps/meta_point02/result
hdfs dfs -text /apps/meta_point02/result/part-m-* | head -10