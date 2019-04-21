#!/bin/sh -v
# Parameter $1 is the name of the Jar file and the class with main()
# Parameter $2 is the HDFS input file path
# Parameter $3 is the HDFS output directory path
hdfs dfs -rm -R $3
yarn jar $1.jar $1 $2 $3
