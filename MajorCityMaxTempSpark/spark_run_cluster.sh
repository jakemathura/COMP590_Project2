# parameter $1 is the name of the Java class with main() and
# also the name of the jar file.
# parameter $2 is the name of the HDFS input file
# parameter $3 is the name of the HDFS output DIRECTORY
hdfs dfs -rm -R $3
spark-submit --deploy-mode cluster --class $1 --master yarn $1.jar $2 $3
