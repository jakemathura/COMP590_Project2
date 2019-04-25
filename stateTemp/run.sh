./hadoop_java.sh CountryOverTime
./yarn_mapred.sh CountryOverTime /user/mayura/GlobalLandTemperaturesByState1.txt /user/mayura/output
hdfs dfs -getmerge /user/mayura/output countryover_results
cat countryover_results

./hadoop_java.sh StateOverTime
./yarn_mapred.sh StateOverTime /user/mayura/GlobalLandTemperaturesByState1.txt /user/mayura/output
hdfs dfs -getmerge /user/mayura/output stateover_results
cat stateover_results

./hadoop_java.sh AvgTempByState
./yarn_mapred.sh AvgTempByState /user/mayura/GlobalLandTemperaturesByState1.txt /user/mayura/output
hdfs dfs -getmerge /user/mayura/output avgtemp_results
cat avgtemp_results
