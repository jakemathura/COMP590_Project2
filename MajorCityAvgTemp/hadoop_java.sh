#!/bin/sh -v
# "Parameter $1 is the name prefix used to identify the Job,"
# "Mapper, and Reducer Java files, and the generated Jar file."
javac $1.java
javac $1Mapper.java
javac $1Reducer.java
jar -cvf $1.jar *.class
