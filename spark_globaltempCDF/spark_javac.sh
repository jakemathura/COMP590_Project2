# parameter $1 is the name of the Java class with main() and
# also the name of the jar file.
rm *.class
javac $1.java
jar cf $1.jar *.class

