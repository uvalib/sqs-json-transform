# set environment
export PATH=$PATH:/usr/local/openjdk-8/bin

java -classpath "dist/sqsjson2xml.jar:lib/*:libaws/*" edu.virginia.sqsjson.SQSQueueDriver

#
# end of file
#
