# set environment
export PATH=$PATH:/usr/local/openjdk-8/bin

java -classpath "dist/sqsjson2xmlr.jar:lib/*:libaws/*" edu.virginia.sqsjson.SQSQueueDriver

#
# end of file
#
