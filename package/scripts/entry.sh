# set environment
export PATH=$PATH:/usr/local/openjdk-8/bin

java -classpath "dist/sqsjson2xml.jar:lib/*:libaws/*" edu.virginia.sqsjson.SQSQueueDriver -threads ${SQS_JSON_TRANSFORM_WORKERS}

#
# end of file
#
