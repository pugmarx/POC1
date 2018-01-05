#!/bin/sh


if [ $# -eq 2 ]
then
    echo "KafkaTopic: "$1
else
    echo "Error!! Format is: $f <KafkaTopic> <file>"
    exit 1
fi

#pv $2 | kafka-console-producer.sh --broker-list ip-172-31-76-34.ec2.internal:9092,ip-172-31-76-34.ec2.internal:9093,ip-172-31-76-34.ec2.internal:9094 --topic $1  > /dev/null
pv $2 | kafka-console-producer.sh --broker-list localhost:9092 --topic $1  > /dev/null
