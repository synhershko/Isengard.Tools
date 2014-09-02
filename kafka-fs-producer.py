#!/usr/bin/env python

# This code uses https://github.com/mumrah/kafka-python , you will need to have it installed
#
#

import sys
import fnmatch
import os.path
from os.path import basename
from kafka.client import KafkaClient
from kafka.consumer import SimpleConsumer
from kafka.producer import SimpleProducer, KeyedProducer

def getFilesToWorkOn(inputPath):
    files = [] # TODO still need to support XMLs
    if os.path.isfile(inputPath) and inputPath.endswith(".csv"):
        files.append(inputPath)
    elif os.path.isdir(inputPath):
        for inputFile in os.listdir(inputPath):
            if fnmatch.fnmatch(inputFile, '*.csv'):
                files.append(inputPath+inputFile)
    return files

def sendFromCsv(producer, f):
    for line in f:
		# Note that the application is responsible for encoding messages to type str
		response = producer.send_messages("csv-data", l)
		#producer.send_messages("my-topic", u'你怎么样?'.encode('utf-8')) # Send unicode message

		if response:
    		print(response[0].error)
    		print(response[0].offset)

# This method is able to read text files with concatenated XMLs in it and send
# individual XMLs as messages
def sendFromXml(producer, f):
	str_list = []
    for line in f:
    	str_list.append(line)

    	if line == "</xml>":
			response = producer.send_messages("xml-data", ''.join(str_list))
			str_list = []
			if response:
	    		print(response[0].error)
    			print(response[0].offset)


def producerMain(args):
	kafka = KafkaClient(args[0])

	# Send messages synchronously in batches. This will collect
	# messages in batch and send them to Kafka after 20 messages are
	# collected or every 60 seconds
	# Notes:
	# * If the producer dies before the messages are sent, there will be losses
	# * Call producer.stop() to send the messages and cleanup
	producer = SimpleProducer(kafka, batch_send=True,
                          batch_send_every_n=20,
                          batch_send_every_t=60)

	# To wait for acknowledgements
	# ACK_AFTER_LOCAL_WRITE : server will wait till the data is written to
	#                         a local log before sending response
	# ACK_AFTER_CLUSTER_COMMIT : server will block until the message is committed
	#                            by all in sync replicas before sending a response
	#producer = SimpleProducer(kafka, async=False,
    #                      req_acks=SimpleProducer.ACK_AFTER_LOCAL_WRITE,
    #                      ack_timeout=2000)


	for path in args[1:]:
		files = getFilesToWorkOn(parth)
		for filename in files:
			f = open(filename, "r")
			if filename.endswith(".csv"):
				sendFromCsv(producer, f)
			elif filename.endswith(".xml"):
				sendFromXml(producer, f)

    producer.stop() # send the remaining batched messages and cleanup
	kafka.close()

	return

# Usage: this.py kafkabrokerhost:9092 path1 [path2 ...]
if __name__ == "__main__":
   producerMain(sys.argv[1:])