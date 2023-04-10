# -*- coding: utf-8 -*-
"""

@author: Hugo Fajardo
This program connects to Apache Kafka and reads the content of a registered topic.
Processes the content of the topic in Apache Flink.

"""

import logging
import sys
import re
import string

from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema

def TwitterProcessing():

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    # write all the data to one file
    env.set_parallelism(1)

    env.add_jars("file:///C:/kafka/kafka_2.12-3.2.0/libs/flink-sql-connector-kafka-1.15.2.jar")
    
    kafka_props = {'bootstrap.servers': '192.168.0.2:9092', 'group.id': 'Twitter'}
    
    deserialization_schema = SimpleStringSchema()

    kafka_consumer = FlinkKafkaConsumer('Twitter',  deserialization_schema, kafka_props)

    kafka_consumer.set_start_from_latest()

    ds = env.add_source(kafka_consumer)

    def split(line):
        yield from line.split()
    
    # compute word count
    ds = ds.flat_map(split) \
           .map(word_munge) \
           .map(lambda i: (i.lower(), 1), output_type=Types.TUPLE([Types.STRING(), Types.INT()])) \
           .key_by(lambda i: i[0]) \
           .reduce(lambda i, j: (i[0], i[1] + j[1])) \
           
    
    ds.print()
    print("_____________________________________________")
    
    # submit for execution
    env.execute()

def word_munge(single_word):                                                                                                                               
    lower_case_word=single_word.lower()                                                                                                                    
    return re.sub(f"[{re.escape(string.punctuation)}]", "", lower_case_word)

if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.ERROR, format="%(message)s")
    
    TwitterProcessing()
