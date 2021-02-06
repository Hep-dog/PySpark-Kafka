from __future__ import print_function
import sys
import EventData
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming.kafka import TopicAndPartition

def spot_decoder(s):
    if s is None:
        return None
    return s

def getID( msg ):
    data = EventData.EventData.GetRootAsEventData( msg, 0 )
    ID = data.PulseId()
    return( ID )

def getValue( msg ):
    data = EventData.EventData.GetRootAsEventData( msg, 0 )
    return( data )

def displayRDD( rdd ):
    idList = rdd.collect()
    for _ in idList:
        print( "Pulse ID is: ", _.PulseId() )

def main():
    topicName = 'Test-OnlineMonitor'
    topic_partition = TopicAndPartition( topicName, 0 )
    from_offsets = { topic_partition : 0 }

    sc = SparkContext(appName="streamingkafka")
    sc.setLogLevel("ERROR") # 减少shell打印日志
    ssc = StreamingContext(sc, 1) # 1秒的计算窗口
    brokers='127.0.0.1:9092'
    topic = 'Test-OnlineMonitor'
    # 使用streaming使用直连模式消费kafka
    message = KafkaUtils.createDirectStream(ssc, [topic], \
                    {"metadata.broker.list": brokers},\
                    fromOffsets=from_offsets,\
                    keyDecoder=spot_decoder,\
                    valueDecoder=spot_decoder)
    res = message.map( lambda x: x[1] )

    #ID = res.map( lambda msg: getID( msg ) )
    ID = res.map( lambda msg: getValue( msg ) )

    #ID.pprint(25)
    #ID.foreachRDD( lambda x: print(x.first()) )
    ID.foreachRDD( lambda x: displayRDD(x) )

    ssc.start()
    ssc.awaitTermination(240)
    ssc.stop()

if __name__ == "__main__":
    main()


