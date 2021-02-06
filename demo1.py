from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, KafkaRDD, KafkaDStream
import EventData

def spot_decoder(s):
    if s is None:
        return None
    return s

def displayRDD( rdd ):
    idList = rdd.collect()
    for _ in idList:
        print( "Pulse ID is: ", _.PulseId() )

def getValue( msg ):
    data = EventData.EventData.GetRootAsEventData( msg, 0 )
    return( data )

def process_dstream( sc, rdd ):
    displayRDD( rdd )
    krdd = KafkaRDD(rdd._jrdd, sc, rdd._jrdd_deserializer)
    off_ranges = krdd.offsetRanges()
    #off_ranges = krdd.offsetRanges()
    for o in off_ranges:
        print( "Topic is: ", o.topic, o.partition, o.fromOffset, o.untilOffset )

def dStreamTokafkadStream( ssc, stream ):
    return KafkaDStream(stream._jdstream, ssc, stream._jrdd_deserializer)

def main():
    brokers='127.0.0.1:9092'
    blist = ['127.0.0.1:9092','10.1.36.83:9092']
    tlist = [['Spark_1'],['Spark_2']]
    topics = ['Spark_1']

    kafka_params ={
        "bootstrap.servers" : "localhost:9092",
        "group.id" : "test",
        "auto.offset.reset" : "smallest"

    }

    #{"metadata.broker.list": ["localhost:2181,10.1.36.83:2181"]},\
    sc = SparkContext(appName="streamingkafka")
    sc.setLogLevel("ERROR") # 减少shell打印日志
    ssc = StreamingContext(sc, 1) # 5秒的计算窗口
    #message = KafkaUtils.createDirectStream(ssc, topics, \
    #                {"metadata.broker.list": brokers},\
    #                keyDecoder=spot_decoder,\
    #                valueDecoder=spot_decoder)
    dstreams = [KafkaUtils.createDirectStream(ssc, tlist[i], \
                    kafka_params,\
                    #{"metadata.broker.list": blist[i]},\
                    keyDecoder=spot_decoder,\
                    valueDecoder=spot_decoder) \
                    for i in range(len(blist))\
                ]
    #message = ssc.union( *dstreams )
    #message = dStreamTokafkadStream( ssc, message )
    for stream in dstreams:
        res = stream.map( lambda x: x[1] )

        ID = res.map( lambda msg: getValue( msg ) )
        print( type(ID) )
        ID.foreachRDD( lambda x: process_dstream( sc, x ) )

    ssc.start()
    ssc.awaitTermination(10)
    ssc.stop()

if __name__ == "__main__":
    main()
