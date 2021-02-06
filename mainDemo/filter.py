from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition, KafkaRDD, KafkaMessageAndMetadata
import EventData, time

def spot_decoder(s):
    if s is None:
        return None
    return s

def displayRDD( rdd ):
    idList = rdd.collect()
    for _ in idList:
        data = EventData.EventData.GetRootAsEventData( _, 0 )
        print( "Pulse ID: %s"\
              % ( data.PulseId() ) )

def do_some_work(rdd):
	pass

def process_dstream(rdd):
    displayRDD(rdd)
    krdd = KafkaRDD(rdd._jrdd, sc, rdd._jrdd_deserializer)
    off_ranges = krdd.offsetRanges()
    #for o in off_ranges:
    #    print(str(o))

def setHandler(msg):
    topic = msg.topic
    partition = msg.partition
    offset = msg.offset
    key = msg.key
    message = msg._rawMessage
    msgAndmeta = KafkaMessageAndMetadata( topic, partition, offset, key, message )
    return msgAndmeta

def getValue( data ):
    return data._rawMessage


conf = SparkConf().setAppName("mytestApp")\
        .set("spark.streaming.kafka.maxRatePerPartition", "1000")\
        .set("spark.streaming.kafka.minRatePerPartition", "1000")
        #.set("spark.streaming.backpressure.enabled", "true")\
        #.set("spark.streaming.backpressure.initialRate", "100")\

sc = SparkContext( conf=conf )
#sc = SparkContext()
sc.setLogLevel("ERROR") # 减少shell打印日志
ssc = StreamingContext(sc, 1)
tlist = ['SparkTest']

kafka_params = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "myUserGroup-0",
    "enable.auto.commit": "true",
    "auto.offset.reset": "smallest"
}

dstream = KafkaUtils.createDirectStream(ssc, tlist, kafka_params,\
		keyDecoder=spot_decoder,\
		valueDecoder=spot_decoder,\
        messageHandler=setHandler )
res = dstream.map( lambda x: getValue(x) )
res.foreachRDD( lambda x : displayRDD(x) )

ssc.start()
ssc.awaitTermination(200)
ssc.stop()
