from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition, KafkaRDD, KafkaMessageAndMetadata
import EventData

def spot_decoder(s):
    if s is None:
        return None
    return s

def getValue( msg ):
    return msg

def displayRDD( rdd ):
    idList = rdd.collect()
    for _ in idList:
        data = EventData.EventData.GetRootAsEventData( _._rawMessage._rawMessage, 0 )
        print( "Topic: %s, Offset: %s, Pulse ID: %s"\
              % ( _.topic, _.offset, data.PulseId() ) )
              #% ( _.topic, _.offset, data.PulseId() ) )

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
    message = msg
    msgAndmeta = KafkaMessageAndMetadata( topic, partition, offset, key, message )
    return msgAndmeta


sc = SparkContext(appName="mytstApp")
sc.setLogLevel("ERROR") # 减少shell打印日志
ssc = StreamingContext(sc, 1)
tlist = ['Spark_1','Spark_2']

kafka_params = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "myUserGroup",
    "enable.auto.commit": "false",
    "auto.offset.reset": "smallest"
}

dstream = KafkaUtils.createDirectStream(ssc, tlist, kafka_params,\
		keyDecoder=spot_decoder,\
		valueDecoder=spot_decoder,\
        messageHandler=setHandler )
res = dstream.map( lambda x: x )
print( type(res) )
dstream  = res.map( lambda msg: getValue( msg ) )
dstream.foreachRDD( lambda x : displayRDD(x) )

ssc.start()
# Wait for the job to finish
try:
    ssc.awaitTermination(20)
except Exception as e:
    ssc.stop()
    raise e  # to exit with error condition
