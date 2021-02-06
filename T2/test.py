from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition, KafkaRDD
import EventData

def spot_decoder(s):
    if s is None:
        return None
    return s

def getValue( msg ):
    data = EventData.EventData.GetRootAsEventData( msg, 0 )
    return( data )

def displayRDD( rdd ):
    idList = rdd.collect()
    for _ in idList:
        print( "Pulse ID is: ", _.PulseId() )

def do_some_work(rdd):
	pass

def process_dstream(rdd):
    displayRDD(rdd)
    krdd = KafkaRDD(rdd._jrdd, sc, rdd._jrdd_deserializer)
    off_ranges = krdd.offsetRanges()
    #for o in off_ranges:
    #    print(str(o))


sc = SparkContext(appName="mytstApp")
sc.setLogLevel("ERROR") # 减少shell打印日志
ssc = StreamingContext(sc, 1)

kafka_params = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "myUserGroup",
    "enable.auto.commit": "false",
    "auto.offset.reset": "smallest"
}

dstream = KafkaUtils.createDirectStream(ssc, ["Spark_1"], kafka_params,\
		keyDecoder=spot_decoder,\
		valueDecoder=spot_decoder)
res = dstream.map( lambda x: x[1] )
dstream  = res.map( lambda msg: getValue( msg ) )
dstream.foreachRDD( lambda x : process_dstream(x) )
#dstream.foreachRDD(process_dstream)
# Start our streaming context and wait for it to 'finish'
ssc.start()

# Wait for the job to finish
try:
    ssc.awaitTermination(5)
except Exception as e:
    ssc.stop()
    raise e  # to exit with error condition
