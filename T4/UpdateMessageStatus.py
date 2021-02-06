from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition, KafkaRDD, KafkaMessageAndMetadata
import EventData

def spot_decoder(s):
    if s is None:
        return None
    return s


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

def getValue( msg ):
    return msg

def getID( msg ):
    data = EventData.EventData.GetRootAsEventData( msg._rawMessage, 0 )
    return data.PulseId()

def displayRDD( rdd ):
    print( type(rdd) )
    #rdd.foreach(getValue)
    idList = rdd.collect()
    for _ in idList:
        data = EventData.EventData.GetRootAsEventData( _._rawMessage, 0 )
        print( "+++++++++++++++++++++ After filter: Topic: %s, Offset: %s, Pulse ID: %s"\
              % ( _.topic, _.offset, data.PulseId() ) )
              #% ( _.topic, _.offset, data.PulseId() ) )
    #data = EventData.EventData.GetRootAsEventData( rdd._rawMessage, 0 )
    #print( "+++++++++++++++++++++ After filter: Topic: %s, Offset: %s, Pulse ID: %s"\
    #      % ( _.topic, _.offset, data.PulseId() ) )
    #      #% ( _.topic, _.offset, data.PulseId() ) )
def displayID( rdd ):
    print("\n")
    idList = rdd.collect()
    for _ in idList:
        print( _[1] )

def getTopic(meta, topicName):
    data, (topic, part, offset, key, value), = meta.__reduce__()
    #print( " =============== Before filter: ", topic, part, offset )
    return topic==topicName

sc = SparkContext(appName="mytstApp")
sc.setLogLevel("ERROR") # 减少shell打印日志
ssc = StreamingContext(sc, 1)
tlist = ['Spark_1','Spark_2']
checkpoint_dir = './Checkpoint/spark'
ssc.checkpoint( checkpoint_dir )

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

def updatefunction( new, last ):
    if last is None:
        #print( "Start value" )
        last =0
    if new:
        #print( "new is: ", new, last )
        return sum(new) + last
    else:
        return last

stream1 = dstream.filter( lambda x : getTopic(x, 'Spark_1') )
stream2 = stream1.map( lambda x : getID(x) )
stream3 = stream2.map( lambda x : (1,  x))
counts = stream3.updateStateByKey( updatefunction )
counts.foreachRDD( lambda x : displayID(x) )

ssc.start()
# Wait for the job to finish
try:
    ssc.awaitTermination(200)
except Exception as e:
    ssc.stop()
    raise e  # to exit with error condition
