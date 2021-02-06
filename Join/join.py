from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition, KafkaRDD, KafkaMessageAndMetadata
import EventData, requests, json, time

def spot_decoder(s):
    if s is None:
        return None
    return s

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

def displayRDD( rdd ):
    print( type(rdd) )
    #rdd.foreach(getValue)
    idList = rdd.collect()
    for _ in idList:
        topic, ID = _[0], _[1]
        print( " Topic : %s, ID: %s" % (topic, ID) )


def getTopic(meta, topicName):
    data, (topic, part, offset, key, value), = meta.__reduce__()
    #print( " =============== Before filter: ", topic, part, offset )
    return topic==topicName

def getID( msg ):
    data = EventData.EventData.GetRootAsEventData( msg._rawMessage, 0 )
    return ( msg.topic, data.PulseId() )
    #return data.PulseId()


tlist = []
numTopic = 2
topicHead= 'SparkTest-'
for i in range(numTopic):
    tlist.append( topicHead + str(i) )

sc = SparkContext(appName="mytstApp")
sc.setLogLevel("ERROR") # 减少shell打印日志
ssc = StreamingContext(sc, 1)
#tlist = ['Spark_1','Spark_2']
checkpoint_dir = './Checkpoint/spark'
ssc.checkpoint( checkpoint_dir )

kafka_params = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "myUserGroup",
    "enable.auto.commit": "false",
    "auto.offset.reset": "smallest"
}
dstream = [KafkaUtils.createDirectStream(ssc, [tlist[0]], kafka_params,\
		    keyDecoder=spot_decoder,\
		    valueDecoder=spot_decoder,\
            messageHandler=setHandler )\
           for i in range(len(tlist))
           ]
countList = []

for index in range(len(tlist)):
    print( tlist[index] )
    tempt = ( dstream[index].map( lambda x : getID(x) ) )
    countList.append( tempt )
    #countList[index].foreachRDD( lambda x : displayRDD(x) )

out = countList[0].leftOuterJoin(countList[1])
out.foreachRDD( lambda x : displayRDD(x) )
out.pprint()


ssc.start()
ssc.awaitTermination(500)
ssc.stop()
