from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from kazoo.client import KazooClient
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

def createZK():
    zkServer = "localhost:2181"
    if 'KazooSingletonInstance' not in globals():
        globals()['KazooSingletonInstance'] = KazooClient( zkServer )
        globals()['KazooSingletonInstance'].start()

    return globals()['KazooSingletonInstance']

def readOffsets( zk, topics, groupID ):
    from_offsets = {}
    for topic in topics:
        childName = '/consumers/' + topic
        zk.ensure_path( childName )
        for partition in zk.get_children(childName):
            childPart = childName +'/' + partition
            zk.ensure_path( childPart )
            topic_partition = TopicAndPartition( topic, int(partition) )
            try:
                offset = int( zk.get(childPart)[0] )
            except:
                print( " ============= Get child partition error ============== " )
                return None
            from_offsets[topic_partition] = offset

    return from_offsets

def saveOffsets(rdd, zk):
    for offset in rdd.offsetRanges():
        topic = offset.topic
        part  = offset.partition
        path = f"/consumers/" + str(topic) + "/" + str(part)
        print( path, offset.untilOffset )
        zk.ensure_path(path)
        zk.set( path, str(offset.untilOffset).encode('utf-8') )
        print(zk.get(path))

def showOffsetRanges( rdd, zk ):
    saveOffsets( rdd, zk )
    offsetRanges = rdd.offsetRanges()
    for offset in offsetRanges:
        print( offset.topic, offset.partition, offset.fromOffset, offset.untilOffset )


def main():

    groupID = 'myUserGroup-0'
    kafka_params = {
        "bootstrap.servers": "localhost:9092",
        "group.id": groupID,
        "enable.auto.commit": "true",
        "auto.offset.reset": "smallest"
    }

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


    zk = createZK()
    #print( zk, globals() )
    from_offsets = readOffsets( zk, tlist, groupID )
    print( from_offsets )

    dstream = KafkaUtils.createDirectStream(ssc, tlist, kafka_params,\
            fromOffsets=from_offsets,\
    		keyDecoder=spot_decoder,\
    		valueDecoder=spot_decoder,\
            messageHandler=setHandler )
    dstream.foreachRDD( lambda x: showOffsetRanges(x, zk) )
    res = dstream.map( lambda x: getValue(x) )
    res.foreachRDD( lambda x : displayRDD(x) )

    ssc.start()
    ssc.awaitTermination(200)
    ssc.stop()

if __name__ == "__main__":
    main()
