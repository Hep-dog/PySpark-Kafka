from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition, KafkaRDD, KafkaMessageAndMetadata
import EventData, requests, json, time

OnlineMonitorParas ={
    "LogRecordFlag" : True,
    "logger" : None,
    "outputDir":'output/',
    "batchNumber" : 750,
    "idStatus" : 0,
    "eventStatusList" : [],
    "prePulseID" : 0,
    "preTenIDList" : [],
    "endPoint" : "Jiyizi-test",
    "metricHead" : "Spark-Test-"
}

def requestPV( result ):
    roll = requests.post("http://10.1.26.63:1988/v1/push", data=json.dumps(result))
    return roll

def collectData( resultList, endPoint, Metric, value, time_st ):
    record = {}
    record['endPoint']  = endPoint
    record['step'] = 30
    record['counterType'] = 'GAUGE'
    record['metric'] = Metric
    record['value']  = value
    record['timestamp'] = time_st
    resultList.append( record )

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

def getID( msg ):
    data = EventData.EventData.GetRootAsEventData( msg._rawMessage, 0 )
    return [ msg.topic, data.PulseId() ]
    #return data.PulseId()

def displayRDD( rdd ):
    print( type(rdd) )
    #rdd.foreach(getValue)
    idList = rdd.collect()
    for _ in idList:
        data = EventData.EventData.GetRootAsEventData( _._rawMessage, 0 )
        print( "+++++++++++++++++++++ After filter: Topic: %s, Offset: %s, Pulse ID: %s"\
              % ( _.topic, _.offset, data.PulseId() ) )

def displayID( rdd ):
    try:
        ( _, ( errCounts, lastID, updateFlag, topicName ) ), = rdd.collect()
        print( topicName, errCounts, lastID, updateFlag )

        if updateFlag:
            time_st = int(time.time())
            targetList = []
            nameMetric = OnlineMonitorParas["metricHead"] + topicName
            collectData( targetList, OnlineMonitorParas["endPoint"], nameMetric, errCounts, time_st )
            roll = requestPV( targetList )
            print( "\nErrorCounting", nameMetric, errCounts, time_st )
            print( " ++++++++++++++++++++++++++++ requests sending status: ", roll.text, " ++++++++++++++++++++++++ " )
    except:
        print("Not enough messages")
    #for _ in idList:
    #    print( _[1] )

def getTopic(meta, topicName):
    data, (topic, part, offset, key, value), = meta.__reduce__()
    #print( " =============== Before filter: ", topic, part, offset )
    return topic==topicName

# Status form: ( Error_counts, previous ID )
def updatefunction( new, last ):
    # clear the status for next loop
    if last:
        last = ( 0, last[1], last[2], last[3] )
    if new:
        result = list( map( lambda x: x[0][1]-x[1][1], zip(new[1:], new[:-1]) ) )
        errNum = len(new) - result.count(1) -1
        if last:
            errNum += last[0]
            if new[0][1]-last[1] != 1:
                errNum += 1
        #print( "lalalalala", new, result, errNum, new[-1] )
        return ( errNum, new[-1][1], 1, new[-1][0] )
    else:
        if last:
            return (last[0], last[1], 0, last[3])
        else:
            return (0, 0, 0, "")

sc = SparkContext(appName="mytstApp")
sc.setLogLevel("ERROR") # 减少shell打印日志
ssc = StreamingContext(sc, 10)
tlist = ['Spark_1','Spark_2']
checkpoint_dir = './Checkpoint/spark'
ssc.checkpoint( checkpoint_dir )

kafka_params = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "myUserGroup",
    "enable.auto.commit": "false",
    "auto.offset.reset": "largest"
}
dstream = [KafkaUtils.createDirectStream(ssc, [tlist[i]], kafka_params,\
		    keyDecoder=spot_decoder,\
		    valueDecoder=spot_decoder,\
            messageHandler=setHandler )\
           for i in range(len(tlist))
           ]

##stream1 = dstream.filter( lambda x : getTopic(x, 'Spark_1') )
#for index in range(len(tlist)):
#    streamList.append(dstream.filter( lambda x : getTopic(x, tlist[index])))
countList = []

for index in range(len(tlist)):
    print( tlist[index] )
    tempt = ( dstream[index].map( lambda x : getID(x) )\
                    .map( lambda x : ( 1,  x))\
                    .updateStateByKey( updatefunction )\
                )
    print( "lalalaall" )
    countList.append( tempt )
    countList[index].foreachRDD( lambda x : displayID(x) )

ssc.start()
ssc.awaitTermination(5000)
ssc.stop()
