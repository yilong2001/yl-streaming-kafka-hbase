# yl-streaming_2_x_kafka_hbase
一个支持 spark structured streaming 流处理的 SQL 框架

## 说明

1、仅支持 Kfaka 作为数据 source；

2、Kafka、HBase 作为数据 sink；

3、支持与 HBase table 、 Hive table 的 Join；

4、pipline 基于 SQL，使用 json 配置；
```
	"piplines":[
		{
			"sql":"select table1.`after.uid` as uid, table1.`after.nick` as nick, table1.`after.grade` as grade
			      ,case when table1.op_type='I' then concat(table1.`after.uid`,'_',table1.`after.nick`) 
			            when table1.op_type='D' then concat(table1.`before.uid`,'_',table1.`before.nick`) 
			            when table1.op_type='U' then concat(table1.`after.uid`,'_',table1.`after.nick`) end as rowkey,
			      'default' as joinkey1, 1 as const_1, table1.`topic.partition` as t_par,
			      table1.`topic.offset` as t_off, table1.`topic.timestamp` as timestamp, table1.op_type from table1 ",
			"temp_table":"temp_hbase_write_1",
			"sinks":[
				{
					"type":"HBaseWrite",
					"dest":"hbase_write_table_1",
					"join":{
						"type":"HBaseJoin",
						"source":"hbase_join_table_1",
						"joinKeys":[
							{
								"lkey":"f:uid",
								"rkey":"f:uid_1"
							},
							{
								"lkey":"f:grade",
								"rkey":"f:grade_1"
							}
						]
					},
					"rowkey":"rowkey"
				},
				{
					"type":"KafkaCommit",
					"dest":"kakfa_commit_table",
					"join":null,
					"rowkey":null
				}
			]
```

5、支持自定义 UDF、UAF；

6、支持 HBase Insert / Delete / Update 语义；
 
7、两个 Streaming Join 需要配置 WaterMark


## 下一步

1、与 UniSQL (Spark SQL 方言) 结合，统一管理元数据，统一 SQL 增强；

创建 kafka table
```
create external table kafka_stream_x
(table string, op_type string, before.nick string, before.grade int, before.uid string,
after.nick string, after.grade int, after.uid string)
STORED BY 'org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler'
TBLPROPERTIES("spark.dialect.storage.type"="kafka",
"spark.dialect.kafka.data.format"="avro",
"spark.dialect.kafka.data.schema"="/tmp/avro_file",
"spark.dialect.kafka.prop.file"="/tmp/prop_file",
"spark.dialect.kafka.window.duration"="5")
```


2、streaming pipline  与 batch workflow 统一模型 ； 

## pipeline 模型
### 从 Kafka 到 Kafka / HBase ，流处理过程中与 Hive Table 或 HBase table 执行 Left Join 
Kafka(Source) =.=> transform (=.=> aggragate) =.=> Left Join Hive Table =.=> Left Join HBase Table =.=> Kafka / HBase Sink =.=> Topic Offset Commit

### 两个 Streaming Join , 必须设置 WaterMark
Kafka(Source) A =.=> Table A (Main Streaming)
                     =.=> transform (=.=> aggragate) =.=> Left Join Hive Table =.=> A left join B =.=> Left Join HBase Table =.=> Kafka / HBase Sink =.=> Topic A Offset Commit
Kafka(Source) B =.=> Table B

# Kafka DStream 处理流程
## 创建 InputDStream
```
val stream = KafkaUtils.createDirectStream[String, String](
  streamingContext,
  PreferConsistent,
  Subscribe[String, String](topics, kafkaParams)
)
```

## transform 算子执行计算
```
stream.map(record => (record.key, record.value))

stream.transform ({rdd => rdd.map{} })
```

## action 算子触发 job
```
stream.foreachRDD { rdd => rdd.mapPartitionsWithIndex{} }

如果没有 action ，会触发异常

Exception in thread "main" java.lang.IllegalArgumentException: requirement failed: No output operations registered, so nothing to execute

```

## InputDStream to OutputDStream
1、action 触发 DStream register ，在 ssc 的 graph scheduler 中增加 OutputDStream 

2、OutputDStream 实际触发执行 transformFunction

## generateJobs
1、在 DStreamGraph start generateJobs 时候，实际上调用了 OutputDStream 的 generateJob

## submit jobset
1、生成的 Job 对象会放入 ThreadPoolExecutor ，同时 send message 给 listener 

2、在 ThreadPoolExecutor 会运行 Job ，即进入 SparkContext 的 runjob 过程

3、SparkContext 的 runjob 在经过环境准备之后，会 send JobSubmitted message

4、DAGScheduler 通过 handleJobSubmitted 处理 job submitted message，分发 RDD 在 executor 上执行

## 生成 Kafka RDD
1、 通过 DStream 的 getOrCompute create RDD

2、首先判断 minbatch 时间是否正确，对于 window 滑动，超出时间窗口的 DStream 会被忽略

3、调用 DStream 的 compute(time) 生成 RDD，即执行 DirectKafkaInputDStream compute ，生成 kafka rdd

4、RDD 的 partition 由所有 topic.partitions 决定

5、一个 partition 一个 KafkaRDDIterator 

## 执行
1、 每个 MiniBatch 都会生成 KafkaRDDIterator ，直到 KafkaRDDIterator 运行完成，才进行下一个 Batch

2、 一个 Topic 的所有 Partition 完成，继续下一个 Batch

# spark kafka stream 的 spark-executor-(+original.group.id) 前缀问题
1、如果 Kafka ACL 开启， 不做授权的 group id 确实不能访问，否则会抛出异常
```
org.apache.kafka.common.errors.GroupAuthorizationException
```

2、不过，spark 通过 assign 的方式，绕过了这个限制

如下这种方式，spark-executor-(original.group.id)，不能访问
```
val consumer = new KafkaConsumer[K, V](kafkaParams)
consumer.assign(topics)
val records = consumer.poll(100)

```

如下这种方式，则可以
```
val consumer = new KafkaConsumer[K, V](kafkaParams)
consumer.assign(topics)
consumer.seek(tp, offset)
val records = consumer.poll(100)

```

spark 
