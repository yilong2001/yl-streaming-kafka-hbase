# yl-streaming_2_x_kafka_hbase
一个支持 spark structured streaming 流处理的 SQL 框架

## 说明

1、仅支持 Kfaka 作为数据源；

2、Kafka、HBase 作为数据宿；

3、支持与 HBase table 、 Hive table 的 Join；

4、使用 json 进行配置；

5、pipline 基于 SQL；

6、支持 HBase Insert / Delete / Update 语义；
 
7、两个 Streaming Join 需要配置 WaterMark

## 下一步改进

1、与 UniSQL (Spark SQL 方言) 结合，统一管理元数据，统一 SQL 增强；

2、streaming pipline  与 batch workflow 统一模型 ； 

## pipeline 模型
### 从 Kafka 到 Kafka / HBase ，流处理过程中与 Hive Table 或 HBase table 执行 Left Join 
Kafka(Source) =.=.=> Left Join Hive Table =.=.=> Left Join HBase Table =.=.=> Kafka / HBase Sink =.=.=> Topic Offset Commit

### 两个 Streaming Join , 必须设置 WaterMark
Kafka(Source) A =.=.=> Table A (Main Streaming)
                     =.=.=> Left Join Hive Table =.=.=> A left join B =.=.=> Left Join HBase Table =.=.=> Kafka / HBase Sink =.=.=> Topic A Offset Commit
Kafka(Source) B =.=.=> Table B


