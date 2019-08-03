package com.example.streaming.util

import com.example.streaming.pipeline.{BasicSchemaConverts, TableSchemaMetaData}
import org.apache.spark.sql.types.{StructField, _}

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

/**
  * Created by yilong on 2019/4/5.
  */
object MetaUtils {
  val DEFAULT_FIELD_SPLIT_CHAR = ","
  val DEFAULT_FIELD_TYPE_SPLIT_CHAR = ":"

  def getTableSchemaMetaData(tableName:String, tableSchemaList:Array[TableSchemaMetaData]) : String = {
    var out : String = null
    tableSchemaList.foreach(ts=>{
      if (ts.tableName.equals(tableName)) out = ts.cols
    })

    out
  }

  def parseTableSchemaString(cols:String) : java.util.Map[String,String] = {
    val m = new java.util.HashMap[String,String]()

    cols.trim.split(DEFAULT_FIELD_SPLIT_CHAR).foreach(fd => {
      val fdtype = fd.trim.split(DEFAULT_FIELD_TYPE_SPLIT_CHAR)
      m.put(fdtype(0), fdtype(1))
    })

    m
  }

  def appendTopicFieldsToStructType(structType: StructType) : StructType =  {
    val st = StructType.apply(structType.fields)

    StructType(
      structType.fields ++ Array( StructField("topic.name",StringType,false) , StructField("topic.partition",IntegerType,false),
        StructField("topic.offset",LongType,false), StructField("topic.timestamp",LongType,false) )
    )
  }

  def appendJoinFieldsToStructType(structType: StructType, cols:String) : StructType = {
    if (cols.length == 0) {
      structType
    } else {
      val tsmap = parseTableSchemaString(cols)

      StructType(
        structType.fields ++ tsmap.map(tp => StructField(tp._1, BasicSchemaConverts.toDatayType(tp._2))).toArray[StructField]
      )
    }
  }
}
