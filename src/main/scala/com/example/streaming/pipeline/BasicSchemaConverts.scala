package com.example.streaming.pipeline

import org.apache.spark.sql.types._


/**
  * Created by yilong on 2019/3/20.
  */
object BasicSchemaConverts {
  //format is : field1:type1,field2:type2...

  def toDatayType(tp:String) : DataType = {
    if (tp.equals("string")) StringType
    else if (tp.equals("int")) IntegerType
    else if (tp.equals("double")) DoubleType
    else if (tp.equals("float")) FloatType
    else if (tp.equals("long")) LongType
    else if (tp.equals("boolean")) BooleanType
    else if (tp.equals("bytes")) BinaryType
    else StringType
  }

  def toSqlType(fieldSchemaStrs: String): StructType = {
    val fields = fieldSchemaStrs.trim.split(",").map(fdschema=>{
      val fdtype = fdschema.trim.split(":")
      StructField(fdtype.apply(0), toDatayType(fdtype.apply(1)), false)
    })

    StructType(fields)
  }
}
