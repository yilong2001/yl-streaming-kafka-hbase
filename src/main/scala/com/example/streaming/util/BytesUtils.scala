package com.example.streaming.util

import org.apache.spark.sql.types._
import java.nio.ByteBuffer

/**
  * Created by yilong on 2019/3/17.
  */
object BytesUtils {
  import org.apache.hadoop.hbase.util.Bytes.toBytes
  def sparkRowToHBaseBytes(obj : Any) : Array[Byte] = {
    if (obj == null) null
    else if (obj.isInstanceOf[String]) toBytes(obj.asInstanceOf[String].toString)
    else if (obj.isInstanceOf[Int]) toBytes(obj.asInstanceOf[Int])
    else if (obj.isInstanceOf[Long]) toBytes(obj.asInstanceOf[Long])
    else if (obj.isInstanceOf[BigDecimal]) toBytes(obj.asInstanceOf[BigDecimal].toDouble)
    else if (obj.isInstanceOf[Double]) toBytes(obj.asInstanceOf[Double])
    else if (obj.isInstanceOf[Float]) toBytes(obj.asInstanceOf[Float])
    else if (obj.isInstanceOf[Boolean]) toBytes(obj.asInstanceOf[Boolean])
    else if (obj.isInstanceOf[Array[Byte]]) obj.asInstanceOf[Array[Byte]]
    else toBytes(obj.toString)
  }

  def arrayByteToObj(in : Array[Byte], dt : DataType):Any = {
    if (in == null || in.length == 0) null
    else {
      dt match {
        case StringType => new String(in, "utf-8")
        case IntegerType => ByteBuffer.wrap(in).getInt
        case DoubleType => ByteBuffer.wrap(in).getDouble
        case FloatType => ByteBuffer.wrap(in).getFloat
        case LongType => ByteBuffer.wrap(in).getLong
        case BooleanType => (ByteBuffer.wrap(in).getInt == 0)
        case BinaryType => in
        case _ => in
      }
    }
  }

}
