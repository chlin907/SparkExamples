// Code is from http://www.jeepxie.net/article/650414.html
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, IntegerType, StructField, StructType}

class MyUDAF extends UserDefinedAggregateFunction{
  override def inputSchema: StructType = StructType(Array(StructField("age", IntegerType)))

  override def bufferSchema: StructType = StructType(Array(StructField("count", IntegerType), StructField("ages", IntegerType)))

  override def dataType: DataType = IntegerType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {buffer(0)=0;buffer(1)=0}

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit={
    buffer(0) = buffer.getInt(0) + 1
    buffer(1) = buffer.getInt(1) + input.getInt(0)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit={
    buffer1(0) = buffer1.getInt(0)+buffer2.getInt(0)
    buffer1(1) = buffer1.getInt(1)+buffer2.getInt(1)
  }

  override def evaluate(buffer: Row): Any = buffer.getInt(1)/buffer.getInt(0)

}

spark.udf.register("udafAverage", new MyUDAF)
import  spark.implicits._
val dfAge = spark.createDataFrame(Seq((22, 1), (24, 1), (11, 2), (15, 2))).toDF("age", "class_id")
// By sql
dfAge.registerTempTable("bigDataTable")
spark.sql("select class_id, udafAverage(age) from bigDataTable group by class_id").show
// By decoration
val udafAvg = new MyUDAF()
dfAge.groupBy('class_id).agg(udafAvg('age)).show
