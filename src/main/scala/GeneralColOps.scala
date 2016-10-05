/**
  *
Created by mbrown on 4/20/16.
Copyright (c) 2016, 3rd Planet Softworks
All rights reserved.

Redistribution and use, with or without modification, are permitted provided that the following conditions are met:

1. Redistributions of this data must retain the above copyright notice, this list of conditions and the following disclaimer.

2. Redistributions requires prior consent 3rd Planet Softworks

3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
  */



/*
  Note:
  Let's make this better by extending the RDD/DataFrame class

 */

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, DataFrame, Row}
import org.apache.spark.sql.types.{StructType, StructField}
import org.apache.spark.sql.hive.HiveContext
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
object GeneralColOps
{
  var sparkContext: SparkContext = null
  var sqlContext: SQLContext = null
  var origDF: DataFrame = null
  var inputColname:String = null
  var outputColName: String = null
  var outputColType: String = null
  var methodOp = "numerical_mapper"

  def modRDD(row:Row,row_posistion:Int = 0,method_call:String,parameters:Map[String,Any]):Row={
    val rowSeq: Seq[Any] = row.toSeq
    var rowListBuffer: ListBuffer[Any] = rowSeq.to[scala.collection.mutable.ListBuffer]
    var value:Int = 0
    if(method_call == "numerical_mapper"){
      //value = numericalMapper(rowSeq(row_posistion)).asInstanceOf[Int]
    }
    rowListBuffer += value
    val newSeq:Seq[Any] = rowListBuffer.toSeq
    val newRow:Row = Row.fromSeq(newSeq)
    return newRow
  }
  def addIdRDD(row:Row): Row ={
    val rowSeq: Seq[Any] = row.toSeq
    var rowListBuffer: ListBuffer[Any] = rowSeq.to[scala.collection.mutable.ListBuffer]
    var valListBuffer: ListBuffer[Any] = new ListBuffer[Any]()
    valListBuffer += 0
    val finalListBuffer = valListBuffer ++ rowListBuffer
    val newSeq:Seq[Any] = finalListBuffer.toSeq
    val newRow:Row = Row.fromSeq(newSeq)
    return newRow
  }
  def addListAstColRDD(row_to:Row,row_from:Row): Row ={
    val rowToSeq: Seq[Any] = row_to.toSeq
    val rowFromSeq: Seq[Any] = row_from.toSeq
    var rowToListBuffer: ListBuffer[Any] = rowToSeq.to[scala.collection.mutable.ListBuffer]
    var rowFromListBuffer: ListBuffer[Any] = rowFromSeq.to[scala.collection.mutable.ListBuffer]
    val rowList: ListBuffer[Any] = rowToListBuffer += rowFromListBuffer
    val freshSeq  = rowList.toSeq
    val freshRow = Row.fromSeq(freshSeq)
    return freshRow
  }
  def addColRDD(row:Row,column_type:String):Row={
    var colType: String = column_type
    colType = colType.toLowerCase
    val rowSeq: Seq[Any] = row.toSeq
    var rowListBuffer: ListBuffer[Any] = rowSeq.to[scala.collection.mutable.ListBuffer]
    if(colType == "integer"){
      var value:Int = 0
      rowListBuffer += value
    }else if(colType == "string"){
      var value:String = ""
      rowListBuffer += value
    }else if(colType == "float"){
      var value:Float = 0
      rowListBuffer += value
    }else if(colType == "decimal"){
      var value:Float = 0
      rowListBuffer += value
    }else if(colType == "double"){
      var value:Double = 0.0
      rowListBuffer += value
    }else if(colType == "long"){
      var value:Long = 0
      rowListBuffer += value
    }else {
      var value = null
      rowListBuffer += value
    }
    val newSeq:Seq[Any] = rowListBuffer.toSeq
    val newRow:Row = Row.fromSeq(newSeq)
    return newRow
  }
  def setStructFieldType():StructField={
    var newField: StructField = new StructField("id", org.apache.spark.sql.types.IntegerType, true)
    return newField
  }
  def setStructFieldType(output_col_name:String,output_col_type:String):StructField={
    var colType: String = output_col_type.toLowerCase()
    if(output_col_type == "integer"){
      var newField: StructField = new StructField(output_col_name, org.apache.spark.sql.types.IntegerType, true)
      return newField
    }else if(output_col_type == "string"){
      var newField: StructField = new StructField(output_col_name, org.apache.spark.sql.types.StringType, true)
      return newField
    }else if(output_col_type == "float"){
      var newField = StructField(output_col_name, org.apache.spark.sql.types.FloatType, true)
      return newField
    }else if(output_col_type == "decimal"){
      //var  newField: StructField = new StructField (output_col_name, org.apache.spark.sql.types.DecimalType, true)
      //outputSchemaFieldArray += newField
      return null
    }else if(output_col_type == "double"){
      var newField = new StructField(output_col_name, org.apache.spark.sql.types.DoubleType, true)
      return newField
    }else if(output_col_type == "long"){
      var newField = StructField (output_col_name, org.apache.spark.sql.types.LongType, true)
      return newField
    }else if(output_col_type == "array"){
      //var newField= StructField(output_col_name, org.apache.spark.sql.types.ListType, true)
      //outputSchemaFieldArray += newField
      return null
    }
    else if(output_col_type == "sourcebytes_srqed"){
      var newField: StructField = new StructField(output_col_name, org.apache.spark.sql.types.IntegerType, true)
      return newField
    }
    return null
  }
  def addIdModCol(input_dataframe: DataFrame): DataFrame = {
    sparkContext = input_dataframe.rdd.context
    //sqlContext = input_dataframe.sqlContext
    sqlContext = new HiveContext(sparkContext)
    val oldDF = input_dataframe.na.drop()
    var inputSchema: StructType = oldDF.schema
    var inputSchemaFieldArray: Array[StructField] = inputSchema.toArray
    var inputSchemaFieldBuffer: ArrayBuffer[StructField] = inputSchemaFieldArray.to[mutable.ArrayBuffer]
    var outputSchemaFieldArray:ArrayBuffer[StructField] = inputSchemaFieldBuffer
    var prependedSchemaArray:ArrayBuffer[StructField] = new ArrayBuffer[StructField]()
    prependedSchemaArray += setStructFieldType()
    var newSchemaArray =  prependedSchemaArray ++ outputSchemaFieldArray
    var finalSchemaArray = newSchemaArray   //finalSchemaArray
    val inDataFrameColsArrayBuff: ArrayBuffer[String] = oldDF.columns.to[mutable.ArrayBuffer]
    val inputRDD: RDD[Row] = oldDF.rdd
    val outputRDD =  inputRDD.map(row => addIdRDD(row))
    val outputStructType:StructType = StructType(finalSchemaArray.toList)
    val freshDF: DataFrame = sqlContext.applySchema(outputRDD,outputStructType)
    val freshDFCollection:Array[Row] = freshDF.collect()
    var freshDFArrayBuffer:ArrayBuffer[Row] = freshDFCollection.to[mutable.ArrayBuffer]
    val freshDFCount: Long = freshDF.count()
    for (count <- 0 to (freshDFArrayBuffer.length - 1 )){
      var rowSeq: Seq[Any] = freshDFArrayBuffer(count).toSeq
      var rowListBuffer: ListBuffer[Any] = rowSeq.to[scala.collection.mutable.ListBuffer]
      //rowListBuffer(0) = count.toDouble
      rowListBuffer(0) = count
      var newRowSeq: Seq[Any] = rowListBuffer.toSeq
      var newRow: Row = Row.fromSeq(newRowSeq)
      freshDFArrayBuffer(count) = newRow
    }
    def delModCol(input_dataframe: DataFrame,colName:String): DataFrame = {
      val oldDF = input_dataframe.na.drop()
      val freshDF:DataFrame = oldDF.drop(colName)
      return freshDF
    }
    val freshestRDD: RDD[Row] = sparkContext.parallelize(freshDFArrayBuffer)
    val freshestDF: DataFrame = sqlContext.applySchema(freshestRDD,outputStructType)
    return freshestDF
  }
  def combineDF(input_dataframe: DataFrame, column_name:String, column_type:String):DataFrame ={
    //......
    return null
  }
  def addCol(input_dataframe: DataFrame, column_name:String, column_type:String): DataFrame = {
    sparkContext = input_dataframe.rdd.context
    sqlContext = input_dataframe.sqlContext
    val oldDF = input_dataframe.na.drop()
    val setSqlContext: SQLContext = input_dataframe.sqlContext
    var inputSchema: StructType = input_dataframe.schema
    var inputSchemaFieldArray: Array[StructField] = inputSchema.toArray
    var inputSchemaFieldBuffer: ArrayBuffer[StructField] = inputSchemaFieldArray.to[mutable.ArrayBuffer]
    var outputSchemaFieldArray:ArrayBuffer[StructField] = inputSchemaFieldBuffer
    outputSchemaFieldArray += setStructFieldType(column_name,column_type)
    val inDataFrameColsArrayBuff: ArrayBuffer[String] = input_dataframe.columns.to[mutable.ArrayBuffer]
    val inputRDD: RDD[Row] = oldDF.rdd
    val outputRDD: RDD[Row] =  inputRDD.map(row => addColRDD(row,column_type))
    val outputStructType:StructType = StructType(outputSchemaFieldArray.toList)
    val freshDF: DataFrame = sqlContext.applySchema(outputRDD,outputStructType)
    return freshDF
  }
}
