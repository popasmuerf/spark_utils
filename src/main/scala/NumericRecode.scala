/**
  *
Created by mbrown on 4/27/16.
Copyright (c) 2016, 3rd Planet Softworks LLC
All rights reserved.

Redistribution and use, with or without modification, are permitted provided that the following conditions are met:

1. Redistributions of this data must retain the above copyright notice, this list of conditions and the following disclaimer.

2. Redistributions requires prior consent 3rd Planet Softworks LLC .

3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
  */

import java.util.Random
import org.apache.spark.sql.types.StructType
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, SQLContext, DataFrame, Row}
import org.apache.spark.sql.types.{StructType, StructField}
import scala.collection.immutable.Range.Inclusive
import scala.collection.mutable
import org.apache.spark.sql.functions._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import org.apache.spark.sql.hive.HiveContext
object  NumericRecode {
  var sparkContext: SparkContext = null
  var sqlContext:SQLContext = null
  var origDF: DataFrame = null
  var inputColname:String = null
  var outputColName: String = null
  var outputColType: String = null
  var methodOp = "numerical_mapper"
  var userMapListBuff:ListBuffer[(Inclusive, Int)] = null
  var rangeMapArray:Array[(Inclusive, Int)] = null
  var rangeMapList:ListBuffer[(Inclusive, Int)] = null
  var rangeMapSeq:Seq[(Inclusive, Int)] = null
  var useMapListBuff2 : ListBuffer[(Inclusive, Int)] = null

  def setDataFrame(arg_df: DataFrame): Unit = {
    origDF = arg_df
  }
  def setDataFrame(arg_context: SparkContext): Unit = {
    sparkContext = arg_context
  }

  def setInColName(arg_name: String): Unit = {
    inputColname = arg_name
  }
  def setOutColName(arg_name: String): Unit = {
    outputColName = arg_name
  }
  def setOutColType(arg_type: String): Unit = {
    outputColType = arg_type
  }
  def setSparkContext(arg_context: SparkContext): Unit = {
    sparkContext = arg_context
  }
  def setSparkContext(): Unit ={
    // var mathOps = new GenMathOps()
    //var colOps = new GeneralColOps()
    val sparkMaster = "local"
    val sparkAppname = "Stand alone Scala Spark app"
    val sparkConf:SparkConf = new SparkConf()
    sparkConf.setAppName(sparkAppname)
    sparkConf.setMaster(sparkMaster)
    val sparkContext = new SparkContext(sparkConf)
  }
  def setSqlContext(arg_context: org.apache.spark.sql.hive.HiveContext): Unit = {
    sqlContext = arg_context
  }
  def setSqlContext(): Unit ={
    val sqlContext:SQLContext = new SQLContext(sparkContext)
  }
  def setMapList(use_map_list: ListBuffer[(Inclusive, Int)]):Unit={
    useMapListBuff2 = use_map_list
  }
  def numericalMapper(row_element:Any): Int ={
    var valueMap:Int =  -1
    var rowElement:Int =  row_element.asInstanceOf[Int]
    for(el <- useMapListBuff2){
      if(el._1.contains(rowElement)){
        valueMap = el._2
        //break
      }
    }
    return valueMap
  }
  def numericalMapper(row_element:Any,userMapListBuff:ListBuffer[(Inclusive, Int)]): Int ={
    var valueMap:Int =  -1
    var rowElement:Int =  row_element.asInstanceOf[Int]
    for(el <- userMapListBuff){
      if(el._1.contains(rowElement)){
        valueMap = el._2
        //break
      }
    }
    return valueMap
  }
  def modRDD(row:Row,row_posistion:Int,userMapListBuff:ListBuffer[(Inclusive, Int)]):Row={
    val rowSeq: Seq[Any] = row.toSeq
    var rowListBuffer: ListBuffer[Any] = rowSeq.to[scala.collection.mutable.ListBuffer]
    var value:Int = 0
    value = numericalMapper(rowSeq(row_posistion),userMapListBuff).toInt
    rowListBuffer += value
    val newSeq:Seq[Any] = rowListBuffer.toSeq
    val newRow:Row = Row.fromSeq(newSeq)
    return newRow
  }
  def setNumericRangeMap(range_array:Array[(Inclusive, Int)]):Unit={
    rangeMapArray = range_array
    sparkContext.parallelize(rangeMapList)
  }
  def setNumericRangeMap(range_list:ListBuffer[(Inclusive, Int)]):Unit={
    rangeMapList = range_list
    sparkContext.parallelize(rangeMapList.to[scala.collection.mutable.ListBuffer])
  }
  def setNumericRangeMap(range_seq:Seq[(Inclusive, Int)]):Unit={
    rangeMapSeq = range_seq
    sparkContext.parallelize(rangeMapArray)
  }
  def setStructFieldType(output_col_name:String):StructField={
    val colType = "integer"
    if(colType == "integer"){
      var newField: StructField = new StructField(output_col_name, org.apache.spark.sql.types.IntegerType, true)
      return newField
    }else if(colType == "string"){
      var newField: StructField = new StructField(output_col_name, org.apache.spark.sql.types.StringType, true)
      return newField
    }else if(colType == "float"){
      var newField = StructField(output_col_name, org.apache.spark.sql.types.FloatType, true)
      return newField
    }else if(colType == "decimal"){
      //var  newField: StructField = new StructField (output_col_name, org.apache.spark.sql.types.DecimalType, true)
      //outputSchemaFieldArray += newField
      return null
    }else if(colType == "double"){
      var newField = new StructField(output_col_name, org.apache.spark.sql.types.DoubleType, true)
      return newField
    }else if(colType == "long"){
      var newField = StructField (output_col_name, org.apache.spark.sql.types.LongType, true)
      return newField
    }else if(colType == "array"){
      //var newField= StructField(output_col_name, org.apache.spark.sql.types.ArrayType, true)
      //outputSchemaFieldArray += newField
      return null
    }
    else if(colType == "sourcebytes_srqed"){
      var newField: StructField = new StructField(output_col_name, org.apache.spark.sql.types.IntegerType, true)
      return newField
    }
    return null
  }
  def recode(input_dataframe: DataFrame,input_column_name:String,output_col_name:String,range_list:ListBuffer[(Inclusive, Int)]): DataFrame = {
    sparkContext = input_dataframe.rdd.context
    //sqlContext = input_dataframe.sqlContext
    sqlContext = new HiveContext(sparkContext)
    val columnsArray: Array[String] = input_dataframe.columns
    if(!columnsArray.contains(input_column_name)){
      println("This input column does not exist....")
      println("I am going kill this job here.  Find out what the correct input column is...")
      sys.addShutdownHook{
        sparkContext.stop()
      }
    }
    val oldDF = input_dataframe.na.drop()
    var inputSchema: StructType = oldDF.schema
    var inputSchemaFieldArray: Array[StructField] = inputSchema.toArray
    var inputSchemaFieldBuffer: ArrayBuffer[StructField] = inputSchemaFieldArray.to[mutable.ArrayBuffer]
    var outputSchemaFieldArray:ArrayBuffer[StructField] = inputSchemaFieldBuffer
    outputSchemaFieldArray += setStructFieldType(output_col_name)
    val inDataFrameColsArrayBuff: ArrayBuffer[String] = oldDF.columns.to[mutable.ArrayBuffer]
    var inColPos: Int = inDataFrameColsArrayBuff.indexOf(input_column_name) //this needs to be put in a try-catch
    val inputRDD: RDD[Row] = oldDF.rdd
    val outputRDD =  inputRDD.map( row => modRDD(row,inColPos,range_list))
    val outputStructType:StructType = StructType(outputSchemaFieldArray.toList)
    val freshDF: DataFrame = sqlContext.createDataFrame(outputRDD,outputStructType)
    return freshDF
  }
}
