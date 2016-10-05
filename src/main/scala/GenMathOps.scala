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

import java.util

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, SQLContext, DataFrame, Row}
import org.apache.spark.sql.types.{StructType, StructField}
import scala.collection.mutable
import org.apache.spark.sql.functions._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
object GenMathOps{
  var sparkContext: SparkContext = null
  var sqlContext: SQLContext = null
  var origDF: DataFrame = null
  var inputColname:String = null
  var outputColName: String = null
  var outputColType: String = null
  var methodOp = "numerical_mapper"
  
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
    def setSqlContext(arg_context: SQLContext): Unit = {
      sqlContext = arg_context
    }
    def setSqlContext(): Unit ={
      val sqlContext:SQLContext = new SQLContext(sparkContext)
    }
    def isZero(arg:Double):Boolean={
      if(arg == 0){
        return true
      }else{
        return false
      }
    }
    def isOne(arg:Double):Boolean={
      if(arg == 1){
        return true
      }else{
        return false
      }
    }
    def isLessThanOne(arg:Double):Boolean={
      if(arg < 1){
        return true
      }else{
        return false
      }
    }
    def exitIfZero(arg:Double):Unit={
      if(arg == 0){
        println("exited because arg = 0 ")
        throw new IllegalArgumentException
        java.lang.System.exit(-1)
      }
    }
    def exitIfOne(arg:Double):Unit={
      if(arg == 1){
        println("exited because arg = 1 ")
        throw new IllegalArgumentException
        java.lang.System.exit(-1)
      }
    }
    def exitIfLessThanOne(arg:Double):Unit={
      if(arg < 1){
        println("exited because arg < 1 ")
        throw new IllegalArgumentException
        java.lang.System.exit(-1)
      }
    }
    
  def getMean(set:Double*): Double ={
    var sum:Double = 0.0
    val setLen:Long = set.length
    for(el<-set){
      sum += el.toDouble
    }
    return sum/setLen
  }
  def getMean(set:Array[Double]): Double ={
    var sum:Double = 0.0
    val setLen:Long = set.length
    for(el<-set){
      sum += el.toDouble
    }
    return sum/setLen
  }
  def getLog(base:Double = 0,argument:Double =0):Double ={
    return  (java.lang.Math.log(argument)) / (java.lang.Math.log(base))
  }
  def getPow(base:Double = 0,exponent:Double =0):Double ={
    return java.lang.Math.pow(base,exponent)
  }
  def getProb(numerator:Double, demonimator:Double):Double = {
    return numerator/demonimator
  }
  def getStdDev(set:Array[Double]):Double={
    var sum:Double = 0.0
    var numerator = 0.0
    var mean:Double = 0.0
    var meanSqrDiff:Double = 0.0
    var dev:Double = 0.0
    for(el <- set){
      sum += el
    }
    mean = sum/set.length
    for(el <- set){
      numerator += Math.pow((el - mean),2)
    }
    meanSqrDiff = numerator/set.length
    dev = Math.sqrt(meanSqrDiff)
    return dev
  }
  def colSum(input_dataframe:DataFrame, input_col_name:String):Double = {
    val oldDF = input_dataframe.na.drop()
    val colDF:DataFrame = oldDF.select(input_col_name)
    val rdd: RDD[Double] = colDF.map(x => x.asInstanceOf[Double] )
    val colRDD: RDD[Row] = colDF.rdd
    //val doubleRDD: RDD[Double] = colRDD.map(row => row(0).asInstanceOf[Double])
    val doubleRDD: RDD[Double] = colRDD.map(row => row(0).toString().toDouble)
    val sum: Double = doubleRDD.sum()
    return sum
  }
  def colAvg(input_dataframe:DataFrame, input_col_name:String):Double = {
    val oldDF = input_dataframe.na.drop()
    val colDF:DataFrame = oldDF.select(input_col_name)
    val colDFCount = colDF.count
    val rdd = colDF.map( x =>  x.asInstanceOf[Double])
    val colRDD: RDD[Row] = colDF.rdd
    val doubleRDD: RDD[Double] = colRDD.map(row => row(0).toString().toDouble)
    val sum: Double = doubleRDD.sum()
    val col_Quotient: Double = sum / colDFCount
    return col_Quotient
  }
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
  def logModRDD(row:Row,row_posistion:Int = 0, base:Double):Row={
    val rowSeq: Seq[Any] = row.toSeq
    var rowListBuffer: ListBuffer[Any] = rowSeq.to[scala.collection.mutable.ListBuffer]
    var value:Double = 0
    //val argument:Double = rowSeq(row_posistion).asInstanceOf[Double]
    val argStr:String = rowSeq(row_posistion).toString
    val argDouble:Double = argStr.toDouble
    value = getLog(base,argDouble )
    rowListBuffer += value
    val newSeq:Seq[Any] = rowListBuffer.toSeq
    val newRow:Row = Row.fromSeq(newSeq)
    return newRow
  }
  def powModRDD(row:Row,row_posistion:Int = 0, exponent:Double):Row={
    val rowSeq: Seq[Any] = row.toSeq
    var rowListBuffer: ListBuffer[Any] = rowSeq.to[scala.collection.mutable.ListBuffer]
    var value:Double = 0
    val argStr:String = rowSeq(row_posistion).toString
    val argDouble:Double = argStr.toDouble
    value = getPow(argDouble,exponent )
    rowListBuffer += value
    val newSeq:Seq[Any] = rowListBuffer.toSeq
    val newRow:Row = Row.fromSeq(newSeq)
    return newRow
  }
  def normalizeModRDD(row:Row,row_posistion:Int = 0, mean:Double,sd:Double,min:Any,max:Any):Row={
    val minium = min.toString().toDouble
    val maximum = max.toString().toDouble
    val rowSeq: Seq[Any] = row.toSeq
    var rowListBuffer: ListBuffer[Any] = rowSeq.to[scala.collection.mutable.ListBuffer]
    var value:Double = 0
    val argStr:String = rowSeq(row_posistion).toString
    val argDouble:Double = argStr.toDouble
    //value = (argDouble - mean)/sd
    val normedVal: Double = (argDouble - minium)/(maximum - minium)
    rowListBuffer += normedVal
    val newSeq:Seq[Any] = rowListBuffer.toSeq
    val newRow:Row = Row.fromSeq(newSeq)
    return newRow
  }
  def setStructFieldType(output_col_name:String,output_col_type:String):StructField={
    val colType = output_col_type.toLowerCase()
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
      //var newField= StructField(output_col_name, org.apache.spark.sql.types.ArrayType, true)
      //outputSchemaFieldArray += newField
      return null
    }
    else if(output_col_type == "sourcebytes_srqed"){
      var newField: StructField = new StructField(output_col_name, org.apache.spark.sql.types.IntegerType, true)
      return newField
    }
    return null
  }
  def powModCol(input_dataframe: DataFrame,input_column_name:String,output_col_name:String,exponent:Double): DataFrame = {
    sparkContext = input_dataframe.rdd.context
    sqlContext = input_dataframe.sqlContext
    val columnsArray: Array[String] = input_dataframe.columns
    if(!columnsArray.contains(input_column_name)){
      println("This input column does not exist....")
      println("I am going kill this job here.  Find out what the correct input column is...")
      sys.addShutdownHook{
        sparkContext.stop()
      }
    }
    val oldDF = input_dataframe.na.drop()
    val output_col_type:String ="double"
    var inputSchema: StructType = oldDF.schema
    var inputSchemaFieldArray: Array[StructField] = inputSchema.toArray
    var inputSchemaFieldBuffer: ArrayBuffer[StructField] = inputSchemaFieldArray.to[mutable.ArrayBuffer]
    var outputSchemaFieldArray:ArrayBuffer[StructField] = inputSchemaFieldBuffer
    outputSchemaFieldArray += setStructFieldType(output_col_name,output_col_type)
    val inDataFrameColsArrayBuff: ArrayBuffer[String] = oldDF.columns.to[mutable.ArrayBuffer]
    var inColPos: Int = inDataFrameColsArrayBuff.indexOf(input_column_name) //this needs to be put in a try-catch
    val inputRDD: RDD[Row] = oldDF.rdd
    val outputRDD =  inputRDD.map( row => powModRDD(row,inColPos,exponent))
    val outputStructType:StructType = StructType(outputSchemaFieldArray.toList)
    println(outputStructType.toString())
    val freshDF: DataFrame = sqlContext.createDataFrame(outputRDD,outputStructType)
    return freshDF
  }
  def logModCol(input_dataframe: DataFrame,input_column_name:String,output_col_name:String,base:Double): DataFrame = {
    sparkContext = input_dataframe.rdd.context
    sqlContext = input_dataframe.sqlContext
    val columnsArray: Array[String] = input_dataframe.columns
    if(!columnsArray.contains(input_column_name)){
      println("This input column does not exist....")
      println("I am going kill this job here.  Find out what the correct input column is...")
      sys.addShutdownHook{
        sparkContext.stop()
      }
    }
    val oldDF = input_dataframe.na.drop()
    val output_col_type:String ="double"
    var inputSchema: StructType = oldDF.schema
    var inputSchemaFieldArray: Array[StructField] = inputSchema.toArray
    var inputSchemaFieldBuffer: ArrayBuffer[StructField] = inputSchemaFieldArray.to[mutable.ArrayBuffer]
    var typeRef:org.apache.spark.sql.types.DoubleType= null
    var outputSchemaFieldArray:ArrayBuffer[StructField] = inputSchemaFieldBuffer
    outputSchemaFieldArray += setStructFieldType(output_col_name,output_col_type)
    val inDataFrameColsArrayBuff: ArrayBuffer[String] = oldDF.columns.to[mutable.ArrayBuffer]
    var inColPos: Int = inDataFrameColsArrayBuff.indexOf(input_column_name) //this needs to be put in a try-catch
    val inputRDD: RDD[Row] = oldDF.rdd
    val outputRDD =  inputRDD.map( row => logModRDD(row,inColPos,base))
    val outputStructType:StructType = StructType(outputSchemaFieldArray.toList)
    println(outputStructType.toString())
    val freshDF: DataFrame = sqlContext.createDataFrame(outputRDD,outputStructType)
    return freshDF
  }
  def normalizeModCol(input_dataframe: DataFrame,input_column_name:String,output_col_name:String,mean:Double,sd:Double): DataFrame = {
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
    val oldDF: DataFrame = input_dataframe.na.drop()
    val descDF: DataFrame = oldDF.describe(input_column_name)
    val descList= descDF.collect().to[mutable.ArrayBuffer]
    val descArray = descList.to[mutable.ArrayBuffer]
    val rowMin = descArray(3)
    val rowMax = descArray(4)
    val min = rowMin(1)
    val max = rowMax(1)
    val output_col_type:String ="double"
    var inputSchema: StructType = oldDF.schema
    var inputSchemaFieldArray: Array[StructField] = inputSchema.toArray
    var inputSchemaFieldBuffer: ArrayBuffer[StructField] = inputSchemaFieldArray.to[mutable.ArrayBuffer]
    var typeRef:org.apache.spark.sql.types.DoubleType= null
    var outputSchemaFieldArray:ArrayBuffer[StructField] = inputSchemaFieldBuffer
    outputSchemaFieldArray += setStructFieldType(output_col_name,output_col_type)
    val inDataFrameColsArrayBuff: ArrayBuffer[String] = oldDF.columns.to[mutable.ArrayBuffer]
    var inColPos: Int = inDataFrameColsArrayBuff.indexOf(input_column_name) //this needs to be put in a try-catch
    val inputRDD: RDD[Row] = oldDF.rdd
    val outputRDD =  inputRDD.map( row => normalizeModRDD(row,inColPos,mean,sd,min,max))
    val outputStructType:StructType = StructType(outputSchemaFieldArray.toList)
    println(outputStructType.toString())
    val freshDF: DataFrame = sqlContext.createDataFrame(outputRDD,outputStructType)
    return freshDF
  }
}
