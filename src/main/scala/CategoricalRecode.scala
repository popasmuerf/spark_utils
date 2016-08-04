import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StructType, StructField}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, DataFrame, SQLContext}

import scala.collection.immutable.Range.Inclusive
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  *
Created by mbrown on 5/12/16.
Copyright (c) 2016, 3rd Planet Softworks Inc
All rights reserved.

Redistribution and use, with or without modification, are permitted provided that the following conditions are met:

1. Redistributions of this data must retain the above copyright notice, this list of conditions and the following disclaimer.

2. Redistributions requires prior consent 3rd Planet Softworks Inc

3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
  */
object CategoricalRecode {
  var sparkContext: SparkContext = null
  var sqlContext:SQLContext = null
  var hiveContext:org.apache.spark.sql.hive.HiveContext = null
  var origDF: DataFrame = null
  var inputColname:String = null
  var outputColName: String = null
  var outputColType: String = null
  var methodOp = "numerical_mapper"
  var userMapListBuff:ListBuffer[(Inclusive, Int)] = null
  var rangeMapArray:Array[(Inclusive, Int)] = null
  var rangeMapList:ListBuffer[(Inclusive, Int)] = null
  var rangeMapSeq:Seq[(Inclusive, Int)] = null
  var useMapListBuff2 : mutable.Map[String, (String, String, String)] = null

  def categoricalMapper(rowVal:Any,catMap:Map[String,String]):String={
    var category:String = null
    val netflag: String = rowVal.toString().replaceAll("""\s+""" ,"")
    category = catMap(netflag)
    return category
  }
  def modRDD(row:Row,row_posistion:Int,catMap:Map[String,String]):Row={
    val rowSeq: Seq[Any] = row.toSeq
    val rowListBuffer : ListBuffer[Any] = rowSeq.to[scala.collection.mutable.ListBuffer]
    var value:String = null
    var rowVal: Any = rowListBuffer(row_posistion)
    value = categoricalMapper(rowVal,catMap)
    rowListBuffer += value
    val newSeq:Seq[Any] = rowListBuffer.toSeq
    val newRow:Row = Row.fromSeq(newSeq)
    return newRow
  }

  def setStructFieldTyåpe(output_col_name:String):StructField={
    val colType = "string"
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
  def recode(input_dataframe: DataFrame,input_column_name:String,output_col_name:String,catMap:Map[String,String]): DataFrame = {
    sparkContext = input_dataframe.rdd.context
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
    val inColPos: Int = inDataFrameColsArrayBuff.indexOf(input_column_name) //this needs to be put in a try-catch
    val inputRDD: RDD[Row] = oldDF.rdd
    val outputRDD =  inputRDD.map( row => modRDD(row,inColPos,catMap))
    val outputStructType:StructType = StructType(outputSchemaFieldArray.toList)
    val freshDF: DataFrame = sqlContext.createDataFrame(outputRDD,outputStructType)
    return freshDF
  }
}
