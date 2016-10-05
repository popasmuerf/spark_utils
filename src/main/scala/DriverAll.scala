/**
  *
Created by mbrown on 4/20/16.
Copyright (c) 2016,  3rd Planet Softworks Inc
All rights reserved.

Redistribution and use, with or without modification, are permitted provided that the following conditions are met:

1. Redistributions of this data must retain the above copyright notice, this list of conditions and the following disclaimer.

2. Redistributions requires prior consent  3rd Planet Softworks Inc.

3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
  */
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import scala.util.Random
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, DataFrame, Row}
import scala.collection.immutable.Range.Inclusive
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object DriverAll {
  def main(args:Array[String]): Unit ={
    //var colOps = new  GeneralColOps()
    val sparkMaster = "local"
    val sparkAppname = "Stand alone Scala Spark app"
    val sparkConf:SparkConf = new SparkConf()
    sparkConf.setAppName(sparkAppname)
    sparkConf.setMaster(sparkMaster)
    //sparkConf.set("spark.driver.allowMultipleContexts", "true")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext:SQLContext = new SQLContext(sparkContext)
    //colOps.setSparkContext(sparkContext)
    //colOps.setSqlContext(sqlContext)
    // GeneralColOps.setSparkContext(sparkContext)
    //GeneralColOps.setSqlContext(sqlContext)
    //GenMathOps.setSparkContext(sparkContext)
    //GenMathOps.setSqlContext(sqlContext)
    //create a dataframe of real numbers
    val numSeq:Seq[Double] = Seq(0000.0,1000.0, 2000.0, 3000.0, 4000.0,5000.0, 6000.0,7000.0, 8000.0,9000.0,10000.0)
    val strSeq:Seq[String] = Seq("zero","one","two","three","four","five","six","seven","eight","nine","ten")
    val protocolSeq:Seq[Int] = Seq(80,443,21,68,53,100,233,888,354,23,25)
    val rowList:ArrayBuffer[Row] = new ArrayBuffer[Row]()
    val rowList1:ArrayBuffer[Row] = new ArrayBuffer[Row]()
    val rowList2:ArrayBuffer[Row] = new ArrayBuffer[Row]()
    for(el <-numSeq){
      rowList += Row(el)
    }
    for(el <-strSeq){
      rowList1 += Row(el)
    }
    val numRDD: RDD[Row] = sparkContext.parallelize(rowList)
    val strRDD : RDD[Row] = sparkContext.parallelize(rowList1)
    val protoRDD : RDD[Row] = sparkContext.parallelize(rowList2)
    var newField: StructField = new StructField("col0", org.apache.spark.sql.types.DoubleType, false)
    var newField1: StructField = new StructField("col1", org.apache.spark.sql.types.StringType, false)
    var newField2: StructField = new StructField("colProto", org.apache.spark.sql.types.IntegerType, false)
    var outputSchemaFieldArray:ListBuffer[StructField] =  new ListBuffer[StructField]()
    var outputSchemaFieldArray1:ListBuffer[StructField] =  new ListBuffer[StructField]()
    var outputSchemaFieldArray2:ListBuffer[StructField] =  new ListBuffer[StructField]()
    outputSchemaFieldArray += newField
    outputSchemaFieldArray1 += newField1
    outputSchemaFieldArray2 += newField2
    val outputStructType:StructType = StructType(outputSchemaFieldArray.toList.toArray)
    val outputStructType1:StructType = StructType(outputSchemaFieldArray1.toList.toArray)
    val df: DataFrame =  sqlContext.createDataFrame(numRDD,outputStructType)
    val df1: DataFrame = sqlContext.createDataFrame(strRDD,outputStructType1)
    val dfRowCollect = df.collect()
    //******************** RUN GENERALCOLOPS ********************
    df.select("*").show()
    df1.select("*").show()
    df.show()
    df1.show()
    val idf = GeneralColOps.addIdModCol(df)
    idf.select("*").show()
    val insdf = GeneralColOps.addCol(idf,"col2","string")
    insdf.select("*").show()
    val insfdf = GeneralColOps.addCol(insdf,"col3","double")
    insfdf.select("*").show()
    //********************END  RUN GENERALCOLOPS ********************
    //******************** RUN GENERALMATHOPS LOCAL ********************
    //GenMathOps.getMean(x)
    //=======================================================
    val numArray = numSeq.toArray
    println("GenMathOps.getMean(x0,x1,x2,x3,x4...xn) : " , GenMathOps.getMean(0000.0,1000.0, 2000.0, 3000.0, 4000.0,5000.0, 6000.0,7000.0, 8000.0,9000.0,10000.0))
    println("GenMathOps.getMean(x(x0,x1,x2,x3,x4)) : " , GenMathOps.getMean(numArray))
    println("GenMathOps.getMean(x0,x1,x2,x3,x4...xn")
    println("Get Log : " +  GenMathOps.getLog(45,100).toString())
    println("Get Pow : " +  GenMathOps.getPow(45,100).toString())
    println("Get Prob: " +  GenMathOps.getProb(50,100).toString())
    println("Get Standard Dev: " +  GenMathOps.getStdDev(numArray).toString())
    //********************END  RUN GENERALMATHOPS ********************

    //******************** RUN GENERALMATHOPS CLUSTER ********************
    //GenMathOps.getMean(x)
    //=======================================================
    val clusterDF = insfdf
    val dlusterDFA: DataFrame = clusterDF.select("col0")
    dlusterDFA.show()
    println("Sum of a column : " +  GenMathOps.colSum(dlusterDFA,"col0"))
    println("Avg of a column : " +  GenMathOps.colAvg(dlusterDFA,"col0"))
    val clusterDFPow: DataFrame = GenMathOps.powModCol(clusterDF,"col0","col0Pow",5)
    clusterDFPow.show()
    val clusterDFLow: DataFrame = GenMathOps.logModCol(clusterDF: DataFrame,"col0","col0Log",10)
    clusterDFLow.show()
    val stddev: Double = GenMathOps.getStdDev(numArray)
    val mean: Double = GenMathOps.getMean(numArray)
    val clusterDFNorm = GenMathOps.normalizeModCol(clusterDF, "col0","col0Norm",mean,stddev)
    clusterDFNorm.show()
    //********************END  RUN GENERALMATHOPS ********************
    //******************** RUN REDCODE NUMERIC CLUSTER ********************
    //GenMathOps.getMean(x)
    //=========val useMapListBuff2: ListBuffer[(Inclusive, Int)] =  ListBuffer (
    val useMapListBuff2: ListBuffer[(Inclusive, Int)] =  ListBuffer (
      (0 to 100,1000) ,
      (101 to 200,2000),
      (201 to 300,3000) ,
      (201 to 1000,5000))
    val protocolDF = null
    val protoDF = GeneralColOps.addCol(clusterDFNorm,"col_prtcl","integer")
    protoDF.show()
    val count = protoDF.count()
    val protoSchema: StructType = protoDF.schema
    val protoRDD2: RDD[Row] = protoDF.rdd
    def setRowAt5(row:Row):Row={
      import scala.util.Random
      var r = scala.util.Random
      val rowSeq = row.toSeq
      var rowListBuffer: ListBuffer[Any] = rowSeq.to[scala.collection.mutable.ListBuffer]
      rowListBuffer(5) =r.nextInt(1001)
      val freshRow: Row =  Row.fromSeq(rowListBuffer)
      return freshRow
    }
    val protoRDD3 = protoRDD2.map(row => setRowAt5(row))
    val protoDF2 = sqlContext.applySchema(protoRDD3,protoSchema)
    protoDF2.show()
    //********************END  RUN GENERALMATHOPS ********************
    //******************** RUN REDCODE NUMERIC CLUSTER ********************

  }

}
