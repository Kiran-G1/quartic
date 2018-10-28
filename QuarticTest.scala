package com.kamali.exe
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.regression.{GBTRegressionModel, GBTRegressor}
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

import org.apache.spark.streaming.{Seconds, StreamingContext}
object QuarticTest{
  def main(args: Array[String]) {    
    
    val sparkConf = new SparkConf().setAppName("QuaritcTest")
    
    val sc =new SparkContext("local[*]","QuarticTest")

    val gbt = new GBTRegressor().setLabelCol("Label").setFeaturesCol("indexedFeatures").setMaxIter(10)

    val vectorAssembler = new VectorAssembler().setInputCols(Array("FeaA","FeaB","FeaC","FeaD","FeaE"))
   .setOutputCol("features")

    val samemodel=GBTRegressor.load("/home/kiran/Pictures/")
    
    
    val ssc = new StreamingContext(sc, Seconds(1))

  
    
    val lines = ssc.socketTextStream("localhost",9999, StorageLevel.MEMORY_AND_DISK_SER)
    val features = lines.flatMap(_.split(","))
    val TestData = Vector(features) 
 
    val predictions= model.transform(TestData)
    predictions.show()


    ssc.start()
    ssc.awaitTermination()
  }
}
