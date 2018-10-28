package com.kamali.exe
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.ml.feature.VectorAssembler
import ml.dmlc.xgboost4j.scala.spark.XGBoostRegressor
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.regression.{GBTRegressionModel, GBTRegressor}
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf
object quartic {

  def main(args: Array[String]) {
   
  val spark  = SparkSession.builder().appName("Quartic Test").master("local[*]").getOrCreate()
  
  //created a spark session; using all cores.
  
  val schema = StructType(
  Array(StructField("Timestamp",StringType),
    StructField("FeaA",DoubleType),
    StructField("FeaB",DoubleType),
    StructField("FeaC",DoubleType),
    StructField("FeaD",DoubleType),
    StructField("FeaE",DoubleType),
    StructField("Label",IntegerType)
  )) 
  //Defined a structure for Data
  
val Rawdata=spark.read.option("header","true").schema(schema).csv("/home/kiran/Pictures/mle_task 2/mle_task/train.csv")
  //Raw data was addressed with the defined schema.  
Rawdata.show()

val cleanedData=Rawdata.na.fill(0)  
  //Filling missing values by 0
import spark.implicits._
val selectiveData=cleanedData.select(cleanedData("Label").as("Label"),$"FeaA",$"FeaB",$"FeaC",$"FeaD",$"FeaE")

selectiveData.show()

val vectorAssembler = new VectorAssembler().setInputCols(Array("FeaA","FeaB","FeaC","FeaD","FeaE"))
  .setOutputCol("features")
  //creating a vector for all the features

 val TrainingData = vectorAssembler.transform(selectiveData).select($"Label",$"features") 
  
TrainingData.show()

val featureIndexer = new VectorIndexer().setInputCol("features")
  .setOutputCol("indexedFeatures")
  .setMaxCategories(5)
  .fit(TrainingData)


  val xgbt = new XGBoostRegressor(Map[String, Any]("num_rounds" -> 100)).setFeaturesCol("indexedFeatures")
   .setLabelCol("Label")
  
  
  
val gbt = new GBTRegressor().setLabelCol("Label").setFeaturesCol("indexedFeatures").setMaxIter(10)

  val pipeline = new Pipeline().setStages(Array(featureIndexer,gbt))
  val model= pipeline.fit(TrainingData)
 println("safe till here");
model.write.overwrite.save("/home/kiran/Pictures/model")
//saving that model

//val samemodel=GBTRegressor.load("/home/kiran/Pictures/")

print("2nd dataframe");
val df2 = spark.read.option("header", true).schema(schema).csv("/home/kiran/Pictures/mle_task 2/mle_task/test.csv").na.fill(0)
df2.show()
val testPriceData=df2.select($"FeaA",$"FeaB",$"FeaC",$"FeaD",$"FeaE")
testPriceData.show()
val TestData = vectorAssembler.transform(testPriceData).select($"features")
TestData.show()
val predictions= model.transform(TrainingData)
predictions.show()

spark.stop()



}
}