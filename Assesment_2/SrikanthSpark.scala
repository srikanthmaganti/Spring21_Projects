package com.srikanth.all_programs

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{CharType, FloatType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.log4j.Level


object SparkQuestions extends App{

  val vList:IndexedSeq[StructField] = for(i <- 1 to 60) yield StructField("V"+i,FloatType)
  val data1Fields = Array(StructField("ID",IntegerType)) ++ vList.slice(0,20)
  val data3Fields = Array(StructField("Class",StringType), StructField("ID",IntegerType)) ++ vList.slice(40,60)
  val data1Schema = StructType(data1Fields.toArray)
  val data3Schema = StructType(data3Fields.toArray)

  Logger.getLogger("org").setLevel(Level.OFF)

  //Creation of Spark session object
  val spark = SparkSession.builder().appName("SparkQuestions")
    .master("local[1]")
    .getOrCreate()

  //Below lines of code represents to read text file
  val textfile = "src/main/resources/all_programs/SparkQuestions/data1.txt"
  val data1 = spark.read
    .format("csv")
    .schema(data1Schema)
    .option("header","true")
    .option("sep","|")
    .load(textfile)
  data1.show()
  println(data1.schema)

  // Below lines of code represents to read XML file
  val xmlfile = "src/main/resources/all_programs/SparkQuestions/data1.XML"
  val data2 = spark.read.format("com.databricks.spark.xml")
    .option("rowTag","data_row")
    .load(xmlfile)
  data2.show()
  println(data2.schema)

  //Below lines of code represents to read Json file
  val jsonfile = "src/main/resources/all_programs/SparkQuestions/data1.json"
  val data3 = spark.read.option("multiline","true").schema(data3Schema).json(jsonfile)
  data3.show()
  println(data3.schema)

  //Below lines of code represent to merge the data frames
  val all_data = data1.join(data2,"ID").join(data3,"ID")
  all_data.show()

  //To create dataframe for reusing and caching
  all_data.cache()
  // create temp table for sql operations
  all_data.createOrReplaceTempView("all_data123")

  //Below lines of code represent new column for row_number
  val all_data_new = all_data.withColumn("ID_NEW",monotonically_increasing_id()+1)
  all_data_new.show()

  // Stats for columns V1...V5 on Class type
  val data_median = all_data.groupBy("Class").agg(min("V1") , max("V1"),mean("V1"),
    min("V2"), max("V2"),mean("V2"),
    min("V3"), max("V3"),mean("V3"),
    min("V4"), max("V4"),mean("V4"),
    min("V5"), max("V5"),mean("V5"))
    .toDF("class", "v1_min", "v1_max","v1_mean","v2_min", "v2_max","v2_mean","v3_min", "v3_max","v3_mean",
      "v4_min", "v4_max","v4_mean", "v5_min", "v5_max","v5_mean")
  data_median.show()

  //  val data_MaxMean = spark.sql("select Class, max(V1) V1_MAX, mean(V1) V1_MEAN, max(V2) V2_MAX, mean(V2) V2_MEAN," +
//    "max(V3) V3_MAX, mean(V3) V3_MEAN, max(V4) V4_MAX, mean(V4) V4_MEAN  from all_data123 group by Class")
//  data_MaxMean.show()

  //Below lines of code represents to filter records having only the max of v1
  val data_maximumrecords = spark.sql("select * from " +
    "(select *, rank() over (partition by Class order by V1 desc) rank from all_data123) where rank = 1")
  data_maximumrecords.show()
}