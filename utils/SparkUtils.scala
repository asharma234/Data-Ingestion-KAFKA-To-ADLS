package com.xxx.xxx.utils

import org.apache.spark.sql.SparkSession

/**
 * project : xxx-xxx-build
 * package : com.xxx.xxx.xxxKafkaToLMO.utils
 * Date :04/May/0000
 */
object SparkUtils {
  def get_local_session(appName:String):SparkSession={

    val ss=SparkSession.builder
      .master("local")
      .appName("Kafka Stream Demo")
      //      .enableHiveSupport()
      .config("spark.hadoop.mapreduce.output.fileoutputformat.compress", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max", "1024")
      .config("spark.rdd.compress", "true")
      .config("spark.broadcast.compress", "true")
      .config("spark.shuffle.compress", "true")
      // .config("spark.akka.frameSize", "512")
      // .config("spark.akka.threads", "10")
      .config("spark.io.compression.codec", "snappy")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.jars.packages", "io.delta:delta-core_2.12:2.0.0")
//      .config("spark.sql.legacy.timeParserPolicy","LEGACY")
      .getOrCreate()
      ss
  }

  def get_cluster_session(appName:String):SparkSession={
    val ss=SparkSession.builder
      .master("yarn")
      .appName(appName)
      //      .enableHiveSupport()
      .config("spark.hadoop.mapreduce.output.fileoutputformat.compress", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max", "1024")
      .config("spark.rdd.compress", "true")
      .config("spark.broadcast.compress", "true")
      .config("spark.shuffle.compress", "true")
      // .config("spark.akka.frameSize", "512")
      // .config("spark.akka.threads", "10")
      .config("spark.io.compression.codec", "snappy")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.jars.packages", "io.delta:delta-core_2.12:2.0.2")
      .config("spark.jars.packages", "io.delta:delta-storage-2.0.2")
      .config("spark.sql.objectHashAggregate.sortBased.fallbackThreshold","10000")
//      .config("spark.sql.legacy.timeParserPolicy","LEGACY")
      .getOrCreate()
    ss
  }


}
