package xxx.xxx.xxx.extract

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

/**
 * project : xxx-xxx-build
 * package : xxx.xxx.xxx.xxx.extract
 * Date :04/May/2023
 */
object ExtractxxxKafkaRecords {

  def defining_kafka_stream_payload(ss: SparkSession, kafka_bootstrap_config: String, kafka_xxx_topic: String, kafka_starting_offsets:String,kafka_maxOffsets_PerTrigger: String): DataFrame = {

    var kafka_stream = ss
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafka_bootstrap_config)
      .option("subscribe", kafka_xxx_topic)
      .option("startingOffsets", kafka_starting_offsets)
      .option("maxOffsetsPerTrigger", kafka_maxOffsets_PerTrigger)
      .option("minOffsetsPerTrigger", 1)
      .option("kafka.security.protocol","SASL_SSL")
      .option("kafka.sasl.kerberos.service.name","kafka")
      .option("kafka.sasl.mechanism","GSSAPI")
   /*   .option("kafka.group.id", "xxx_xxx_xxx_grp")*/
      .load()

    kafka_stream.selectExpr("cast (value as string) as msg ")

/*
    kafka_stream.select(
      col("offset").cast("string"),
      col("partition").cast("string"),
      col("value").cast("string").alias("msg"))
*/

  }
}
