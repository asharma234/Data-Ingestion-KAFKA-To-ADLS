package xxx.xxx.xxx.driver

import xxx.xxx.xxx.entities.xxxSchema
import xxx.xxx.xxx.extract.ExtractxxxKafkaRecords.defining_kafka_stream_payload
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession, functions}
import org.apache.spark.sql.types.StructType
import io.delta.tables.DeltaTable
import org.apache.spark.sql.streaming.Trigger
import java.time.{LocalDateTime, ZoneId}
import org.apache.hadoop.io.{BytesWritable, LongWritable}
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import org.apache.spark.SparkConf
import xxx.xxx.xxx.transform.ProcessMicroBatch.process_micro_batch
import xxx.xxx.xxx.utils.EnviromentConfig.params
import xxx.xxx.xxx.utils.EnviromentConfig.EnvironmentMetadata._
import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}

/**
  * project : xxx-xxx-build
  * package : xxx.xxx.xxx.xxx.driver
  * Date :04/May/2023
  * @Author : Arpan Sharma
  * 
  */
object xxxKafkaToxxxDriver extends App {

  val configFile = args(0).trim

  val logger = org.apache.log4j.LogManager.getLogger(getClass)

  val sparkConf = new SparkConf()
  params.get.foreach { keyValue => sparkConf.set(keyValue._1, keyValue._2) }

  val ss: SparkSession = SparkSession
    .builder()
    .master("yarn")
    .config(sparkConf)
    .getOrCreate()

  ss.sparkContext.setLogLevel("info")

  logger.info("create schema object")

  val xxx_schema: StructType = Encoders.product[xxxSchema].schema

  val lmo_tbl_delta_path: DeltaTable = DeltaTable.forPath(ss, cris_min_schema_path)

  logger.info("write streaming data to delta table")

  data_flow.toLowerCase() match {

    case s if s == "streaming" || s == "stream" => {

      logger.info("Started reading streaming messages from batch")

      val kafka_payload = defining_kafka_stream_payload(ss, kafka_bootstrap_config, kafka_xxx_topic, kafka_starting_offsets, kafka_maxOffsets_PerTrigger)

      logger.info("Streaming messages from batch")

      withRetry(write_stream_payload(ss, kafka_payload, lmo_tbl_delta_path, xxx_schema), max_retries.toInt, retry_delay_in_secs.toInt.seconds)

      //write_stream_payload(ss, kafka_payload, lmo_tbl_delta_path, xxx_schema)
    }
    case "batch" =>
      Try {
        import ss.implicits._

        val branded_xxx_batch_path_adls = args(1)

        val process_time = args(2)

        val datePattern = "(\\d{4}-\\d{2}-\\d{2})".r

        val extractedDate = datePattern.findFirstIn(branded_xxx_batch_path_adls).getOrElse("")

        val updatedPath = if (process_time == "01-00") {
          val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
          val currentDate = LocalDate.parse(extractedDate, formatter).minusDays(1)
          val previousDate = currentDate.minusDays(1).format(formatter)
          val nextDate = currentDate.plusDays(1).format(formatter)
          branded_xxx_batch_path_adls.replace(extractedDate, s"{$previousDate,$currentDate,$nextDate}")
        } else {
          branded_xxx_batch_path_adls // Keep the original path if no date is found
        }

        logger.info("Finalized path for this batch run " + updatedPath)

        var BrandedSeqFileDf = ss.sparkContext.sequenceFile(updatedPath, classOf[LongWritable], classOf[BytesWritable])
          .map(x => new String(x._2.copyBytes(), "utf-8"))
          .toDS().selectExpr("value msg")
          .withColumn("message_time",
            functions.substring(functions.regexp_replace(functions.regexp_extract($"msg","""messageTime.:"([^\\s",]+)""", 0),"""messageTime":"""", ""), 0, 10))

        BrandedSeqFileDf = if (process_time == "01-00") {
          val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
          val extractedDateMinusOne = LocalDate.parse(extractedDate, formatter).minusDays(1).toString
          logger.info("Batch Processing Filter Date "+ extractedDateMinusOne)
          BrandedSeqFileDf.filter(functions.col("message_time") === extractedDateMinusOne).drop("message_time")
        } else {
          logger.info("Batch Processing Filter Date "+ extractedDate)
          BrandedSeqFileDf.filter(functions.col("message_time") === extractedDate).drop("message_time")
        }
        process_micro_batch(BrandedSeqFileDf, lmo_tbl_delta_path, xxx_schema, 1)

      }
      match {
        case Success(_) => "*** Branded To LMO PROCESS SUCCESSFUL ***"
        case Failure(exception) => s"***  Branded To LMO PROCESS FAILED ***\n${
          exception.getMessage
        }"
          throw exception
      }
  }
  logger.info("*************** Operation Retry Handler ***************")

  def withRetry[T](operation: => T, maxRetries: Int, delay: FiniteDuration): T = {
    var result: Option[T] = None
    var retries = 0
    while (result.isEmpty && retries < maxRetries) {
      Try {
        result = Some(operation)
      } match {
        case Success(_) =>
        case Failure(ex) =>
          retries += 1
          if (retries < maxRetries) {
            logger.info(s"Batch Time when failure occur -->  ${LocalDateTime.now(ZoneId.systemDefault)} Retry attempt $retries after ${delay.toSeconds} seconds due to: ${ex.getMessage}")
            Thread.sleep(delay.toMillis)
          } else {
            throw ex
          }
      }
    }
    result.getOrElse(throw new RuntimeException("Retry operation failed after maximum retries."))
  }

  logger.info("*************** Write Payload Function ***************")

  def write_stream_payload(ss: SparkSession, incomingDf: DataFrame, lmo_data_path: DeltaTable, xxx_data_schema: StructType): Unit = {
    incomingDf
      .writeStream
      .format("console")
      .queryName("cris_msg")
      .outputMode("append")
      .option("mergeSchema", "true")
      .option("checkpointLocation", chkPntDir)
      .trigger(Trigger.ProcessingTime("120 seconds"))
      .foreachBatch { (batchDF: DataFrame, batchId: Long) => {
        ss.catalog.clearCache()
        process_micro_batch(batchDF, lmo_data_path, xxx_data_schema, batchId)
        ss.catalog.clearCache()
      }
      }
      .start()
      .awaitTermination()
  }
}
