package com.xxx.xxx.transform

import DerivingCrisMinSchRecords._
import MergeBatch.merge_incremental_batch
import com.xxx.xxx.driver.MemberProfileKafkaToLMODriver.{logger, ss}
import com.xxx.xxx.load.UpsertRecords.upsert_cris_records
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.StructType
import io.delta.tables.DeltaTable
import java.time.{Duration, LocalDateTime, ZoneId}
import com.xxx.xxx.utils.EnviromentConfig.EnvironmentMetadata.{batch_to_vacuum, vacuum_retention_period}

/**
  * project : xxx-xxx-build
  * package : com.xxx.xxx.xxxKafkaToLMO.transform
  * Date :04/May/2023
  */
object ProcessMicroBatch {

  /**
    * DRIVER METHOD. It invokes sequentially the necessary methodS to:
    * Parsing the JSON message
    * Renaming the fields
    * Processing or deriving the fields
    * Incremental merge
    * Update/Insert to deltalake
    *
    * @param input_df           : Dataframe => Input- batch with the xxx Payload coming from Kafka
    * @param batchId            : Long => batch identifier
    * @param xxx_min_schema_dt : DeltaTable => LMO delta table
    * @return => Unit
    *
    */

  def process_micro_batch(input_df: DataFrame, xxx_min_schema_dt: DeltaTable, xxx_schema: StructType, batchId: Long) = {

    val batchStart = LocalDateTime.now(ZoneId.systemDefault)

    logger.info("Batch Start Time for batch id " + batchId + " " + LocalDateTime.now(ZoneId.systemDefault))

    logger.info("Parsing the json into a Dataframe")

    val raw_xxx_record = input_df.select(from_json(col("msg"), xxx_schema).as("profile")) /*,col("offset"), col("partition"))*/

    logger.info("Renaming corresponding fields")

    val renamed_xxx_record = rename_xxx_records(raw_xxx_record)

    logger.info("Processing the required fields")

    val processed_xxx_record = process_xxx_records(renamed_xxx_record)

    logger.info("Merging the incremental batch")

    val merged_incremental_batch = merge_incremental_batch(processed_xxx_record)

    logger.info("Upsertion to delta lake")

    upsert_xxx_records(merged_incremental_batch, xxx_min_schema_dt)

    try {
      if (batchId % batch_to_vacuum.toInt == 0 && batchId != 0) {
        logger.info("Compaction Started... at " + LocalDateTime.now(ZoneId.systemDefault))
        xxx_min_schema_dt.optimize().executeCompaction()
        logger.info("Compaction End... at " + LocalDateTime.now(ZoneId.systemDefault))
      }
      else {
        logger.info("Compaction skipped...")
      }

      if (batchId % batch_to_vacuum.toInt == 0 && batchId != 0) {
        logger.info("Vaccum Optimizing Started ... at " + LocalDateTime.now(ZoneId.systemDefault))
        ss.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", false)
        xxx_min_schema_dt.vacuum(vacuum_retention_period.toInt)
        logger.info("Vaccum Optimizing Ended... at " + LocalDateTime.now(ZoneId.systemDefault))
      }
      else {
        logger.info("Vaccum Optimizing skipped...")
      }
    } catch {
      case e: Exception => logger.error("Error during optimization: ", e)
    }

    val batchEnd = LocalDateTime.now(ZoneId.systemDefault)

    logger.info("Batch End Time for batch id " + batchId + " " + LocalDateTime.now(ZoneId.systemDefault))

    val duration = Duration.between(batchStart, batchEnd)

    logger.info("Overall Time Taken for batch id " + batchId + " " + "is " + duration.toMinutes)

  }

}
