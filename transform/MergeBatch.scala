package com.xxx.xxx.transform

import com.xxx.xxx.utils.UDFsCrisIngestion.{merging_seqmapmaps_udf, merging_seqmaps_udf}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, collect_list, expr}


/**
 * project : xxx-xxx-build
 * package : com.xxx.xxx.memberProfileKafkaToLMO.transform
 * Date :04/May/0000
 */
object MergeBatch {

  /**
   * MERGE INCREMENTAL BATCH METHOD. It invokes method to:
   * Get primitive fields for a latest time stamp with messages of same customer_id
   * Validating
   * Update / Insert / Delete into Deltalake

  @param df: Dataframe => Input- batch with the CRIS Payload coming from Kafka
  @param batchId: Long => batch identifier
   */
  def merge_incremental_batch(processed_df: DataFrame) : DataFrame = {

    val logger = org.apache.log4j.LogManager.getLogger(getClass)
    val update_columns = Seq(
      "message_time",
      "message_uuid",
      "message_source",
      "message_name",
      "message_format",
      "created_date",
      "created_date_time",
      "changed_date_time",
      "xxx_uuid",
      "xxx_timestamp",
      "last_updated",
      "processing_time",
      "created_year",
      "created_month"
      

      /*
      "message_time", "message_uuid", "message_source", "message_name", "message_format","created_date","created_date_time","changed_date_time",
      "xxx_uuid","xxx_timestamp","last_updated", "processing_time","created_year","created_month"
      */

    )

    val merge_seqmap_columns = Seq("address_details","preferences","contact_details")

    val merge_map_columns = Seq("xxx_source_records")

    logger.info("aggregate all columns except the key individually by time_stamp")

    def agg_update_cols = processed_df.columns
      .filter(update_columns.contains(_))
     // .map(colName => expr(s"max_by($colName, if(isNotNull($colName), changed_date_time, null))").as(colName))
      .map {colName =>expr(s"max_by($colName, case when isNotNull($colName) then changed_date_time else null end)").as(colName)}

    def agg_seqmap_merge_cols = processed_df.columns
      .filter(merge_seqmap_columns.contains(_))
      .map(colName => { merging_seqmapmaps_udf(collect_list(col(colName))).as(colName)})

    def agg_map_merge_cols = processed_df.columns
      .filter(merge_map_columns.contains(_))
      .map(colName => { merging_seqmaps_udf(collect_list(col(colName))).as(colName)})

    logger.info("The result of the aggregation")

    val aggregated_update = processed_df
      .groupBy(col("customer_number"))
      .agg(agg_update_cols.head, agg_update_cols.tail: _*)

    aggregated_update.cache().count()

    val aggregated_merge_seqmap = processed_df
      .groupBy(col("customer_number"))
      .agg(agg_seqmap_merge_cols.head, agg_seqmap_merge_cols.tail: _*)

    aggregated_merge_seqmap.cache().count()

    val aggregated_merge_map = processed_df
      .groupBy(col("customer_number"))
      .agg(agg_map_merge_cols.head, agg_map_merge_cols.tail: _*)

    aggregated_merge_map.cache().count()

    aggregated_update.
      join(aggregated_merge_seqmap, List("customer_number")).
      join(aggregated_merge_map, List("customer_number"/*,"action_id"*/))

  }
}
