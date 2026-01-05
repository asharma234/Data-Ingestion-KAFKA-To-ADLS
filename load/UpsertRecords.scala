package xxx.xxx.xxx.load

import xxx.xxx.xxx.driver.xxx.ss
import xxx.xxx.xxx.utils.UDFsCrisIngestion._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import io.delta.tables.DeltaTable
import java.time.{LocalDateTime, ZoneId}

/**
  * project : xxx-xxx-build
  * package : com.xxx.xxx.xxx.load
  * Date :04/May/0000
  */
object UpsertRecords {

  /**
    * Method to update/merge the new records coming from Kafka with records stored in Delta Lake
    *
    * @param upsert_processed_xxx_records : DataFrame => Dataframe with the incremental xxx records
    * @param xxx_min_schema_dt            : DeltaTable => Delta Table with the existing xxx records
    * @return => Unit
    *
    */
  def upsert_xxx_records(upsert_processed_xxx_records: DataFrame, xxx_min_schema_dt: DeltaTable) = {


    /*
        *Comment this below logic for future reference
        *
       val getpartition = upsert_processed_xxx_records
         .select(col("created_date"))
         .filter(col("created_date").isNotNull)
         .distinct()

       val splitParttiton = getpartition.
         selectExpr(
           "date_format(created_date, 'yyyy') as created_year",
           "date_format(created_date, 'MM') as created_month"
         ).distinct()
         .orderBy("created_year", "created_month")

       import ss.implicits._

       val resultSeq = splitParttiton.map(row => (row.getAs[Int]("created_year").toString, row.getAs[Int]("created_month").toString)).collect().toSeq

       val partitionCondition = resultSeq
         .map { case (created_year, created_month) => s"('$created_year', '$created_month')" }
         .mkString(", ")

       //val fullCondition = s"((ex.created_year, ex.created_month) IN ($partitionCondition or (ex.created_year IS NULL and ex.created_month IS NULL))"

       val fullCondition = s"((ex.created_year IS NULL AND ex.created_month IS NULL) OR (ex.created_year, ex.created_month) IN ($partitionCondition))"

      // println(s"${fullCondition} AND " + "ex.customer_number = in.customer_number")

   */
    xxx_min_schema_dt
      .as("ex")
      .merge(
        upsert_processed_xxx_records
          .as("in"),
        //s"${fullCondition} AND " +
          "ex.customer_number = in.customer_number")
      .whenMatched("ex.last_updated < in.last_updated")
      .update(Map(
        "message_time" -> update_string_udf(col("in.message_time"), col("ex.message_time")),
        "message_uuid" -> update_string_udf(col("in.message_uuid"), col("ex.message_uuid")),
        "message_source" -> update_string_udf(col("in.message_source"), col("ex.message_source")),
        "message_name" -> update_string_udf(col("in.message_name"), col("ex.message_name")),
        "message_format" -> update_string_udf(col("in.message_format"), col("ex.message_format")),
        "created_date" -> update_string_udf(col("in.created_date"), col("ex.created_date")),
        "created_date_time" -> update_string_udf(col("in.created_date_time"), col("ex.created_date_time")),
        "changed_date_time" -> update_string_udf(col("in.changed_date_time"), col("ex.changed_date_time")),
        
        "address_details" -> merging_2mapmaps_udf(col("in.address_details"), col("ex.address_details")),
        "preferences" -> merging_2mapmaps_udf(col("in.preferences"), col("ex.preferences")),
        "contact_details" -> merging_2mapmaps_udf(col("in.contact_details"), col("ex.contact_details")),
        "xxx_source_records" -> update_xxx_source_records_udf(col("in.xxx_source_records"), col("ex.xxx_source_records")),
        // "bin" -> update_string_udf(col("in.bin"), col("ex.bin")),

        "created_year" -> update_string_udf(col("in.created_year"), col("ex.created_year")),
        "created_month" -> update_string_udf(col("in.created_month"), col("ex.created_month"))
        
      ))
      .whenMatched("ex.last_updated >= in.last_updated")
      .update(Map(
        "message_time" -> update_string_udf(col("ex.message_time"), col("in.message_time")),
        "message_uuid" -> update_string_udf(col("ex.message_uuid"), col("in.message_uuid")),
        "message_source" -> update_string_udf(col("ex.message_source"), col("in.message_source")),
        "message_name" -> update_string_udf(col("ex.message_name"), col("in.message_name")),
        "message_format" -> update_string_udf(col("ex.message_format"), col("in.message_format")),
        "created_date" -> update_string_udf(col("ex.created_date"), col("in.created_date")),
        "created_date_time" -> update_string_udf(col("ex.created_date_time"), col("in.created_date_time")),
        "changed_date_time" -> update_string_udf(col("ex.changed_date_time"), col("in.changed_date_time")),
        "address_details" -> merging_2mapmaps_udf(col("ex.address_details"), col("in.address_details")),
        "preferences" -> merging_2mapmaps_udf(col("ex.preferences"), col("in.preferences")),
        "contact_details" -> merging_2mapmaps_udf(col("ex.contact_details"), col("in.contact_details")),
        "xxx_source_records" -> update_xxx_source_records_udf(col("ex.xxx_source_records"), col("in.xxx_source_records")),
        // "bin" -> update_string_udf(col("ex.bin"), col("in.bin")),

        "created_year" -> update_string_udf(col("ex.created_year"), col("in.created_year")),
        "created_month" -> update_string_udf(col("ex.created_month"), col("in.created_month"))
       ))
      .whenNotMatched
      .insertAll()
      .execute()

}
}



