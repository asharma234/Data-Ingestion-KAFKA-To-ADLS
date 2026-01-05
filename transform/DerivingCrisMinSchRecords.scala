package com.xxx.xxx.transform

import com.xxx.xxx.utils.UDFsCrisIngestion.{derive_address_udf, derive_xxx_source_records_udf, tsConversionToLongUdf,  derive_preference_udf, derive_profile_contact_udf, generate_xxx_timestamp_udf, parse_tselement_udf, validate_string_udf}
import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.functions._

/**
  * project : xxx-xxx-build
  * package : com.xxx.xxx.xxxKafkaToLMO.transform
  * Date :04/May/2023
  */
object DerivingxxxMinSchRecords {
  /**
    * Method to rename the xxx Mimum Schema columns from the xxx source format to what is expect in our Repository Table
    *
    * @param xxx_record_raw : DataFrame => Raw Dataframe coming from xxx Kafka Topic
    * @return DataFrame => xxx Columns renamed
    */
  def rename_xxx_records(xxx_record_raw: DataFrame): DataFrame = {
    cris_record_raw.select(
      col("profile.metadata.messageTime").as("message_time"),
      col("profile.metadata.messageUUID").as("message_uuid"),
      col("profile.metadata.messageSource").as("message_source"),
      col("profile.metadata.messageName").as("message_name"),
      col("profile.metadata.messageFormat").as("message_format"),
     
      col("profile.message.profileCreated").as("created_date_time"),
      col("profile.message.createdDateTime").as("changed_date_time"),
      col("profile.message.customerID").as("customer_number"),
      col("profile.message.addressDetails").as("address_details"),
      col("profile.message.preferenceDetails").as("preferences"),
      col("profile.message.profileContactDetails").as("contact_details")
      
    )
  }

  /**
    * Method to generate/derive column values
    *
    * @param xxx_record_renamed : DataFrame => Renamed Dataframe with required columns
    * @return DataFrame => xxx Column values generated/derived
    */
  def process_xxx_records(xxx_record_renamed: DataFrame): DataFrame = {


    val transDirFields: Map[String, Column] = Map(
      "message_time" -> validate_string_udf(col("message_time")),
      "message_uuid" -> validate_string_udf(col("message_uuid")),
      "message_name" -> validate_string_udf(col("message_name")),
      "message_format" -> validate_string_udf(col("message_format")),
      "created_date_time" -> validate_string_udf(col("created_date_time")),
      "created_date" -> validate_string_udf(date_format(col("created_date_time"), "yyyy-MM-dd")),
      "changed_date_time" -> when(col("changed_date_time").isNull, lit("1985-01-01 01:01:01.001")).otherwise(col("changed_date_time")),
      "customer_number" -> validate_string_udf(col("customer_number")),
      "xxx_uuid" -> validate_string_udf(col("xxx_uuid")),
	  "xxx_timestamp" -> validate_string_udf(col("xxx_timestamp")),
      "processing_time" -> from_unixtime(generate_helix_timestamp_udf(), "yyyy-MM-dd HH:mm:ss")
    )

    val transDerFields: Map[String, Column] = Map(
      "changed_date_time" -> parse_tselement_udf(col("changed_date_time")),
      "created_year" -> validate_string_udf(date_format(col("created_date"), "yyyy")),
      "created_month" -> validate_string_udf(date_format(col("created_date"), "MM"))
    )

    val transComFields: Map[String, Column] = Map(
      "last_updated" -> tsConversionToLongUdf(col("changed_date_time")),
      "xxx_source_records" -> derive_xxx_source_records_udf(col("xxx_uuid"), col("xxx_timestamp"), lit("CRIS")),
      "address_details" -> derive_address_udf(col("address_details"), col("changed_date_time"), col("created_date_time")),
      "preferences" -> derive_preference_udf(col("preferences"), col("changed_date_time"), col("created_date_time")),
      "contact_details" -> derive_profile_contact_udf(col("contact_details"), col("changed_date_time"), col("created_date_time")),
      "minor_details" -> derive_minor_details_udf(col("minor_details"), col("changed_date_time"), col("created_date_time")),
      "guardian_details" -> derive_guardian_details_udf(col("guardian_details"), col("changed_date_time"), col("created_date_time")),
      "linked_member_details" -> derive_linked_member_details_udf(col("linked_member_details"), col("changed_date_time"), col("created_date_time"))
    )

    // Apply transformations
    val updatedDF = xxx_record_renamed
      .withColumns(transDirFields)
      .withColumns(transDerFields)
      .withColumns(transComFields)

    updatedDF

  }

}


