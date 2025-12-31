package xxx.xxx.xxx.driver
import xxx.xxx.xxx.entities.xxxMinSchema
import xxx.xxx.xxx.utils.SparkUtils.get_cluster_session
import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions.{lit, date_format}

/**
 * project : xxx-xxx-build
 * package : com.xxx.xxx.xxx.driver
 * Date :04/May/0000
 * @author: Arpan Sharma
 */
object CreatexxxDeltaTable extends App {

  val logger = org.apache.log4j.LogManager.getLogger(getClass)
  val app_name: String = args(0).trim()
  val lmoDeltaPath: String = args(1).trim()

  logger.info("spark submit arguments local")
  logger.info("creating spark session")
  val spark = get_cluster_session(app_name)
  spark.sparkContext.setLogLevel("ERROR")

  logger.info("Creating dummy record")

  import spark.implicits._

  val minSchemaDF = Seq(new xxxMinSchema("111", null, null, null, null, null, null, null, null,
    null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,null,null,null,null,null, null, null, null, null)).toDF

  val df = minSchemaDF
    .withColumn("created_year",date_format(lit("1985-01-01 00:00:00"), "yyyy"))
    .withColumn("created_month",date_format(lit("1985-01-01 00:00:00"), "MM"))

  logger.info("Inserting dummy record")

  df.orderBy("primary_key")
    .write
    .format("delta")
    .option("overwriteSchema", "true")
    .option("enableChangeDataFeed", "true")
    .mode("overwrite")
    .partitionBy("created_year","created_month")
    .save(lmoDeltaPath)

  logger.info("Removing dummy record")

  val minSchemaDt: DeltaTable = DeltaTable.forPath(spark, lmoDeltaPath)
  minSchemaDt.delete("primary_key = '111'")

/*
  logger.info("Display delta table and schema")
  var lmoDeltaDF = spark.read.format("delta").load(lmoDeltaPath)
  lmoDeltaDF.show(false)
  lmoDeltaDF.printSchema()
*/

}
