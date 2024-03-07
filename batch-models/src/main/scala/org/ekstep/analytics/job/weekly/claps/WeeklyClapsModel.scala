package org.ekstep.analytics.dashboard.weekly.claps

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.ekstep.analytics.dashboard.DashboardUtil.{cassandraTableAsDataFrame, druidDFOption, emptySchemaDataFrame, parseConfig, show}
import org.ekstep.analytics.dashboard.{DashboardConfig, DashboardUtil, DummyInput, DummyOutput}
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.{Empty, FrameworkContext, IBatchModelTemplate}
import org.ekstep.analytics.updater.ContentMetrics
import org.ekstep.analytics.util.{AppConfig, DummyInput, DummyOutput}
import org.joda.time.format.DateTimeFormatter
import org.joda.time.{DateTime, DateTimeConstants, DateTimeZone, format}
import org.joda.time.format.DateTimeFormat
import org.joda.time.DateTimeZone

object WeeklyClapsModel extends IBatchModelTemplate[String, DummyInput, DummyOutput, DummyOutput] with Serializable{

  implicit val className: String = "org.ekstep.analytics.dashboard.weekly.claps.WeeklyClapsModel"

  override def name() = "WeeklyClapsModel"

   def preProcess(events: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyInput] = {
     JobLogger.log("WeeklyClapsModel preProcess started")
    val executionTime = System.currentTimeMillis()
     JobLogger.log("WeeklyClapsModel preProcess ended")
    sc.parallelize(Seq(DummyInput(executionTime)))
  }

   def algorithm(events: RDD[DummyInput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyOutput] = {
     JobLogger.log("WeeklyClapsModel algorithm started")
    val timestamp = events.first().timeStap// extract timestamp from input
    implicit val spark: SparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()
    processWeeklyClaps(timestamp, config)
     JobLogger.log("WeeklyClapsModel algorithm ended")
    sc.parallelize(Seq()) // return empty rdd
  }

   def postProcess(events: RDD[DummyOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyOutput] = {
    sc.parallelize(Seq())
  }

  def processWeeklyClaps(l: Long, config: Map[String, AnyRef]) (implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    // parse model config
    JobLogger.log("WeeklyClapsModel processWeeklyClaps started")
    println(config)

    // get weekStart, weekEnd and dataTillDate(previous day) from today's date
    val (weekStart, weekEnd, weekEndTime, dataTillDate) = getThisWeekDates()
    implicit val conf: DashboardConfig = parseConfig(config)
    //    val weekStart = ""     //for manual testing
    //    val weekEndTime = ""

    //get existing weekly-claps data
    var df = learnerStatsDataFrame()(spark,sc,fc,conf)
    // get platform engagement data from summary-events druid datasource
    val platformEngagementDF = usersPlatformEngagementDataframe(weekStart, weekEndTime)(spark,conf)
    JobLogger.log("WeeklyClapsModel platformEngagementDF ")
    df = df.join(platformEngagementDF, Seq("userid"), "full")

    df = df.withColumn("w4", map(
      lit("timespent"), when(col("platformEngagementTime").isNull, 0).otherwise(col("platformEngagementTime")),
      lit("numberOfSessions"), when(col("sessionCount").isNull, 0).otherwise(col("sessionCount"))
    ))

    val condition = col("w4")("timespent") >= 60.0 && !col("claps_updated_this_week")

    if(dataTillDate.equals(weekEnd) && !dataTillDate.equals(df.select(col("last_updated_on")))) {
      JobLogger.log("Started weekend updates")
      df = df.select(
        col("w2").alias("w1"),
        col("w3").alias("w2"),
        col("w4").alias("w3"),
        col("w4"),
        col("total_claps"),
        col("userid"),
        col("platformEngagementTime"),
        col("sessionCount"),
        col("claps_updated_this_week")
      )
      df = df.withColumn("total_claps", when(col("w4")("timespent") < 60.0, 0).otherwise(col("total_claps")))
        .withColumn("total_claps", when(condition, col("total_claps") + 1).otherwise(col("total_claps")))
        .withColumn("last_updated_on", lit(dataTillDate))
        .withColumn("claps_updated_this_week", lit(false))
        .withColumn("w4", map(lit("timespent"), lit(0.0), lit("numberOfSessions"), lit(0)))

      JobLogger.log("Completed weekend updates")

    } else {
      df = df.withColumn("total_claps", when(condition, col("total_claps") + 1).otherwise(col("total_claps")))
        .withColumn("claps_updated_this_week", when(condition, lit(true)).otherwise(col("claps_updated_this_week")))
    }

    df = df.withColumn("total_claps", when(col("total_claps").isNull, 0).otherwise(col("total_claps")))
      .withColumn("claps_updated_this_week", when(col("claps_updated_this_week").isNull, false).otherwise(col("claps_updated_this_week")))

    df = df.drop("platformEngagementTime","sessionCount")

    writeToCassandra(df, "sunbird", "learner_stats")
  }

  def getThisWeekDates(): (String, String, String, String) = {
    JobLogger.log("WeeklyClapsModel getThisWeekDates started")
    val istTimeZone = DateTimeZone.forID("Asia/Kolkata")
    val currentDate = DateTime.now(istTimeZone)
    val dateFormatter: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd").withZone(istTimeZone)
    val formatter: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZone(istTimeZone)
    val dataTillDate = currentDate.minusDays(1)
    val startOfWeek = dataTillDate.withDayOfWeek(DateTimeConstants.MONDAY).withTimeAtStartOfDay()
    val endOfWeek = startOfWeek.plusDays(6).withTime(23, 59, 59, 999)
    JobLogger.log("WeeklyClapsModel getThisWeekDates ended")
    (formatter.print(startOfWeek), dateFormatter.print(endOfWeek), formatter.print(endOfWeek), dateFormatter.print(dataTillDate))
  }

  def learnerStatsDataFrame()(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): DataFrame = {
    JobLogger.log("WeeklyClapsModel learnerStatsDataFrame started")
    val df = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraLearnerStatsTable)
    show(df, "Learner stats data")
    df
  }

  def cassandraTableAsDataFrame(keySpace: String, table: String)(implicit spark: SparkSession): DataFrame = {
    JobLogger.log("WeeklyClapsModel cassandraTableAsDataFrame started")
    spark.read.format("org.apache.spark.sql.cassandra")
      .option("inferSchema", "true")
      .option("keyspace", keySpace)
      .option("table", table)
      .load()
      .persist(StorageLevel.MEMORY_ONLY)
  }

  def usersPlatformEngagementDataframe(weekStart: String, weekEnd: String)(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    val query = raw"""SELECT uid AS userid, SUM(total_time_spent) / 60.0 AS platformEngagementTime, COUNT(*) AS sessionCount FROM \"summary-events\" WHERE dimensions_type='app' AND __time >= TIMESTAMP '${weekStart}' AND __time <= TIMESTAMP '${weekEnd}' AND uid IS NOT NULL GROUP BY 1"""
    val df = druidDFOption(query, AppConfig.getConfig("druid.sql.host"), limit = 1000000).orNull
    if (df == null) return emptySchemaDataFrame(usersPlatformEngagementSchema)
    show(df, "usersPlatformEngagementDataframe")
    df
  }

  val usersPlatformEngagementSchema: StructType = StructType(Seq(
    StructField("userid", StringType, nullable = true),
    StructField("platformEngagementTime", FloatType, nullable = true),
    StructField("sessionCount", IntegerType, nullable = true)
  ))

  def writeToCassandra(data: DataFrame, keyspace: String, table: String)(implicit spark: SparkSession): Unit = {
    data.write.format("org.apache.spark.sql.cassandra")
      .mode("append")
      .options(Map("table" -> table, "keyspace" -> keyspace))
      .save()
  }
}