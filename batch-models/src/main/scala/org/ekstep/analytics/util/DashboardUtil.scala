package org.ekstep.analytics.dashboard

import org.apache.hadoop.fs.{FileSystem, Path}
//import redis.clients.jedis.{Jedis, JedisPool}
import org.apache.spark.SparkContext
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.util.EntityUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SaveMode, SparkSession, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.dispatcher.KafkaDispatcher
import org.ekstep.analytics.framework.storage.CustomS3StorageService
import org.joda.time.{DateTime, DateTimeConstants, DateTimeZone, format}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.sunbird.cloud.storage.BaseStorageService
import org.sunbird.cloud.storage.factory.{StorageConfig, StorageServiceFactory}
//import redis.clients.jedis.exceptions.JedisException
//import redis.clients.jedis.params.ScanParams

import java.io.{File, FileWriter, Serializable}
import java.util
import scala.util.Try
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.reflect.io.Directory

case class DashboardConfig(
                                  debug: String,
                                  validation: String,
                                  cutoffTime: Float,
                                  cassandraUserKeyspace: String,
                                  cassandraLearnerStatsTable: String
                                ) extends Serializable


object DashboardUtil extends Serializable {

  implicit var debug: Boolean = false
  implicit var validation: Boolean = false

  /**
   * Adds more utility functions to spark DataFrame
   * @param df implicit data frame reference
   */
  implicit class DataFrameMod(df: DataFrame) extends Serializable {

    /**
     * for each value in column `groupByKey`, order rows by `orderByKey` and filter top/bottom rows
     * does not order the final data frame, `rowNumColName` is added
     *
     * @param groupByKey cols to group by
     * @param orderByKey col to order by
     * @param limit number of rows to take from each group
     * @param desc descending flag for the orderByKey
     * @param rowNumColName column name for group wise row numbers
     * @return data frame with the operation applied
     */
    def groupByLimit(groupByKey: Seq[String], orderByKey: String, limit: Int, desc: Boolean = false,
                     rowNumColName: String = "rowNum"): DataFrame = {
      if (groupByKey.isEmpty) throw new Exception("groupByLimit error: groupByKey is empty")

      val ordering = if (desc) {
        df.col(orderByKey).desc
      } else {
        df.col(orderByKey).asc
      }
      df
        .withColumn(rowNumColName, row_number().over(Window.partitionBy(groupByKey.head, groupByKey.tail:_*).orderBy(ordering)))
        .filter(col(rowNumColName) <= limit)
    }

    /**
     * duration format a column
     *
     * @param inCol input column name
     * @param outCol output column name
     * @return data frame with duration formatted column
     */
    def durationFormat(inCol: String, outCol: String = null): DataFrame = {
      val outColName = if (outCol == null) inCol else outCol
      df.withColumn(outColName,
        when(col(inCol).isNull, lit(""))
          .otherwise(
            format_string("%02d:%02d:%02d",
              expr(s"${inCol} / 3600").cast("int"),
              expr(s"${inCol} % 3600 / 60").cast("int"),
              expr(s"${inCol} % 60").cast("int")
            )
          )
      )
    }

    /**
     * collect values in keyField and valueField as a map
     *
     * @param keyField key field
     * @param valueField value field
     * @tparam T type of the value
     * @return java.util.Map[String, String] of keyField and valueField values
     */
    def toMap[T](keyField: String, valueField: String): util.Map[String, String] = {
      df.rdd.map(row => (row.getAs[String](keyField), row.getAs[T](valueField).toString))
        .collectAsMap()
    }

  }

  val allowedContentCategories = Seq("Course", "Program", "Blended Program", "CuratedCollections", "Standalone Assessment")

  def validateContentCategories(categories: Seq[String]): Unit = {

  }

  /**
   * add a `timestamp` column with give constant value, will override current `timestamp` column if present
   * @param timestamp provided timestamp value
   * @return data frame with timestamp column
   */
  def withTimestamp(df: DataFrame, timestamp: Long): DataFrame = {
    df.withColumn("timestamp", lit(timestamp))
  }

  def getDate(): String = {
    val dateFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd").withZone(DateTimeZone.forOffsetHoursMinutes(5, 30));
    dateFormat.print(System.currentTimeMillis());
  }

  def getThisWeekDates(): (String, String, String, String) = {
    val istTimeZone = DateTimeZone.forID("Asia/Kolkata")
    val currentDate = DateTime.now(istTimeZone)
    val dateFormatter: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd").withZone(istTimeZone)
    val formatter: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZone(istTimeZone)
    val dataTillDate = currentDate.minusDays(1)
    val startOfWeek = dataTillDate.withDayOfWeek(DateTimeConstants.MONDAY).withTimeAtStartOfDay()
    val endOfWeek = startOfWeek.plusDays(6).withTime(23, 59, 59, 999)
    (formatter.print(startOfWeek), dateFormatter.print(endOfWeek), formatter.print(endOfWeek), dateFormatter.print(dataTillDate))
  }

  /* Util functions */
  def csvWrite(df: DataFrame, path: String, header: Boolean = true): Unit = {
    // spark 2.4.x has a bug where the csv does not get written with header row if the data frame is empty, this is a workaround
    if (df.isEmpty) {
      StorageUtil.simulateSparkOverwrite(path, "part-0000-XXX.csv", df.columns.mkString(",") + "\n")
    } else {
      df.write.mode(SaveMode.Overwrite).format("csv").option("header", header.toString).save(path)
    }
  }

  def csvWritePartition(df: DataFrame, path: String, partitionKey: String, header: Boolean = true): Unit = {
    df.write.mode(SaveMode.Overwrite).format("csv").option("header", header.toString)
      .partitionBy(partitionKey).save(path)
  }

  def kafkaDispatch(data: RDD[String], topic: String)(implicit sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
    if (topic == "") {
      println("ERROR: topic is blank, skipping kafka dispatch")
    } else if (conf.broker == "") {
      println("ERROR: broker list is blank, skipping kafka dispatch")
    } else {
      KafkaDispatcher.dispatch(Map("brokerList" -> conf.broker, "topic" -> topic, "compression" -> conf.compression), data)
    }
  }

  def kafkaDispatch(data: DataFrame, topic: String)(implicit sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
    kafkaDispatch(data.toJSON.rdd, topic)
  }

  def kafkaDispatchDS[T](data: Dataset[T], topic: String)(implicit sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
    kafkaDispatch(data.toJSON.rdd, topic)
  }

  def apiThrowException(method: String, url: String, body: String): String = {
    val request = method.toLowerCase() match {
      case "post" => new HttpPost(url)
      case _ => throw new Exception(s"HTTP method '${method}' not supported")
    }
    request.setHeader("Content-type", "application/json")  // set the Content-type
    request.setEntity(new StringEntity(body))  // add the JSON as a StringEntity
    val httpClient = HttpClientBuilder.create().build()  // create HttpClient
    val response = httpClient.execute(request)  // send the request
    val statusCode = response.getStatusLine.getStatusCode  // get status code
    if (statusCode < 200 || statusCode > 299) {
      throw new Exception(s"ERROR: got status code=${statusCode}, response=${EntityUtils.toString(response.getEntity)}")
    } else {
      EntityUtils.toString(response.getEntity)
    }
  }

  def api(method: String, url: String, body: String): String = {
    try {
      apiThrowException(method, url, body)
    } catch {
      case e: Throwable => {
        println(s"ERROR: ${e.toString}")
        return ""
      }
    }
  }

  def hasColumn(df: DataFrame, path: String): Boolean = Try(df(path)).isSuccess

  def dataFrameFromJSONString(jsonString: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val dataset = spark.createDataset(jsonString :: Nil)
    spark.read.option("mode", "DROPMALFORMED").option("multiline", value = true).json(dataset)
  }
  def emptySchemaDataFrame(schema: StructType)(implicit spark: SparkSession): DataFrame = {
    spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
  }

  def druidSQLAPI(query: String, host: String, resultFormat: String = "object", limit: Int = 10000): String = {
    // TODO: tech-debt, use proper spark druid connector when available, no official connector for this version of spark as of now
    val url = s"http://${host}:8888/druid/v2/sql"
    val requestBody = s"""{"resultFormat":"${resultFormat}","header":false,"context":{"sqlOuterLimit":${limit}},"query":"${query}"}"""
    api("POST", url, requestBody)
  }

  def druidDFOption(query: String, host: String, resultFormat: String = "object", limit: Int = 10000)(implicit spark: SparkSession): Option[DataFrame] = {
    var result = druidSQLAPI(query, host, resultFormat, limit)
    result = result.trim()
    // return empty data frame if result is an empty string
    if (result == "") {
      println(s"ERROR: druidSQLAPI returned empty string")
      return None
    }
    val df = dataFrameFromJSONString(result).persist(StorageLevel.MEMORY_ONLY)
    if (df.isEmpty) {
      println(s"ERROR: druidSQLAPI json parse result is empty")
      return None
    }
    // return empty data frame if there is an `error` field in the json
    if (hasColumn(df, "error")) {
      println(s"ERROR: druidSQLAPI returned error response, response=${result}")
      return None
    }
    // now that error handling is done, proceed with business as usual
    Some(df)
  }

  def cassandraTableAsDataFrame(keySpace: String, table: String)(implicit spark: SparkSession): DataFrame = {
    spark.read.format("org.apache.spark.sql.cassandra")
      .option("inferSchema", "true")
      .option("keyspace", keySpace)
      .option("table", table)
      .load()
      .persist(StorageLevel.MEMORY_ONLY)
  }

  def postgresTableAsDataFrame(postgresUrl: String, tableName: String, dbUserName: String, dbCredential: String)(implicit spark: SparkSession): DataFrame = {
    val connectionProperties = new java.util.Properties()
    connectionProperties.setProperty("user", dbUserName)
    connectionProperties.setProperty("password", dbCredential)
    connectionProperties.setProperty("driver", "org.postgresql.Driver")

    spark.read.jdbc(postgresUrl, tableName, connectionProperties)
  }

  def saveDataframeToPostgresTable(df: DataFrame, dwPostgresUrl: String, tableName: String,
                                   dbUserName: String, dbCredential: String)(implicit spark: SparkSession): Unit = {
    df.write
      .mode(SaveMode.Overwrite)
      .option("url", dwPostgresUrl)
      .option("dbtable", tableName)
      .option("user", dbUserName)
      .option("password", dbCredential)
      .option("driver", "org.postgresql.Driver")
      .format("jdbc")
      .save()
  }

  def elasticSearchDataFrame(host: String, index: String, query: String, fields: Seq[String], arrayFields: Seq[String] = Seq())(implicit spark: SparkSession): DataFrame = {
    var dfr = spark.read.format("org.elasticsearch.spark.sql")
      .option("es.read.metadata", "false")
      .option("es.nodes", host)
      .option("es.port", "9200")
      .option("es.index.auto.create", "false")
      .option("es.nodes.wan.only", "true")
      .option("es.nodes.discovery", "false")
    if (arrayFields.nonEmpty) {
      dfr = dfr.option("es.read.field.as.array.include", arrayFields.mkString(","))
    }
    var df = dfr.option("query", query).load(index)
    df = df.select(fields.map(f => col(f)):_*).persist(StorageLevel.MEMORY_ONLY) // select only the fields we need and persist
    df
  }

  def saveDataframeToPostgresTable_With_Append(df: DataFrame, dwPostgresUrl: String, tableName: String,
                                               dbUserName: String, dbCredential: String)(implicit spark: SparkSession): Unit = {

    println("inside write method")
    df.write
      .mode(SaveMode.Append)
      .option("url", dwPostgresUrl)
      .option("dbtable", tableName)
      .option("user", dbUserName)
      .option("password", dbCredential)
      .option("driver", "org.postgresql.Driver")
      .format("jdbc")
      .save()
  }

  def truncateWarehouseTable(table: String)(implicit spark: SparkSession, conf: DashboardConfig): Unit = {
    val dwPostgresUrl = s"jdbc:postgresql://${conf.dwPostgresHost}/${conf.dwPostgresSchema}"
    val postgresProperties = new java.util.Properties()
    postgresProperties.setProperty("user", conf.dwPostgresUsername)
    postgresProperties.setProperty("password", conf.dwPostgresCredential)
    postgresProperties.setProperty("driver", "org.postgresql.Driver")

    val connection = java.sql.DriverManager.getConnection(dwPostgresUrl, postgresProperties)
    try {
      val statement = connection.createStatement()
      statement.executeUpdate(s"TRUNCATE TABLE ${table}")
    } finally {
      connection.close()
    }
  }


  def mongodbTableAsDataFrame(mongoDatabase: String, collection: String)(implicit spark: SparkSession): DataFrame = {
    val schema = new StructType()
      .add("topiccount", IntegerType, true)
      .add("postcount", IntegerType, true)
      .add("sunbird-oidcId", StringType, true)
      .add("username", StringType, true)
    val df = spark.read.schema(schema).format("com.mongodb.spark.sql.DefaultSource").option("database", mongoDatabase).option("collection", collection).load()
    val filterDf = df.select("sunbird-oidcId").where(col("username").isNotNull or col("topiccount") > 0 and (col("postcount") > 0))
    val renamedDF = filterDf.withColumnRenamed("sunbird-oidcId", "userid")
    renamedDF
  }

  def writeToCassandra(data: DataFrame, keyspace: String, table: String)(implicit spark: SparkSession): Unit = {
    data.write.format("org.apache.spark.sql.cassandra")
      .mode("append")
      .options(Map("table" -> table, "keyspace" -> keyspace))
      .save()
  }

  /* Config functions */
  def getConfig[T](config: Map[String, AnyRef], key: String, default: T = null): T = {
    val path = key.split('.')
    var obj = config
    path.slice(0, path.length - 1).foreach(f => { obj = obj.getOrElse(f, Map()).asInstanceOf[Map[String, AnyRef]] })
    obj.getOrElse(path.last, default).asInstanceOf[T]
  }
  def getConfigModelParam(config: Map[String, AnyRef], key: String, default: String = ""): String = getConfig[String](config, key, default)
  def getConfigSideBroker(config: Map[String, AnyRef]): String = getConfig[String](config, "sideOutput.brokerList", "")
  def getConfigSideBrokerCompression(config: Map[String, AnyRef]): String = getConfig[String](config, "sideOutput.compression", "snappy")
  def getConfigSideTopic(config: Map[String, AnyRef], key: String): String = getConfig[String](config, s"sideOutput.topics.${key}", "")

  def parseConfig(config: Map[String, AnyRef]): DashboardConfig = {
    DashboardConfig(
      debug = getConfigModelParam(config, "debug"),
      validation = getConfigModelParam(config, "validation"),
      cutoffTime = getConfigModelParam(config, "cutoffTime", "60.0").toFloat,
      cassandraUserKeyspace = getConfigModelParam(config, "cassandraUserKeyspace"),
      cassandraLearnerStatsTable = getConfigModelParam(config, "cassandraLearnerStatsTable")
    )
  }

  /* Config functions end */

  def checkAvailableColumns(df: DataFrame, expectedColumnsInput: List[String]) : DataFrame = {
    expectedColumnsInput.foldLeft(df) {
      (df, column) => {
        if(!df.columns.contains(column)) {
          df.withColumn(column, lit(null).cast(StringType))
        } else df
      }
    }
  }

  /**
   * return parsed int or zero if parsing fails
   * @param s string to parse
   * @return int or zero
   */
  def intOrZero(s: String): Int = {
    try {
      s.toInt
    } catch {
      case e: Exception => 0
    }
  }

  /***
   * Validate if return value from one code block is equal to the return value from other block. Uses blocks so that the
   * spark code in blocks is only executed if validation=true
   *
   * @param msg info on what is being validated
   * @tparam T return type from the blocks
   * @throws AssertionError if values from blocks do not match
   */
  @throws[AssertionError]
  def validate[T](block1: => T, block2: => T, msg: String = ""): Unit = {
    if (validation) {
      val r1 = block1
      val r2 = block2
      if (r1.equals(r2)) {
        println(s"VALIDATION PASSED: ${msg}")
        println(s"  - value = ${r1}")
      } else {
        println(s"VALIDATION FAILED: ${msg}")
        println(s"  - value ${r1} does not equal value ${r2}")
        // throw new AssertionError("Validation Failed")
      }
    }
  }

  def show(df: DataFrame, msg: String = ""): Unit = {
    println("____________________________________________")
    println("SHOWING: " + msg)
    if (debug) {
      df.show()
      println("Count: " + df.count())
    }
    df.printSchema()
  }

  def showDS[T](ds: Dataset[T], msg: String = ""): Unit = {
    println("____________________________________________")
    println("SHOWING: " + msg)
    if (debug) {
      ds.show()
      println("Count: " + ds.count())
    }
    ds.printSchema()
  }
}

object Redis extends Serializable {

  var redisConnect: Jedis = null
  var redisHost: String = ""
  var redisPort: Int = 0
  var redisTimeout: Int = 30000

  // open/close redis connection
  def closeRedisConnect(): Unit = {
    if (redisConnect != null) {
      redisConnect.close()
      redisConnect = null
    }
  }
  def getOrCreateRedisConnect(host: String, port: Int): Jedis = {
    if (redisConnect == null) {
      redisConnect = createRedisConnect(host, port)
    } else if (redisHost != host || redisPort != port) {
      try {
        closeRedisConnect()
      } catch {
        case e: Exception => {}
      }
      redisConnect = createRedisConnect(host, port)
    }
    redisConnect
  }
  def getOrCreateRedisConnect(conf: DashboardConfig): Jedis = getOrCreateRedisConnect(conf.redisHost, conf.redisPort)
  def createRedisConnect(host: String, port: Int): Jedis = {
    redisHost = host
    redisPort = port
    if (host == "") return null
    new Jedis(host, port, redisTimeout)
  }
  def createRedisConnect(conf: DashboardConfig): Jedis = createRedisConnect(conf.redisHost, conf.redisPort)

  // get key value
  def get(key: String)(implicit conf: DashboardConfig): String = {
    get(conf.redisHost, conf.redisPort, conf.redisDB, key)
  }
  def get(host: String, port: Int, db: Int, key: String): String = {
    try {
      getWithoutRetry(host, port, db, key)
    } catch {
      case e: JedisException =>
        redisConnect = createRedisConnect(host, port)
        getWithoutRetry(host, port, db, key)
    }
  }
  def getWithoutRetry(host: String, port: Int, db: Int, key: String): String = {
    if (key == null || key.isEmpty) {
      println(s"WARNING: key is empty")
      return "" // or any other default value
    }
    val jedis = getOrCreateRedisConnect(host, port)
    if (jedis == null) {
      println(s"WARNING: jedis=null means host is not set, skipping fetching the redis key=${key}")
      return "" // or any other default value
    }
    if (jedis.getDB != db) jedis.select(db)

    // Check if the key exists in Redis
    if (!jedis.exists(key)) {
      println(s"WARNING: Key=${key} does not exist in Redis")
      return "" // or any other default value
    }

    jedis.get(key)
  }

  // set key value
  def update(key: String, data: String)(implicit conf: DashboardConfig): Unit = {
    update(conf.redisHost, conf.redisPort, conf.redisDB, key, data)
  }
  def update(db: Int, key: String, data: String)(implicit conf: DashboardConfig): Unit = {
    update(conf.redisHost, conf.redisPort, db, key, data)
  }
  def update(host: String, port: Int, db: Int, key: String, data: String): Unit = {
    try {
      updateWithoutRetry(host, port, db, key, data)
    } catch {
      case e: JedisException =>
        redisConnect = createRedisConnect(host, port)
        updateWithoutRetry(host, port, db, key, data)
    }
  }
  def updateWithoutRetry(host: String, port: Int, db: Int, key: String, data: String): Unit = {
    var cleanedData = ""
    if (data == null || data.isEmpty) {
      println(s"WARNING: data is empty, setting data='' for redis key=${key}")
      cleanedData = ""
    } else {
      cleanedData = data
    }
    val jedis = getOrCreateRedisConnect(host, port)
    if (jedis == null) {
      println(s"WARNING: jedis=null means host is not set, skipping saving to redis key=${key}")
      return
    }
    if (jedis.getDB != db) jedis.select(db)
    jedis.set(key, cleanedData)
  }

  // get map field value
  def getMapField(key: String, field: String)(implicit conf: DashboardConfig): String = {
    getMapField(conf.redisHost, conf.redisPort, conf.redisDB, key, field)
  }
  def getMapField(host: String, port: Int, db: Int, key: String, field: String): String = {
    try {
      getMapFieldWithoutRetry(host, port, db, key, field)
    } catch {
      case e: JedisException =>
        redisConnect = createRedisConnect(host, port)
        getMapFieldWithoutRetry(host, port, db, key, field)
    }
  }

  def getMapFieldWithoutRetry(host: String, port: Int, db: Int, key: String, field: String): String = {
    if (key == null || key.isEmpty) {
      println(s"WARNING: key is empty")
      return ""
    }
    val jedis = getOrCreateRedisConnect(host, port)
    if (jedis == null) {
      println(s"WARNING: jedis=null means host is not set, skipping fetching the redis key=${key}")
      return ""
    }
    if (jedis.getDB != db) jedis.select(db)

    // Check if the key exists in Redis
    if (!jedis.exists(key)) {
      println(s"WARNING: Key=${key} does not exist in Redis")
      return ""
    }

    // Fetch all fields and values from the Redis hash
    jedis.hget(key, field)
  }

  // set map field value
  def updateMapField(key: String, field: String, data: String)(implicit conf: DashboardConfig): Unit = {
    updateMapField(conf.redisHost, conf.redisPort, conf.redisDB, key, field, data)
  }
  def updateMapField(host: String, port: Int, db: Int, key: String, field: String, data: String): Unit = {
    try {
      updateMapFieldWithoutRetry(host, port, db, key, field, data)
    } catch {
      case e: JedisException =>
        redisConnect = createRedisConnect(host, port)
        updateMapFieldWithoutRetry(host, port, db, key, field, data)
    }
  }
  def updateMapFieldWithoutRetry(host: String, port: Int, db: Int, key: String, field: String, data: String): Unit = {
    var cleanedData = ""
    if (data == null || data.isEmpty) {
      println(s"WARNING: data is empty, setting data='' for redis key=${key}")
      cleanedData = ""
    } else {
      cleanedData = data
    }
    val jedis = getOrCreateRedisConnect(host, port)
    if (jedis == null) {
      println(s"WARNING: jedis=null means host is not set, skipping saving to redis key=${key}")
      return
    }
    if (jedis.getDB != db) jedis.select(db)
    jedis.hset(key, field, cleanedData)
  }

  // get map
  def getMapAsDataFrame(key: String, schema: StructType)(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    getMapAsDataFrame(conf.redisHost, conf.redisPort, conf.redisDB, key, schema)
  }

  def getMapAsDataFrame(host: String, port: Int, db: Int, key: String, schema: StructType)(implicit spark: SparkSession): DataFrame = {
    val data = getMap(host, port, db, key)
    if (data.isEmpty) {
      DashboardUtil.emptySchemaDataFrame(schema)
    } else {
      spark.createDataFrame(data.map(r => Row(r._1, r._2)).toList, schema)
    }
  }

  def getMap(key: String)(implicit conf: DashboardConfig): util.Map[String, String] = {
    getMap(conf.redisHost, conf.redisPort, conf.redisDB, key)
  }

  def getMap(host: String, port: Int, db: Int, key: String): util.Map[String, String] = {
    try {
      getMapWithoutRetry(host, port, db, key)
    } catch {
      case e: JedisException =>
        redisConnect = createRedisConnect(host, port)
        getMapWithoutRetry(host, port, db, key)
    }
  }

  def getMapWithoutRetry(host: String, port: Int, db: Int, key: String): util.Map[String, String] = {
    if (key == null || key.isEmpty) {
      println(s"WARNING: key is empty")
      return new util.HashMap[String, String]()
    }
    val jedis = getOrCreateRedisConnect(host, port)
    if (jedis == null) {
      println(s"WARNING: jedis=null means host is not set, skipping fetching the redis key=${key}")
      return new util.HashMap[String, String]()
    }
    if (jedis.getDB != db) jedis.select(db)

    // Check if the key exists in Redis
    if (!jedis.exists(key)) {
      println(s"WARNING: Key=${key} does not exist in Redis")
      return new util.HashMap[String, String]()
    }

    // Fetch all fields and values from the Redis hash
    jedis.hgetAll(key)
  }

  // set map, replace all keys
  def dispatch(key: String, data: util.Map[String, String], replace: Boolean = true)(implicit conf: DashboardConfig): Unit = {
    dispatch(conf.redisHost, conf.redisPort, conf.redisDB, key, data, replace)
  }
  def dispatch(db: Int, key: String, data: util.Map[String, String], replace: Boolean)(implicit conf: DashboardConfig): Unit = {
    dispatch(conf.redisHost, conf.redisPort, db, key, data, replace)
  }
  def dispatch(host: String, port: Int, db: Int, key: String, data: util.Map[String, String], replace: Boolean): Unit = {
    try {
      dispatchWithoutRetry(host, port, db, key, data, replace)
    } catch {
      case e: JedisException =>
        redisConnect = createRedisConnect(host, port)
        dispatchWithoutRetry(host, port, db, key, data, replace)
    }
  }
  def dispatchWithoutRetry(host: String, port: Int, db: Int, key: String, data: util.Map[String, String], replace: Boolean): Unit = {
    if (data == null || data.isEmpty) {
      println(s"WARNING: map is empty, skipping saving to redis key=${key}")
      return
    }
    val jedis = getOrCreateRedisConnect(host, port)
    if (jedis == null) {
      println(s"WARNING: jedis=null means host is not set, skipping saving to redis key=${key}")
      return
    }
    if (jedis.getDB != db) jedis.select(db)
    if (replace) {
      replaceMap(jedis, key, data)
    } else {
      jedis.hset(key, data)
    }
  }

  def getHashKeys(jedis: Jedis, key: String): Seq[String] = {
    val keys = ListBuffer[String]()
    val scanParams = new ScanParams().count(100)
    var cur = ScanParams.SCAN_POINTER_START
    do {
      val scanResult = jedis.hscan(key, cur, scanParams)
      scanResult.getResult.foreach(res => {
        keys += res.getKey
      })
      cur = scanResult.getCursor
    } while (!cur.equals(ScanParams.SCAN_POINTER_START))
    keys.toList
  }

  def replaceMap(jedis: Jedis, key: String, data: util.Map[String, String]): Unit = {
    // this deletes the keys that do not exist anymore manually
    val existingKeys = getHashKeys(jedis, key)
    val toDelete = existingKeys.toSet.diff(data.keySet())
    if (toDelete.nonEmpty) jedis.hdel(key, toDelete.toArray:_*)
    // this will update redis hash map keys and create new ones, but will not delete ones that have been deleted
    jedis.hset(key, data)
  }

  /**
   * Convert data frame into a map, and save to redis
   *
   * @param redisKey key to save df data to
   * @param df data frame
   * @param keyField column name that forms the key (must be a string)
   * @param valueField column name that forms the value
   * @tparam T type of the value column
   */
  def dispatchDataFrame[T](redisKey: String, df: DataFrame, keyField: String, valueField: String, replace: Boolean = true)(implicit conf: DashboardConfig): Unit = {
    import DashboardUtil._
    dispatch(redisKey, df.toMap[T](keyField, valueField), replace)
  }

}