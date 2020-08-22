package ca.rohith.bigdata.applications

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Applications extends App with Log {

  //Initiates Spark Session and creates Spark Context
  val spark = SparkSession.builder()
    .appName("Spark Streaming").master("local[*]").getOrCreate()
  val ssc = new StreamingContext(spark.sparkContext, Seconds(10))

  //creating Dataframe using Spark SQL from Source
  val tripDf: DataFrame = spark.read.option("header", "true")
    .csv("/user/winter2020/rohith/project5/trips/")
  val calendarDateDf: DataFrame = spark.read.option("header", "true")
    .csv("/user/winter2020/rohith/project5/calendar_dates")
  val frequenciesDf: DataFrame = spark.read.option("header", "true")
    .csv("/user/winter2020/rohith/project5/frequencies")

  //creating temporary view for DataFrames to join
  tripDf.createOrReplaceTempView("trip")
  calendarDateDf.createOrReplaceTempView("calendarDate")
  frequenciesDf.createOrReplaceTempView("frequencies")

  //joining dimensions trip,calendarDate and frequencies using SQL query to make single enrichedTrip DF
  val enrichedTrip: DataFrame = spark.sql(
    """SELECT t.trip_id,t.trip_headsign,t.wheelchair_accessible,
      |cd.date,cd.exception_type,f.start_time,
      |f.end_time FROM trip t join calendarDate cd ON
      |t.service_id=cd.service_id JOIN frequencies f ON
      |f.trip_id = t.trip_id""".stripMargin)

  //kafka consumer configuration
  val kafkaConf = Map[String, String](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
    ConsumerConfig.GROUP_ID_CONFIG -> "application-group",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"
  )
  //creating the stream and subscribing to the topic
  val topic = "stop-time-2"
  val inputStream: InputDStream[ConsumerRecord[String, String]] =
    KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](List(topic), kafkaConf)
    )

  inputStream.map(_.value()).foreachRDD(rdd => enrichStopTimes(rdd))

  ssc.start()
  ssc.awaitTermination() //this keeps the Spark Stream up and running

  def enrichStopTimes(rdd: RDD[String]): Unit = {
    import spark.implicits._
    val stopTimes: RDD[StopTimes] = rdd.map(csv => StopTimes(csv))
    val stopTimesDf: DataFrame = stopTimes.toDF
    val output = enrichedTrip.join(stopTimesDf, "trip_id")
    output.coalesce(1).write.mode(SaveMode.Append)
      .csv("/user/winter2020/rohith/project5/enriched_stop_time")
  }
}
