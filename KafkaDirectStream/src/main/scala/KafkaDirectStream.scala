import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.log4j.Logger

import org.json.JSONObject

object KafkaDirectStream {

  // importing log4j
  // making it globally accessible for current object
  private val logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: KafkaDirectStream <brokers> <consumerGroup> <topic>")
      System.exit(1)
    }
    // creating sc, ssc and spark_session
    val spark = SparkSession.builder.appName("KafkaDirectStream").enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(5))

    // setting spark log level to ERROR
    sc.setLogLevel("ERROR")

    val Array(zkQuorum, consumerGroup, topic) = args
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> zkQuorum,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> consumerGroup,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    logger.info("Creating Kafka Direct Stream")
    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](Array(topic), kafkaParams)
    )

    stream.foreachRDD(
      rddRaw => {
        //val offsetranges = rddRaw.asInstanceOf[HasOffsetRanges].offsetRanges
        val json_string_rdd = rddRaw.map(_.value.toString)

        // here we are trying to filter any in correct json strings else it will lead to corrupted record
        // in the spark df
        val filtered_json_string = json_string_rdd.filter(json_validator)
        val df = spark.read.json(filtered_json_string)
        print(df.printSchema())
      }
    )
    ssc.start()
    ssc.awaitTermination()
  }

  def json_validator(json_string: String): Boolean = {
    /***
      *  This method tries to load the JSON[String] => JSON[Object]
      *  If it loads we return true and else false
      */
    val bool = try{
      new JSONObject(json_string)
      true
    }
    catch {
      case e: Exception =>
        false
    }
    bool
  }
}

