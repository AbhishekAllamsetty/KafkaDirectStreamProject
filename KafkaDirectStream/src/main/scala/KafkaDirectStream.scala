import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.InputDStream

import org.apache.log4j.Logger
import org.json.JSONObject
import java.io.File
import com.typesafe.config.{Config, ConfigFactory}

object KafkaDirectStream {

  // importing log4j
  // making it globally accessible for current object
  private val logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KafkaDirectStream <brokers> <consumerGroup> <topic> <table.properties>")
      System.exit(1)
    }
    val Array(zkQuorum, consumerGroup, topic, table_conf_path) = args

    // reading configs
    val config = ConfigFactory.parseFile(new File(table_conf_path))

    // creating sc, ssc and spark_session
    val spark = SparkSession.builder.appName("KafkaDirectStream").enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(5))

    // setting spark log level to ERROR
    sc.setLogLevel("INFO")

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
      PreferConsistent, //it consistently distributes paritions accross all the executors
      Subscribe[String, String](Array(topic), kafkaParams)
    )

    stream.foreachRDD(
      rddRaw => {
        //val offsetranges = rddRaw.asInstanceOf[HasOffsetRanges].offsetRanges
        val json_string_rdd = rddRaw.map(_.value.toString)

        // here we are trying to filter any in correct json strings else it will lead to corrupted record
        // in the spark df
        val filtered_json_string = json_string_rdd.filter(json_validator)

        // reading the rdd as json
        val df = spark.read.json(filtered_json_string)

        // selection on corrupted record will raise an exception and kill job
        // so not processing anything if data is null
        // processing only if df is not empty
        if (!df.head(1).isEmpty){
          var data_df = df.select(explode(col("values"))).select("col.*")

          //converting all the column types to string
          import org.apache.spark.sql.types.StringType
          data_df = data_df.select(data_df.columns.map(c => col(c).cast(StringType)) : _*)

          val table_name = df.select("table_name").collect()(0)(0).toString
          val rows_count = data_df.count().toInt

          if (rows_count > 0){
            val table_path = get_table_path(table_name, config)

            // adding current timestamp to the table column
            import java.time.LocalDateTime
            data_df = data_df.withColumn("insert_ts", lit(current_timestamp()))

            // writing the data to location
            logger.info("Writing data to  ::  " + table_name)
            data_df.write.mode("append").format("parquet").save(table_path)
          }
        }
        else{
          logger.info("No message found!!")
        }
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
        logger.error("Not a valid json message  ::  " + e)
        logger.error("Invalid Json Message  ::  \n\t%s" + json_string)
        false
    }
    bool
  }

  def get_table_path(table_name: String, config: Config): String = {
    /***
      * In this function we are trying to get the path of the table where it needs to be written
      * If the path is not found then we return write an error and exit the job
      */
    var table_path = ""
    try{
      val config_prop = table_name + "_path"
      table_path = config.getString(config_prop)
    }
    catch {
      case e: Exception =>
        logger.error("Table location property not found!!  ::  "+e)
        System.exit(1)
    }
    table_path
  }
}

