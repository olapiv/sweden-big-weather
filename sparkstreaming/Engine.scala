package sparkstreaming

import java.util.HashMap

import spray.json._
import DefaultJsonProtocol._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import java.util.{Date, Properties}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.util.Random
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Cluster, Host, Metadata, Session}
import com.datastax.spark.connector.streaming._
import org.apache.spark.streaming.dstream.{InputDStream, DStream}

case class Coord(lon: Double, lat: Double)
case class CityTempDataPoint(temperatureKelvin: Double, coordinates: Coord, city: String)
case class GridTempDataPoint(temperatureKelvin: Double, coordinates: Coord)

object Engine {

  def main(args: Array[String]) {

    val BROKER_URL = sys.env("BROKER_URL") // "kafka:9092"
    val ZOOKEEPER_URL = sys.env("ZOOKEEPER_URL") // "zookeeper:2181"
    val CASSANDRA_URL = sys.env("CASSANDRA_URL") // "cassandra"
    val KAFKA_PRODUCING_TOPIC = "grid-temperatures"
    val KAFKA_CONSUMING_TOPIC = "city-temperatures"

    def initialiseProducer(): KafkaProducer[String, String] = {
        val props = new Properties()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_URL)
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "GridTemperatureProducer")
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        new KafkaProducer[String, String](props)
    }

    def initialiseCassandra(): Session  = {
        val cluster = Cluster.builder().addContactPoint(CASSANDRA_URL).build() 
        val session = cluster.connect()
        session.execute("CREATE KEYSPACE IF NOT EXISTS weather_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
        session.execute("CREATE TABLE IF NOT EXISTS weather_keyspace.city_temps (coords_json text PRIMARY KEY, temp_kelvin double);")
        session
    }

    def initialiseSparkSession(): SparkSession = {
        // 2 working threads
        val sparkSess = SparkSession
              .builder()
              .appName("Calculating Temperatures")
              .config("spark.cassandra.connection.host", CASSANDRA_URL)
              .config("spark.cassandra.connection.port", "9042")
              .master("local[2]")
              .getOrCreate();
        sparkSess
    }

    def initialiseStreamingContext(sparkSess: SparkSession): StreamingContext = {
        // Batch interval of 1 sec
        val ssc = new StreamingContext(sparkSess.sparkContext, Seconds(1))
        ssc.checkpoint("file:///tmp/spark/checkpoint")
        ssc
    }

    def initialiseKafkaStream(ssc: StreamingContext): InputDStream[(String, String)] = {
        val kafkaConf = Map(
            "metadata.broker.list" -> BROKER_URL,
            "zookeeper.connect" -> ZOOKEEPER_URL,
            "group.id" -> "kafka-spark-streaming",
            "zookeeper.connection.timeout.ms" -> "1000")
        val topics = Set(KAFKA_CONSUMING_TOPIC)
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConf, topics)
    }

    def produceNewMessages(data: DStream[(Coord, Double)]): Unit = {
        data.foreachRDD(rdd => {
            rdd.foreachPartition { partitionOfRecords =>
                val producer = initialiseProducer()
                partitionOfRecords.foreach(record => {
                    implicit val coordFormat = jsonFormat2(Coord)
                    implicit val gridFormat = jsonFormat2(GridTempDataPoint)
                    val gridData = GridTempDataPoint(record._2, record._1)
                    val message = new ProducerRecord[String, String](KAFKA_PRODUCING_TOPIC, null, gridData.toJson.compactPrint)
                    producer.send(message)
                })
                producer.close()
            }
        })
    }

    val cassSession = initialiseCassandra()
    val sparkSess = initialiseSparkSession()
    val ssc = initialiseStreamingContext(sparkSess)

    val messages = initialiseKafkaStream(ssc)
    val parsedDataPairs = messages.map(w => {
        implicit val coordFormat = jsonFormat2(Coord)
        implicit val tempFormat = jsonFormat3(CityTempDataPoint)
        (w._2).parseJson.convertTo[CityTempDataPoint]
    }).map(m => {
        (m.coordinates, m.temperatureKelvin)
    })
    produceNewMessages(parsedDataPairs) // Just for fun

    // Save to single data point to Cassandra
    val cassandraDataPairs = parsedDataPairs.map(m => {
        implicit val coordFormat = jsonFormat2(Coord)
        (m._1.toJson.compactPrint, m._2)
    })
    cassandraDataPairs.saveToCassandra("weather_keyspace", "city_temps", SomeColumns("coords_json", "temp_kelvin"))

    // Retrieve multiple data points from Cassandra
    // TODO: Filter data
    val df = sparkSess.read.format("org.apache.spark.sql.cassandra")
        .options(scala.collection.immutable.Map( "table" -> "city_temps", "keyspace" -> "weather_keyspace"))
        .load()
    df.show()

    // TODO: Do calculations

    // TODO: Forward to Kafka
    // produceNewMessages(calculatedDataPairs)

    ssc.start()
    ssc.awaitTermination()
    cassSession.close()
  }
}
