package sparkstreaming

import java.util.HashMap

import spray.json._
import DefaultJsonProtocol._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import kafka.serializer.{DefaultDecoder, StringDecoder}
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
        session.execute("CREATE KEYSPACE IF NOT EXISTS avg_space WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
        session.execute("CREATE TABLE IF NOT EXISTS avg_space.avg (key text PRIMARY KEY, weather text);")
        session
    }

    def initialiseStreamingContext(): StreamingContext = {
      // Spark Stream context with 2 working threads and batch interval of 1 sec.
      val conf = new SparkConf().set("spark.cassandra.connection.host", CASSANDRA_URL).setMaster("local[2]").setAppName("Spark Streaming - Temperatures")
      val ssc = new StreamingContext(conf, Seconds(1))
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
                    // val message = new ProducerRecord[String, String](KAFKA_PRODUCING_TOPIC, record.asInstanceOf[String])
                    val message = new ProducerRecord[String, String](KAFKA_PRODUCING_TOPIC, null, gridData.toJson.compactPrint)
                    producer.send(message)
                })
                producer.close()
            }
        })
    }

    val session = initialiseCassandra()
    val ssc = initialiseStreamingContext()
    val messages = initialiseKafkaStream(ssc)


    // Parse messages
    println("MESSAGES!!!! " + messages)

    val parsedDataPairs = messages.map(w => {
        implicit val coordFormat = jsonFormat2(Coord)
        implicit val tempFormat = jsonFormat3(CityTempDataPoint)
        (w._2).parseJson.convertTo[CityTempDataPoint]
    }).map(m => {
        // implicit val coordFormat = jsonFormat2(Coord)
        (m.coordinates, m.temperatureKelvin)
        // (m.coordinates.toJson.compactPrint, m.temperatureKelvin)
    })
    println("PAIRS!!!! " + parsedDataPairs)

    // parsedDataPairs.saveToCassandra("avg_space", "avg", SomeColumns("key", "weather"))

    // TODO: Retrieve from Cassandra
    // TODO: Do calculations

    produceNewMessages(parsedDataPairs)

    ssc.start()
    ssc.awaitTermination()
    session.close()
  }
}
