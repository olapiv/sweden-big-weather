package sparkstreaming

import java.util.HashMap

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
import org.apache.spark.streaming.dstream.InputDStream

object Engine {
  def main(args: Array[String]) {

    val BROKER_URL = sys.env("BROKER_URL") // "kafka:9092"
    val ZOOKEEPER_URL = sys.env("BROKER_URL") // "zookeeper:2181"
    val CASSANDRA_URL = "127.0.0.1"
    val KAFKA_PRODUCING_TOPIC = "grid-temperatures"
    val KAFKA_CONSUMING_TOPIC = "city-temperatures"

    def initialiseProducer(): KafkaProducer[String, String] = {
        val props = new Properties()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_URL)
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "ScalaProducerExample")
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        new KafkaProducer[String, String](props)
    }

    def initialiseCassandra(): Cluster.Session  = {
        val cluster = Cluster.builder().addContactPoint(CASSANDRA_URL).build() 
        val session = cluster.connect()
        session.execute("CREATE KEYSPACE IF NOT EXISTS avg_space WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
        session.execute("CREATE TABLE IF NOT EXISTS avg_space.avg (key text PRIMARY KEY, weather text);")
        session
    }

    def initialiseStreamingContext(): StreamingContext = {
      // Spark Stream context with 2 working threads and batch interval of 1 sec.
      val conf = new SparkConf().set("spark.cassandra.connection.host", CASSANDRA_URL).setMaster("local[2]").setAppName("Spark Streaming - Temperatures")
      val topics = Set("city-temperatures")
      val ssc = new StreamingContext(conf, Seconds(1))
      ssc.checkpoint("file:///tmp/spark/checkpoint")
      ssc
    }

    def initialiseKafkaStream(): InputDStream[String, String, StringDecoder, StringDecoder] = {
      val kafkaConf = Map(
        "metadata.broker.list" -> BROKER_URL,
        "zookeeper.connect" -> ZOOKEEPER_URL,
        "group.id" -> "kafka-spark-streaming",
        "zookeeper.connection.timeout.ms" -> "1000")

      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConf, Set(KAFKA_CONSUMING_TOPIC))
    }

    def produceMessages(data: InputDStream[String, String, StringDecoder, StringDecoder]): Unit = {
      data.foreachRDD(rdd => {
        rdd.foreachPartition { partitionOfRecords =>
          val producer = initialiseProducer()
          partitionOfRecords.foreach(record => {
            val message = new ProducerRecord[String, String](KAFKA_PRODUCING_TOPIC, record.asInstanceOf[String])
            producer.send(message)
          })
          producer.close()
          if (connection != null) connection.close()
          if (channel != null) channel.close()
        }
      })
    }

    val session = initialiseCassandra()
    val ssc = initialiseStreamingContext()
    val messages = initialiseKafkaStream()

    println(messages)

    val pairs = messages.map(w => ((w._2).split(","))).map(m => (m(0), m(1)))

    pairs.saveToCassandra("avg_space", "avg", SomeColumns("key", "weather"))

    // TODO: Do calculations

    produceMessages(pairs)

    ssc.start()
    ssc.awaitTermination()
    session.close()
  }
}
