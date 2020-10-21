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
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._

object KafkaSpark {
  def main(args: Array[String]) {

    // connect to Cassandra and make a keyspace and table as explained in the document
    val cluster = Cluster.builder().addContactPoint("127.0.0.1").build() 
    
    println("About to connect to cluster")
    val session = cluster.connect()
    println("Connected to cluster")


    println("About to execute creation of Keyspace")
    //To execute a command on a connected Cassandra instance, you can use the execute command as below
    session.execute("CREATE KEYSPACE IF NOT EXISTS avg_space WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
    println("Created Keyspace")

    println("About to execute creation of Table")
    session.execute("CREATE TABLE IF NOT EXISTS avg_space.avg (key text PRIMARY KEY, weather text);")
    println("Created Table")

    //Spark Stream context with 2 working threads and batch interval of 1 sec. 
    val conf = new SparkConf().setMaster("local[2]").setAppName("Spark Streaming  - AVG")
    val topics = Set("avg")
    val ssc = new StreamingContext(conf, Seconds(1))
    ssc.checkpoint("file:///tmp/spark/checkpoint")
    // make a connection to Kafka and read (key, value) pairs from it
    //val topics = ? 
    val kafkaConf = Map(
        "metadata.broker.list" -> "localhost:9092", 
        "zookeeper.connect" -> "localhost:2181", 
        "group.id" -> "kafka-spark-streaming", 
        "zookeeper.connection.timeout.ms" -> "1000")
        
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConf, topics)
    val pairs = messages.map(w => ((w._2).split(","))).map(m => (m(0), m(1)))
    //println(messages)

    // measure the average value for each key in a stateful manner
    // def mappingFunc(key: String, value: Option[Double], state: State[(Double, Int)]): (String, Double) = {
    //   // Create a tuple to store the sum and counter
    //   val (stateSum, stateCounter) = state.getOption.getOrElse(0.0d, 0)
    //   // Then store the total from all states
    //   val totalSum = value.getOrElse(0.0d) + stateSum
    //   // Then store the total counter from all states
    //   val totalCounter = stateCounter + 1
    //   // update the tuple
    //   state.update((totalSum, totalCounter))
    //   // return the average value for each key
    //   return (key, totalSum/totalCounter)

    // }
    // //use mapWithState to calculated the average value of each key in a statful manner
    // val stateDstream = pairs.mapWithState(StateSpec.function(mappingFunc _))

    pairs.saveToCassandra("avg_space", "avg", SomeColumns("key", "weather"))

    ssc.start()
    ssc.awaitTermination()
    session.close()
  }
}
