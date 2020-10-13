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

// import org.apache.spark.sql.cassandra._
// import com.datastax.spark.connector._
// import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
// import com.datastax.spark.connector.streaming._

object KafkaSpark {
  def main(args: Array[String]) {

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
    val pairs = messages.map(w => ((w._2).split(","))).map(m => (m(0), m(1).toDouble))

    // measure the average value for each key in a stateful manner
    def mappingFunc(key: String, value: Option[Double], state: State[(Double, Int)]): (String, Double) = {
      // Create a tuple to store the sum and counter
      val (stateSum, stateCounter) = state.getOption.getOrElse(0.0d, 0)
      // Then store the total from all states
      val totalSum = value.getOrElse(0.0d) + stateSum
      // Then store the total counter from all states
      val totalCounter = stateCounter + 1
      // update the tuple
      state.update((totalSum, totalCounter))
      // return the average value for each key
      return (key, totalSum/totalCounter)

    }
    //use mapWithState to calculated the average value of each key in a statful manner
    val stateDstream = pairs.mapWithState(StateSpec.function(mappingFunc _))

    ssc.start()
    ssc.awaitTermination()
  }
}
