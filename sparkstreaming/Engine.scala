package sparkstreaming

import java.util.Properties

import com.datastax.driver.core.{Cluster, Session}
import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.streaming._
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.sql.{SparkSession, DataFrame, Dataset}
import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka._
import spray.json.DefaultJsonProtocol._
import spray.json._
import org.apache.spark.sql.functions._

case class Coord(lon: Double, lat: Double)
case class CityTempDataPoint(temperatureKelvin: Double, coordinates: Coord, city: String)
case class GridTempDataPoint(temperatureKelvin: Double, coordinates: Coord)

case class CassDataPoint(lat: Double, lon: Double, temp_kelvin: Double)

object Engine {

  def main(args: Array[String]) {

    val BROKER_URL = sys.env("BROKER_URL") // "kafka:9092"
    val ZOOKEEPER_URL = sys.env("ZOOKEEPER_URL") // "zookeeper:2181"
    val CASSANDRA_HOST = sys.env("CASSANDRA_HOST") // "cassandra"
    val KAFKA_PRODUCING_TOPIC = "grid-temperatures"
    val KAFKA_CONSUMING_TOPIC = "city-temperatures"

    //  Sweden coordinates:
    val MIN_LON = 10.5;
    val MAX_LON = 24.9;
    val MIN_LAT = 55.3;
    val MAX_LAT = 69.1;

    def initialiseProducer(): KafkaProducer[String, String] = {
        val props = new Properties()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_URL)
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "GridTemperatureProducer")
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        new KafkaProducer[String, String](props)
    }

    def initialiseCassandra(): Session  = {
        val cluster = Cluster.builder().addContactPoint(CASSANDRA_HOST).build()
        val session = cluster.connect()
        session.execute("CREATE KEYSPACE IF NOT EXISTS weather_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
        // session.execute("DROP TABLE weather_keyspace.city_temps;")
        session.execute("CREATE TABLE IF NOT EXISTS weather_keyspace.city_temps (lat double, lon double, temp_kelvin double, PRIMARY KEY ((lat, lon)));")
        session
    }

    def initialiseSparkSession(): SparkSession = {
        // 2 working threads
        val sparkSess = SparkSession
              .builder()
              .appName("Calculating Temperatures")
              .config("spark.cassandra.connection.host", CASSANDRA_HOST)
              .config("spark.cassandra.connection.port", "9042")
              .master("local[2]")
              .getOrCreate();
        sparkSess.sparkContext.setLogLevel("WARN")
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

    // def produceNewMessages(data: DStream[(Coord, Double)]): Unit = {
    //     data.foreachRDD(rdd => {
    //         rdd.foreachPartition { partitionOfRecords =>
    //             val producer = initialiseProducer()
    //             partitionOfRecords.foreach(record => {
    //                 implicit val coordFormat = jsonFormat2(Coord)
    //                 implicit val gridFormat = jsonFormat2(GridTempDataPoint)
    //                 val gridData = GridTempDataPoint(record._2, record._1)
    //                 val message = new ProducerRecord[String, String](KAFKA_PRODUCING_TOPIC, null, gridData.toJson.compactPrint)
    //                 producer.send(message)
    //             })
    //             producer.close()
    //         }
    //     })
    // }

    def getAllCitiesInBlock(sparkSess: SparkSession, minLat: Double, maxLat: Double, minLon: Double, maxLon: Double): Dataset[CassDataPoint] = {
        import sparkSess.implicits._

        sparkSess
            .read
            .format("org.apache.spark.sql.cassandra")
            .options(scala.collection.immutable.Map(
                "keyspace" -> "weather_keyspace",
                "table" -> "city_temps"
            ))
            .load().as[CassDataPoint].filter(x =>
                x.lat > minLat &&
                x.lat < maxLat &&
                x.lon > minLon &&
                x.lon < maxLon
            )
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
        (m.coordinates.lat, m.coordinates.lon , m.temperatureKelvin)
    })

    parsedDataPairs.saveToCassandra("weather_keyspace", "city_temps", SomeColumns("lat", "lon", "temp_kelvin"))

    // TODO: Calculate lon & lat boundaries for city

    // Retrieve multiple data points from Cassandra
    val ds = getAllCitiesInBlock(sparkSess, MIN_LAT, MAX_LAT, MIN_LON, MAX_LON)
    ds.show()

    // TODO: Do calculations

    // TODO: Forward to Kafka
    // produceNewMessages(calculatedDataPairs)

    ssc.start()
    ssc.awaitTermination()
    cassSession.close()
  }
}
