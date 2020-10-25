package sparkstreaming

import java.util.Properties

import com.datastax.driver.core.{Cluster, Session}
import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.streaming._
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka._
import spray.json.DefaultJsonProtocol._
import spray.json._
import org.apache.spark.sql.functions._

case class Coords(lon: Double, lat: Double)
case class CityTempDataPoint(temperatureKelvin: Double, coordinates: Coords, city: String)
case class GridTempDataPoint(temperatureKelvin: Double, coordinates: Coords)

case class CassDataPoint(lat: Double, lon: Double, temp_kelvin: Double)

case class BoundaryCoords(topLeft: Coords, bottomRight: Coords)

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

    val SIDE_LENGTH_SMALL_BLOCK: Double = 0.5 // lat/lon (TODO: Improve metric later)

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

     def produceNewMessages(data: DStream[List[(Coords, Double)]]): Unit = {
         data.foreachRDD(rdd => {
             rdd.foreachPartition { partitionOfRecords =>
                 val producer = initialiseProducer()
                 partitionOfRecords.foreach(record => {
                     record.foreach(gridEl =>  {
                         implicit val coordFormat = jsonFormat2(Coords)
                         implicit val gridFormat = jsonFormat2(GridTempDataPoint)
                         val gridData = GridTempDataPoint(gridEl._2, gridEl._1)
                         val message = new ProducerRecord[String, String](KAFKA_PRODUCING_TOPIC, null, gridData.toJson.compactPrint)
                         producer.send(message)
                     })
                 })
                 producer.close()
             }
         })
     }

    def getCityBlockBoundaries(cityCoords: Coords): BoundaryCoords = {

        // TODO: Calculate proper borders from cityCoords

        BoundaryCoords(
          Coords(MAX_LON, MIN_LAT),
          Coords(MIN_LON, MAX_LAT)
        )
    }

    def getAllCitiesInBlock(sparkSess: SparkSession, bounds: BoundaryCoords): Dataset[CassDataPoint] = {
        import sparkSess.implicits._
        sparkSess
            .read
            .format("org.apache.spark.sql.cassandra")
            .options(scala.collection.immutable.Map(
                "keyspace" -> "weather_keyspace",
                "table" -> "city_temps"
            ))
            .load()
            .as[CassDataPoint]
            .filter(x =>
                x.lat > bounds.topLeft.lat &&
                x.lat < bounds.bottomRight.lat &&
                x.lon > bounds.bottomRight.lon &&
                x.lon < bounds.topLeft.lon
            )
    }

    def createNewGridDataset(sparkSess: SparkSession, boundaryBlock: BoundaryCoords) : Dataset[CassDataPoint] = {
        import sparkSess.implicits._
        val deltaLat = boundaryBlock.topLeft.lat - boundaryBlock.bottomRight.lat
        val deltaLon = boundaryBlock.topLeft.lon - boundaryBlock.bottomRight.lon
        val numLatPoints = math.floor(deltaLat / SIDE_LENGTH_SMALL_BLOCK).toInt
        val numLonPoints = math.floor(deltaLon / SIDE_LENGTH_SMALL_BLOCK).toInt
        val numGridPoints = numLatPoints * numLonPoints

        val gridCoords = (1 to numGridPoints).map( index =>
            CassDataPoint(
                boundaryBlock.bottomRight.lat + SIDE_LENGTH_SMALL_BLOCK * (math.ceil(index/numLonPoints) - 1),  // lat
                boundaryBlock.topLeft.lon + SIDE_LENGTH_SMALL_BLOCK * ((index % numLonPoints) - 1),  // lon
                290  // temp
            )
        )
        gridCoords.toDS()
    }

    val cassSession = initialiseCassandra()
    val sparkSess = initialiseSparkSession()
    val ssc = initialiseStreamingContext(sparkSess)

    val messages = initialiseKafkaStream(ssc)
    val parsedDataPairs = messages.map(w => {
        implicit val coordFormat = jsonFormat2(Coords)
        implicit val tempFormat = jsonFormat3(CityTempDataPoint)
        (w._2).parseJson.convertTo[CityTempDataPoint]
    }).map(m => {
        (m.coordinates.lat, m.coordinates.lon , m.temperatureKelvin)
    })

    parsedDataPairs.saveToCassandra("weather_keyspace", "city_temps", SomeColumns("lat", "lon", "temp_kelvin"))

    val calculatedDataPairs = parsedDataPairs.map(m => {
        val boundaryBlock = getCityBlockBoundaries(Coords(m._2, m._1))
        val cityDS = getAllCitiesInBlock(sparkSess, boundaryBlock)
        val gridDS = createNewGridDataset(sparkSess, boundaryBlock)

        // TODO: Do calculations

        gridDS.rdd.map(m => (Coords(m.lon, m.lat), m.temp_kelvin)).collect.toList
    })

    produceNewMessages(calculatedDataPairs)

    ssc.start()
    ssc.awaitTermination()
    cassSession.close()
  }
}
