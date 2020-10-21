package producer

import scalaj.http._
import spray.json._
import DefaultJsonProtocol._
import com.fasterxml.jackson._
import scala.collection.JavaConversions._
import scala.collection.mutable
import java.util.Map
import java.io._
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random
import kafka.producer.KeyedMessage

// JSON Structure of OpenweatherMap API
case class Coord(lon: Double, lat: Double)
case class MainEntry(temp: Double)
case class Place(coord: Coord, main: MainEntry, name: String)
case class PlaceList(list: List[Place])

case class RequiredDataPoint(temperatureKelvin: Double, coordinates: Coord, city: String)


object TemperatureProducer extends App {

    val WEATHER_API_TOKEN = sys.env("WEATHER_API_TOKEN")
    val BROKER_URL = sys.env("BROKER_URL") // "localhost:9092"
    val KAFKA_TOPIC = "city-temperatures"

    def initialiseProducer(brokerURL: String): KafkaProducer[String, String] = {
        val props = new Properties()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerURL)
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "ScalaProducerExample")
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        new KafkaProducer[String, String](props)
    }

    def getDataString(): String = {
        // val url = "http://api.openweathermap.org/data/2.5/group?id=601972,602137,602149,602150&APPID=" + WEATHER_API_TOKEN
        // val result = scala.io.Source.fromURL(url).mkString 
        
        val jsonFile = new File("sampleWeatherData.json")
        val result = scala.io.Source.fromFile(jsonFile).mkString

        result
    }
   
    def parseJSON(jsonString: String): PlaceList = {
        val jsonAst = jsonString.parseJson

        // Create the formats and provide them implicitly
        implicit val coordFormat = jsonFormat2(Coord)
        implicit val mainFormat = jsonFormat1(MainEntry)
        implicit val placeFormat = jsonFormat3(Place)
        implicit val placeListFormat = jsonFormat1(PlaceList)

        jsonAst.convertTo[PlaceList]
    }

    // Kafka
    val producer = initialiseProducer(BROKER_URL)

    while (true) {
        val dataString = getDataString();
        val placeList = parseJSON(dataString)
        val transformedDataList = placeList.list.map(x => RequiredDataPoint(x.main.temp, x.coord, x.name))

        implicit val coordFormat = jsonFormat2(Coord)
        implicit val requiredDataFormat = jsonFormat3(RequiredDataPoint)


        for (dataPoint <- transformedDataList) {
            Thread.sleep(200)
            val data = new ProducerRecord[String, String](KAFKA_TOPIC, null, dataPoint.toJson.compactPrint)
            producer.send(data)
            println(data)
        }
    }

    producer.close()
    
}

