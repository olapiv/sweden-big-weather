package generator

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


object ScalaProducerExample extends App {
   
    def weatherList: String = {
        // val url = "http://api.openweathermap.org/data/2.5/group?id=601972,602137,602149,602150&APPID=f857d120fca3c9d9138f63e95df28464"
        // val result = scala.io.Source.fromURL(url).mkString 
        
        val jsonFile = new File("results.json")
        val result = scala.io.Source.fromFile(jsonFile).mkString

        //println(result)
        val jsonAst = result.parseJson
        //println(jsonAst)
        val json = jsonAst.prettyPrint 
        //println(json)

        case class Coord(lon: Double, lat: Double)
        case class MainEntry(temp: Double)
        case class Place(coord: Coord, main: MainEntry, name: String)
        case class PlaceList(list: List[Place])

        // create the formats and provide them implicitly
        implicit val coordFormat = jsonFormat2(Coord)
        implicit val mainFormat = jsonFormat1(MainEntry)
        implicit val placeFormat = jsonFormat3(Place)
        implicit val placeListFormat = jsonFormat1(PlaceList)

        val value = jsonAst.convertTo[PlaceList]
        //println(placeList)
        println(key)
        
        return value + ""
    }

    val alphabet = 'a' to 'z'
    val events = 10000
    val rnd = new Random()
    val i = Random.nextInt(alphabet.size)

    val topic = "avg"

    val brokers = if (sys.env("BROKERS") != null) sys.env("BROKERS") else "localhost:9092"
    println("Kafka Brokers: " + brokers)

    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "ScalaProducerExample")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    
    val key = alphabet(i) + ""
    
    while (true) {
        val data = new ProducerRecord[String, String](topic, key, weatherList)
        producer.send(data)
        print(data + "\n")
    }

    producer.close()
    
}

