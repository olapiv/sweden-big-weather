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
import org.apache.kafka.clients.admin.{NewTopic, AdminClient, ListTopicsOptions}

// JSON Structure of OpenweatherMap API
case class Coord(lon: Double, lat: Double)
case class MainEntry(temp: Double)
case class Place(coord: Coord, main: MainEntry, name: String)
case class PlaceList(list: List[Place])

case class RequiredDataPoint(temperatureKelvin: Double, coordinates: Coord, city: String)


object TemperatureProducer extends App {

    // From parseCities.py & allCities.json
    val CITY_IDs = "601972,602137,602149,602150,602201,602261,602426,602566,602804,602909,602912,602913,603035,603037,603261,603291,603292,603303,603304,603448,603569,603570,603760,603761,603814,603819,603823,603897,603919,604006,604010,604382,604419,604488,604490,604496,604779,605028,605155,605314,605387,605393,605428,605647,605676,605857,605859,606072,606084,606086,606141,606209,606239,606249,606266,606333,606350,606363,606370,606388,606507,606530,606531,606638,606642,606649,606714,606833,606834,813418,2599289,2599299,2599415,2599418,2600754,2602446,2602687,2661886,2662148,2662149,2662324,2662517,2662659,2662666,2662689,2662706,2662758,2662777,2662778,2662795,2662830,2662851,2662868,2662880,2662881,2662935,2662946,2662996,2663036,2663077,2663161,2663254,2663293,2663398,2663399,2663432,2663435,2663482,2663514,2663535,2663536,2663540,2663548,2663585,2664129,2664179,2664194,2664201,2664203,2664292,2664315,2664330,2664339,2664415,2664441,2664454,2664529,2664783,2664852,2664855,2664870,2664880,2664921,2664939,2664994,2664996,2665018,2665063,2665090,2665093,2665170,2665171,2665174,2665403,2665451,2665452,2665487,2665578,2665675,2665691,2665708,2665844,2665857,2665901,2665902,2666199,2666218,2666219,2666237,2666238,2666264,2666493,2666545,2666669,2666670,2666771,2666823,2666950,2667094,2667109,2667253,2667264,2667302,2667303,2667328,2667401,2667402,2667405,2667499,2667599,2667628,2667757,2667809,2667847,2667872,2667876,2667904,2668180,2668195,2668247,2668365,2668429,2668516,2668552,2668674,2669019,2669046,2669047,2669057,2669086,2669098,2669112,2669113,2669117,2669118,2669193,2669254,2669342,2669414,2669415,2669768,2669772,2669788,2670036,2670044,2670061,2670127,2670128,2670278,2670403,2670470,2670478,2670536,2670540,2670542,2670603,2670613,2670710,2670715,2670776,2670781,2670833,2670878,2670879,2670883,2670995,2671055,2671060,2671206,2671221,2671224,2671230,2671389,2671392,2671490,2671529,2671530,2672053,2673076,2673146,2673621,2673629,2673693,2673722,2673723,2673730,2673758,2673829,2673875,2673952,2673969,2674181,2674209,2674326,2674594,2674649,2674803,2674993,2675078,2675079,2675146,2675175,2675280,2675365,2675382,2675396,2675397,2675406,2675408,2675416,2675418,2675561,2675691,2676174,2676176,2676207,2676214,2676215,2676222,2676224,2676238,2676488,2676511,2676586,2676591,2676592,2676644,2676700,2676731,2677019,2677025,2677062,2677063,2677233,2677234,2677239,2677356,2677370,2677456,2677548,2677593,2677644,2677719,2677869,2678129,2678153,2678173,2678205,2678210,2678265,2678528,2678687,2678804,2678913,2678926,2679106,2679107,2679226,2679300,2679302,2679397,2679567,2679698,2679721,2679819,2679832,2679855,2679920,2680068,2680075,2680227,2680350,2680352,2680460,2680657,2680662,2680764,2680820,2680959,2680969,2680977,2680985,2681368,2681416,2681447,2681576,2681587,2681743,2681821,2681825,2681866,2682155,2682487,2682532,2682569,2682570,2682706,2682776,2682834,2682850,2682913,2682977,2682995,2683371,2683436,2683709,2683803,2683835,2684213,2684231,2684386,2684394,2684395,2684485,2684593,2684636,2684653,2684945,2685092,2685177,2685699,2685724,2685747,2685750,2685828,2685867,2685900,2685903,2685915,2685981,2686087,2686153,2686160,2686161,2686162,2686209,2686286,2686380,2686466,2686469,2686493,2686596,2686649,2686655,2686656,2686657,2686674,2686858,2686979,2687062,2687063,2687272,2687420,2687444,2687454,2687498,2687509,2687517,2687636,2687690,2687693,2687698,2687700,2687897,2687902,2688131,2688172,2688188,2688248,2688250,2688255,2688367,2688368,2689018,2689192,2689204,2689287,2689336,2689344,2689452,2689471,2689513,2689520,2689570,2689580,2689733,2690055,2690061,2690101,2690167,2690170,2690283,2690478,2690578,2690580,2690713,2690829,2690843,2690866,2690901,2690902,2690959,2690960,2690989,2691230,2691271,2691326,2691348,2691387,2691395,2691400,2691407,2691440,2691457,2691459,2691462,2691471,2691510,2691542,2691602,2691742,2691743,2691792,2691798,2692049,2692059,2692349,2692469,2692475,2692596,2692611,2692613,2692624,2692630,2692633,2692637,2692644,2692661,2692748,2692876,2692943,2692965,2692969,2692972,2693002,2693049,2693081,2693152,2693301,2693313,2693346,2693347,2693524,2693555,2693678,2693759,2694165,2694264,2694283,2694366,2694375,2694417,2694475,2694502,2694503,2694522,2694529,2694537,2694552,2694554,2694560,2694631,2694759,2694762,2694779,2694808,2694831,2694838,2694893,2695016,2695079,2696058,2696059,2696328,2696329,2696332,2696334,2696473,2696479,2696500,2696503,2696608,2696650,2696804,2697295,2697455,2697703,2697719,2697720,2697727,2697766,2697859,2697861,2697944,2698242,2698293,2698295,2698433,2698520,2698681,2698691,2698697,2698726,2698729,2698733,2698738,2698739,2698745,2698753,2698767,2699050,2699176,2699206,2699281,2699282,2699308,2699310,2699393,2699394,2699767,2699768,2699777,2699780,2699786,2699791,2699879,2699957,2700011,2700079,2700162,2700199,2700218,2700279,2700487,2700497,2700509,2700597,2700663,2700694,2700754,2700791,2700800,2700801,2700839,2700854,2700858,2700947,2700960,2701078,2701096,2701123,2701154,2701221,2701223,2701590,2701640,2701679,2701680,2701712,2701713,2701714,2701715,2701725,2701727,2701757,2701792,2701980,2702259,2702260,2702261,2702405,2702449,2702469,2702489,2702683,2702776,2702908,2702923,2702976,2702977,2702979,2702996,2702997,2703038,2703145,2703212,2703268,2703292,2703300,2703321,2703330,2703339,2703348,2703396,2703525,2703546,2703610,2703684,2703714,2703756,2703827,2703935,2704008,2704042,2704114,2704136,2704245,2704327,2704397,2704398,2704611,2704613,2704619,2704620,2704638,2704654,2704662,2704716,2704885,2704956,2704962,2705049,2705055,2705075,2705085,2705127,2705294,2705402,2705562,2705718,2706003,2706040,2706054,2706057,2706100,2706183,2706184,2706323,2706392,2706435,2706448,2706483,2706485,2706523,2706543,2706619,2706678,2706713,2706766,2706767,2706914,2706917,2706981,2706982,2707057,2707138,2707217,2707363,2707366,2707396,2707399,2707608,2707664,2707682,2707684,2707952,2707953,2708016,2708025,2708152,2708326,2708363,2708365,2708366,2708426,2708429,2708511,2708663,2708665,2708710,2708794,2708821,2708889,2709196,2709214,2709494,2709600,2709628,2709912,2709969,2710205,2710268,2710343,2710683,2710731,2710797,2710885,2711174,2711256,2711451,2711508,2711509,2711526,2711533,2711537,2711790,2711798,2711820,2711896,2712044,2712045,2712190,2712255,2712373,2712377,2712409,2712411,2712414,2712642,2712651,2712859,2712887,2712988,2712995,2713193,2713219,2713350,2713385,2713509,2713516,2713596,2713656,2713659,2713679,2713797,2713943,2713964,2713971,2713974,2714327,2714401,2714618,2714678,2714727,2714765,2714795,2714903,2715081,2715164,2715324,2715329,2715341,2715351,2715458,2715459,2715566,2715568,2715572,2715573,2715656,2715658,2715945,2715946,2715951,2715953,2716080,2716085,2716165,2716166,2716281,2716296,2716437,2716443,2716586,2716601,2716668,2716758,2716825,2716854,2716863,2716968,2717126,2717266,2717268,2717400,2717401,2717412,2717517,2717558,2717720,2717824,2717842,2717881,2717884,2717898,2718133,2718144,2718245,2718271,2718393,2718759,2718836,2718839,2718975,2718991,2719106,2719111,2719212,2719245,2719312,2719318,2719466,2719485,2719609,2719670,2719757,2719961,2719965,2720004,2720028,2720037,2720114,2720382,2720383,2720437,2720485,2720496,2720501,2720504,2720670,2720678,2720679,2720681,2720689,2720793,2721035,2721226,2721259,2721285,2721348,2721357,2721505,2721518,2721534,2721581,2721585,2721828,2722026,2722120,2722320,2722362,2722363,2722545,2722563,2722570,2722734,2722742,2722751,2722799,2722836,2722932,2722996,2723079,2723128,2723201,2723287,2723350,2723362,2723440,2723488,2723503,2723567,2724134,2724180,2724227,2724231,2724320,2724321,2724327,2724410,2724413,2724435,2724472,2724621,2724693,2724778,2724956,2724958,2724962,2725003,2725065,2725073,2725123,2725134,2725136,2725192,2725194,2725201,2725271,2725336,2725348,2725371,2725372,2725378,2725379,2725422,2725432,2725439,2725456,2725469,2725471,2725722,2725898,2725901,2725924,2726005,2726026,2726049,2726163,2726240,2726285,2726314,2726330,2726332,2726347,2726394,2726416,2726470,2726568,2726576,2726615,2726705,2726755,2726756,2726939,2726983,2727130,2727222,2727234,2727314,2727363,2727594,2727664,2727731,3326000,3326030,3326037,3326084,3328014,3331621,3332754,3332888,3336566,3336568,3336569,3337385,3337386,6269436,6619708,6930653,6930654,6943587,6944523,7283918,7304451,7304459,7670962,7839202,7839203,7931985,8051325,8126430,8131853"
    val MAX_IDs = 20

    val WEATHER_API_TOKEN = sys.env("WEATHER_API_TOKEN")
    val BROKER_URL = sys.env("BROKER_URL") // "localhost:9092"
    val KAFKA_TOPIC = "city-temperatures"

    // The is actually not necessary! The topic is created by itself
    def createTopic(): Unit = {
        val props = new Properties()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_URL)
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "ScalaProducerExample")
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        val localKafkaAdmin = AdminClient.create(props)

        // localKafkaAdmin.createTopics(List(new NewTopic(KAFKA_TOPIC, 1, 1)))
    }

    def initialiseProducer(): KafkaProducer[String, String] = {
        val props = new Properties()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_URL)
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "ScalaProducerExample")
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        new KafkaProducer[String, String](props)
    }

    def getDataString(cityIdsString: String): String = {
        
        // val url = "http://api.openweathermap.org/data/2.5/group?id=601972,602137,602149,602150&APPID=" + WEATHER_API_TOKEN
        val url = "http://api.openweathermap.org/data/2.5/group?id=" + cityIdsString + "&APPID=" + WEATHER_API_TOKEN
        val result = scala.io.Source.fromURL(url).mkString 
        
        // val jsonFile = new File("sampleWeatherData.json")
        // val result = scala.io.Source.fromFile(jsonFile).mkString

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

    val cityChunks = CITY_IDs.split(",").grouped(MAX_IDs).toList
    val cityChunkStrings = cityChunks.map(listChunk => listChunk.mkString(","))

    // Kafka
    // createTopic()
    val producer = initialiseProducer()

    while (true) {
        for (cityChunkString <- cityChunkStrings) {
            println(cityChunkString)
            val dataString = getDataString(cityChunkString);
            val placeList = parseJSON(dataString)
            val transformedDataList = placeList.list.map(x => RequiredDataPoint(x.main.temp, x.coord, x.name))

            implicit val coordFormat = jsonFormat2(Coord)
            implicit val requiredDataFormat = jsonFormat3(RequiredDataPoint)

            for (dataPoint <- transformedDataList) {
                Thread.sleep(20)
                val data = new ProducerRecord[String, String](KAFKA_TOPIC, null, dataPoint.toJson.compactPrint)
                producer.send(data)
                println(data)
            }
            Thread.sleep(3000)
        }
    }

    producer.close()
    
}

