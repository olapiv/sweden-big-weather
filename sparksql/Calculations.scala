// package sparksql

// import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
// import org.apache.spark.sql.functions._
// import org.apache.spark.sql.cassandra._
// import com.datastax.spark.connector._
// import com.datastax.spark.connector.cql.CassandraConnector
// import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
// import scala.collection.JavaConversions._
// import scala.collection.mutable
// import java.util.Map
// import java.io._


// object Calculations extends App{

//     val spark = SparkSession.builder.master("local[*]").appName("Calculations").getOrCreate()
//     import spark.implicits._
//     val cassandra = if (sys.env("CASSANDRA") != null) sys.env("CASSANDRA") else "127.0.0.1"
//     println("Cassandra: " + cassandra)
//     //val cluster = Cluster.builder().addContactPoint(cassandra).build() 
    
//     println("About to connect to cluster")
//     //val session = cluster.connect()
//     println("Connected to cluster")

//     val df = spark.read.format("org.apache.spark.sql.cassandra")
//         .options(scala.collection.immutable.Map( "table" -> "avg", "keyspace" -> "avg_space"))
//         .load()

//     //val df = spark.read.table("avg")
//     println("Print table avg")
//     df.show()
    
// }
