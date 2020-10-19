name := "Producer"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.kafka" % "kafka_2.11" % "1.0.0"

libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.2"
libraryDependencies +=  "org.scalaj" %% "scalaj-http" % """|2.4.2""".stripMargin
libraryDependencies += "io.spray" %%  "spray-json" % """|1.3.5""".stripMargin
