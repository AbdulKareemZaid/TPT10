ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "Twitter Stream Processing",
    logLevel := Level.Warn,
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.3",
      "org.apache.spark" %% "spark-sql" % "3.5.3",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.3",
      "com.johnsnowlabs.nlp" %% "spark-nlp" % "5.5.2",
      "org.apache.spark" %% "spark-mllib" % "3.5.4",
      "commons-lang" % "commons-lang" % "2.6",
      "org.mongodb.spark" %% "mongo-spark-connector" % "10.4.0"

    )
  )
