name := "predictionIO"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.0", // spark-related
  "org.apache.spark" %% "spark-sql" % "2.4.0", // spark-related
  "org.apache.spark" %% "spark-mllib" % "2.4.0", // spark-related
  "org.apache.spark" %% "spark-streaming" % "2.4.0", // spark-related
  "org.apache.spark" %% "spark-hive" % "2.4.0", // spark-hive
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.6.5",
  "mysql" % "mysql-connector-java" % "5.1.21", // mysql
  "net.debasishg" %% "redisclient" % "3.41", // redis
  "org.apache.predictionio" % "predictionio-sdk-java-client" % "0.13.0", // predictionIO
  "com.github.javafaker" % "javafaker" % "1.0.2", // Faker library
)


// hive jdbc (nếu dùng spark-hive thì bỏ cái này đi tránh lỗi), cần java 11
//libraryDependencies ++= Seq("org.apache.hive" % "hive-jdbc" % "2.1.1-cdh6.2.0"
// exclude("ch.qos.logback", "logback-classic"))
//1.1.0-cdh5.14.4: Production, 2.1.1-cdh6.2.0: Sandbox

// kafka/spark-kafka
libraryDependencies ++= Seq("org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.0",
  "io.confluent" % "kafka-avro-serializer" % "5.0.0",
  "io.confluent" % "kafka-json-schema-serializer" % "5.5.0"
)

// hbase client
//libraryDependencies ++= Seq( //1.1.0-cdh5.14.4: Production, 2.1.1-cdh6.2.0: Sandbox
//  //"org.apache.hadoop" % "hadoop-core" % "1.2.1", // bỏ cái này đi k sao, rất hay lỗi
//  "org.apache.hbase" % "hbase" % "2.1.0-cdh6.2.0",
//  "org.apache.hbase" % "hbase-client" % "2.1.0",
//  "org.apache.hbase" % "hbase-common" % "2.1.0",
//  "org.apache.hbase" % "hbase-server" % "2.1.0"
//)

// spark-hbase (nếu dùng spark-hive thì bỏ cái này đi tránh lỗi), cần java 11
//libraryDependencies += "org.apache.hbase" % "hbase-spark" % "2.1.0-cdh6.2.0"

// resolvers de? download tu` web khac thay vi`  https://repo1.maven.org/maven2/...
resolvers += "Confluent Maven Repository" at "https://packages.confluent.io/maven/"
resolvers += "Cloudera Maven" at "https://repository.cloudera.com/artifactory/cloudera-repos/"
resolvers += "Cloudera Maven1" at "https://repository.cloudera.com/artifactory/libs-release-local/"
resolvers += "jitpack" at "https://jitpack.io"

dependencyOverrides ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.5", // 2.9.0 cho spark-hbase
  "com.fasterxml.jackson.core" % "jackson-core" % "2.6.5", // 2.9.0 cho spark-hbase
  //"com.fasterxml.jackson.core" % "jackson-annotations" % "2.6.0",
  "com.google.guava" % "guava" % "15.0"
)
