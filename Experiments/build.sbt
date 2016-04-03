name := "Experiments"
version := "1.0"
scalaVersion := "2.10.5"
libraryDependencies ++= Seq("org.apache.spark" % "spark-core_2.10" % "1.6.1",
                            "org.apache.spark" % "spark-mllib_2.10" % "1.6.1",
                            "org.apache.hadoop" % "hadoop-client" % "2.6.0")
resolvers += "Akka Repository" at "http://repo.akka.io/releases/"
