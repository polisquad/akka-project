// Akka
libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.5.21",
  "com.typesafe.akka" %% "akka-testkit" % "2.5.21" % Test
)

// Scala test
libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.5"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"
resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases"
addSbtPlugin("com.artima.supersafe" % "sbtplugin" % "1.1.3")

