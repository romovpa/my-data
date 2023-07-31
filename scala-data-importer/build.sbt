ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.2.2"

lazy val root = (project in file("."))
  .settings(
    name := "scala-data-importer",
    idePackagePrefix := Some("mydata")
  )

libraryDependencies += "javax.mail" % "javax.mail-api" % "1.6.2"
libraryDependencies += "com.sun.mail" % "javax.mail" % "1.6.2"
libraryDependencies += "org.apache.tika" % "tika-core" % "2.6.0"
libraryDependencies += "org.apache.tika" % "tika-parsers" % "2.6.0"
libraryDependencies += "org.apache.commons" % "commons-email" % "1.5"

