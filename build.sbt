val myVersion = "1.7"
val myScalaVersion = "2.11.8"
val mySparkVersion = "2.2.0"

lazy val commonSettings = Seq(
  organization := "org.btrust",
  version := myVersion,
  scalacOptions := Seq("-unchecked", "-deprecation"),
  scalaVersion := myScalaVersion)

lazy val spark = Seq(
  libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.1",
  libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test", 
  libraryDependencies += "org.apache.spark" %% "spark-sql" % mySparkVersion % Provided,
  libraryDependencies += "org.apache.spark" %% "spark-core" % mySparkVersion % Provided)

lazy val herramienta = (project in file("herramienta"))
  .settings(commonSettings: _*)
  .settings(spark: _*)
  .settings(name := "Herramienta Chedraui")
