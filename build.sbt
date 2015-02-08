import sbt._
import Keys._

name := "cf150204"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark"  %%  "spark-core"  %   "1.2.0" % "provided",
  "org.apache.spark"  %%  "spark-mllib"  %  "1.2.0" % "provided",
  "joda-time"         %   "joda-time"   %   "2.7"
)

