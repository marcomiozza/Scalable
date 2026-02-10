ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "earthquake-spark",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.5.1" % "provided"
    )
  )


assembly / mainClass := Some("Main")

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}