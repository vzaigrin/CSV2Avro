ThisBuild / version := "1.0"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "CSV2Avro",
    libraryDependencies ++= Seq(
      "com.typesafe"       % "config"       % "1.4.1",
      "org.apache.commons" % "commons-csv"  % "1.9.0",
      "org.apache.avro"    % "avro"         % "1.11.0",
      "org.slf4j"          % "slf4j-api"    % "1.7.35",
      "org.slf4j"          % "slf4j-simple" % "1.7.35"
    ),
    assembly / assemblyMergeStrategy := {
      case m if m.toLowerCase.endsWith("manifest.mf")       => MergeStrategy.discard
      case m if m.toLowerCase.matches("meta-inf.*\\.sf$")   => MergeStrategy.discard
      case "reference.conf"                                 => MergeStrategy.concat
      case x: String if x.contains("UnusedStubClass.class") => MergeStrategy.first
      case _                                                => MergeStrategy.first
    }
  )
