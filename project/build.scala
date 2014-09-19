import sbt._
import sbt.Keys._

object SpookyStuffBuild extends Build {

  // Settings
  override lazy val settings = super.settings ++ Seq(
    scalacOptions ++= Seq(
      "-deprecation",
      "-unchecked"
    ),
    version := "0.1.0-SNAPSHOT",
    scalaVersion := "2.10.4"
  )

  // Core
  lazy val core = Project("core", file("core")).
    settings(coreSettings: _*).
    settings(Assembly.settings: _*)

  lazy val coreSettings = Seq(
    name := "spookystuff-core",
    libraryDependencies ++= Seq(
      "org.apache.spark"             % "spark-core_2.10"% "1.1.0",
      "org.apache.spark"             % "spark-sql_2.10" % "1.1.0",
      "com.github.detro.ghostdriver" % "phantomjsdriver"% "1.1.0",
      "org.apache.tika"              % "tika-java7"     % "1.5",
      "org.jsoup"                    % "jsoup"          % "1.7.3",
      "com.syncthemall"              % "boilerpipe"     % "1.2.2",
      "org.glassfish.jersey.core"    % "jersey-client"  % "2.12",
      "org.scalamacros"              % "paradise_2.10.4"% "2.0.1",
      "org.scalatest"                % "scalatest_2.10" % "2.2.0" % "test",
      "junit"                        % "junit"          % "4.11"  % "test"
    )
  )

  // Shell
  lazy val shell = Project("shell", file("shell")).
    settings(shellSettings: _*).
    dependsOn(core).
    settings(Assembly.settings: _*)

  lazy val shellSettings = Seq(
    name := "spookystuff-shell",
    libraryDependencies ++= Seq(
      "org.apache.spark" % "spark-repl_2.10" % "1.0.2"
    )
  )

  // Example
  lazy val example = Project("example", file("example")).
    settings(exampleSettings: _*).
    dependsOn(core).
    settings(Assembly.settings: _*)

  lazy val exampleSettings = Seq(
    name := "spookystuff-example"
  )

}

object Assembly {
  import sbtassembly.Plugin._
  import AssemblyKeys._

  // Assembly Settings
  lazy val settings = assemblySettings ++ Seq(
    test in assembly := {},
    mergeStrategy in assembly := {
      case PathList("org", "datanucleus", xs @ _*)             => MergeStrategy.discard
      case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
      case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
      case "log4j.properties"                                  => MergeStrategy.discard
      case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
      case "reference.conf"                                    => MergeStrategy.concat
      case _                                                   => MergeStrategy.first
    }
  )
}
