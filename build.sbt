inThisBuild(
  Seq(
    version            := "0.1.0-SNAPSHOT",
    scalaVersion       := "3.3.0",
    crossScalaVersions := Seq(scalaVersion.value, "2.13.8")
  )
)

val zioVersion    = "2.0.15"
val hadoopVersion = "3.3.5"

lazy val root = (project in file("."))
  .settings(
    name := "zio-hdfs",
    libraryDependencies ++= Seq(
      "dev.zio"          %% "zio"                % zioVersion,
      "dev.zio"          %% "zio-streams"        % zioVersion,
      "dev.zio"          %% "zio-test"           % zioVersion    % Test,
      "dev.zio"          %% "zio-test-sbt"       % zioVersion    % Test,
      "org.apache.hadoop" % "hadoop-common"      % hadoopVersion,
      "org.apache.hadoop" % "hadoop-minicluster" % hadoopVersion % Test,
      // Minicluster does not provide Mockito because of test jar transitivy issue
      "org.mockito"       % "mockito-core"       % "2.28.2"      % Test
    )
  )
