lazy val scala212 = "2.12.14"
lazy val supportedScalaVersions = List(
  scala212,
)

val Java11 = "adopt@1.11"

// Weso dependencies
lazy val wdsubVersion          = "0.0.16"

// Other dependencies
lazy val declineVersion        = "2.1.0"
lazy val munitVersion          = "0.7.27"
lazy val munitEffectVersion    = "1.0.5"
lazy val sparkVersion          = "3.1.2"
lazy val jacksonVersion        = "2.12.2"
lazy val sparkFastTestsVersion = "1.0.0"

// Other dependencies 
lazy val wdsub             = "es.weso"                    %% "wdsub"               % wdsubVersion
lazy val decline           = "com.monovore"               %% "decline"             % declineVersion
lazy val declineEffect     = "com.monovore"               %% "decline-effect"      % declineVersion
lazy val jackson           = "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion
lazy val munit             = "org.scalameta"              %% "munit"               % munitVersion
lazy val munitEffect       = "org.typelevel"              %% "munit-cats-effect-3" % munitEffectVersion
lazy val sparkSql          = "org.apache.spark"           %% "spark-sql"           % sparkVersion
lazy val sparkGraphx       = "org.apache.spark"           %% "spark-graphx"        % sparkVersion
lazy val sparkFast         = "com.github.mrpowers"        %% "spark-fast-tests"    % sparkFastTestsVersion


lazy val MUnitFramework = new TestFramework("munit.Framework")

ThisBuild / githubWorkflowJavaVersions := Seq(Java11)

lazy val sparkWdsubRoot = project
  .in(file("."))
  .enablePlugins(
    DockerPlugin,
    ScalaUnidocPlugin,
    SiteScaladocPlugin,
    AsciidoctorPlugin,
    SbtNativePackager,
    WindowsPlugin,
    JavaAppPackaging,
    LauncherJarPlugin
    )
    .enablePlugins(BuildInfoPlugin)
  .settings(dockerSettings)
  .aggregate()
  .dependsOn()
  .settings(
    libraryDependencies ++= Seq(
      wdsub, 
      sparkSql, 
      sparkGraphx, 
      jackson,
      sparkFast % Test
    ),
    fork := true,
    ThisBuild / turbo := true,
    ThisBuild / crossScalaVersions := supportedScalaVersions,
//    Compile / run / mainClass := Some("es.weso.shexs.Main"),
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
    buildInfoPackage := "buildinfo"
  )

/* ********************************************************
 ******************** Grouped Settings ********************
 **********************************************************/

import com.typesafe.sbt.packager.docker.DockerChmodType

lazy val dockerSettings = Seq(
  dockerRepository := Some("wesogroup"), 
  Docker / packageName := "sparkwdsub",
  dockerBaseImage := "openjdk:11",
  dockerAdditionalPermissions ++= Seq((DockerChmodType.UserGroupWriteExecute, "/tmp")),
  Docker / daemonUserUid := Some("0"),
  Docker / daemonUser    := "root"
)

resolvers += "Spark Packages Repo" at "https://repos.spark-packages.org/"