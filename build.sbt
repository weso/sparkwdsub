lazy val scala212 = "2.12.14"
lazy val supportedScalaVersions = List(
  scala212,
)

val Java11 = "adopt@1.11"
val Java8 = "adopt@1.8"

// Weso dependencies
// lazy val wdsubVersion          = "0.0.16"
lazy val shexsVersion          = "0.1.97"
lazy val srdfVersion           = "0.1.104"
lazy val utilsVersion          = "0.1.99"
lazy val documentVersion       = "0.0.33"


// Other dependencies
lazy val catsVersion           = "2.6.1"
lazy val declineVersion        = "2.1.0"
lazy val munitVersion          = "0.7.27"
lazy val munitEffectVersion    = "1.0.5"
lazy val sparkVersion          = "3.1.2"
lazy val jacksonVersion        = "2.12.2"
lazy val sparkFastTestsVersion = "1.0.0"
lazy val wikidataToolkitVersion = "0.12.1"


// Other dependencies 
lazy val catsCore          = "org.typelevel"                %% "cats-core"          % catsVersion
lazy val catsKernel        = "org.typelevel"                %% "cats-kernel"        % catsVersion
// lazy val wdsub             = "es.weso"                      %% "wdsub"               % wdsubVersion
lazy val decline           = "com.monovore"                 %% "decline"             % declineVersion
lazy val declineEffect     = "com.monovore"                 %% "decline-effect"      % declineVersion
lazy val jackson           = "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion
lazy val munit             = "org.scalameta"                %% "munit"               % munitVersion
lazy val munitEffect       = "org.typelevel"                %% "munit-cats-effect-3" % munitEffectVersion
lazy val sparkSql          = "org.apache.spark"             %% "spark-sql"           % sparkVersion
lazy val sparkGraphx       = "org.apache.spark"             %% "spark-graphx"        % sparkVersion
lazy val sparkFast         = "com.github.mrpowers"          %% "spark-fast-tests"    % sparkFastTestsVersion

lazy val wdtk_dumpfiles   = "org.wikidata.wdtk" % "wdtk-dumpfiles"   % wikidataToolkitVersion
lazy val wdtk_wikibaseapi = "org.wikidata.wdtk" % "wdtk-wikibaseapi" % wikidataToolkitVersion
lazy val wdtk_datamodel   = "org.wikidata.wdtk" % "wdtk-datamodel"   % wikidataToolkitVersion
lazy val wdtk_rdf         = "org.wikidata.wdtk" % "wdtk-rdf"         % wikidataToolkitVersion
lazy val wdtk_storage     = "org.wikidata.wdtk" % "wdtk-storage"     % wikidataToolkitVersion
lazy val wdtk_util        = "org.wikidata.wdtk" % "wdtk-util"        % wikidataToolkitVersion

// WESO components
lazy val document          = "es.weso"                    %% "document"        % documentVersion
lazy val srdf              = "es.weso"                    %% "srdf"            % srdfVersion
lazy val srdfJena          = "es.weso"                    %% "srdfjena"        % srdfVersion
lazy val srdf4j            = "es.weso"                    %% "srdf4j"          % srdfVersion
lazy val utils             = "es.weso"                    %% "utils"           % utilsVersion
lazy val shex              = "es.weso"                    %% "shex"            % shexsVersion

lazy val MUnitFramework = new TestFramework("munit.Framework")

ThisBuild / githubWorkflowJavaVersions := Seq(Java8)

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
  .settings(
    assembly / mainClass := Some("es.weso.sparkwdsub.Main"),
    assembly / assemblyJarName := "wdsub.jar",
    ThisBuild / assemblyMergeStrategy := {
     case x if Assembly.isConfigFile(x) => MergeStrategy.concat      
     case PathList("META-INF", xs @ _*) => MergeStrategy.discard
     case x => MergeStrategy.first
    },
    dockerSettings
    )
  .aggregate()
  .dependsOn()
  .settings(
    libraryDependencies ++= Seq(
      catsCore, catsKernel,
 //     wdsub, 
      srdf,
      srdfJena, 
      shex,
      wdtk_dumpfiles, 
      wdtk_wikibaseapi,
      sparkSql, 
      sparkGraphx, 
      jackson,
      decline, declineEffect,
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