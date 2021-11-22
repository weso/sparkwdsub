[![Continuous Integration](https://github.com/weso/sparkwdsub/actions/workflows/ci.yml/badge.svg)](https://github.com/weso/sparkwdsub/actions/workflows/ci.yml)

# sparkwdsub

Spark processing of wikidata subsets using Shape Expressions

This repo contains an example script that processes Wikidata subsets using Shape Expressions

# Building and running

The system requires [sbt](https://www.scala-sbt.org/) to be built. Once you download and install it. 
You only need to run:

```sh
sbt assembly
```

which will generate a flat jar with all dependencies in folder: `target/scala-2.12/` called: `sparkwdsub.jar`.

## Using a local cluster

In order to run the system locally, you need to download and install [Apache Spark](http://spark.apache.org/). You should have the executable `spark-submit` accessible in your path.

Once you have Spark installed and the `sparkwdsub.jar` generated you can run with the following example:


```sh
spark-submit --class "es.weso.wdsub.spark.Main" --master local[4] target/scala-2.12/sparkwdsub.jar -d examples/sample-dump1.json.gz  -m cluster -n testCities -s examples/cities.shex -k -o target/cities
```

which will generate a folder `target/cities` with information about the extracted dump.


## Using AWS

In AWS, you can do the following steps:

- Upload a Wikidata dump to Amazon S3. Dumps from wikidata are available [here](https://dumps.wikimedia.org/wikidatawiki/entities/)
- Create a cluster in Amazon EMR. Inside the cluster configure 2 steps: one step to run the spark-submit and another one to fail after the run finishes.
The step to run sparkwdsub has the following configuration in our system:
```sh
spark-submit --deploy-mode client --class es.weso.wdsub.spark.Main s3://weso/projects/wdsub/sparkwdsub.jar --mode cluster --name sparkwdsubLabra01 --dump s3://weso/datasets/wikidata/dump_20151116.json --schema s3://weso/projects/wdsub/author.shex --out s3://weso/projects/wdsub/
```

## Using Google Cloud

Instructions pending...

# Command line

```
Usage: sparkwdsub dump --schema <file> [--out <file>] [--site <string>] [--maxIterations <integer>] [--verbose] [--loggingLevel <string>] <dumpFile>
 Process example dump file.
 Options and flags:
     --help
         Display this help text.
     --schema <file>, -s <file>
         schema path
     --out <file>, -o <file>
         output path
     --site <string>
         Base url, default =http://www.wikidata.org/entity
     --maxIterations <integer>
        Max iterations for Pregel algorithm, default =20
     --verbose
         Verbose mode
     --loggingLevel <string>
         Logging level (ERROR, WARN, INFO), default=ERROR
```

Example:

```
sparkwdsub dump -s examples/cities.shex examples/6lines.json
```

