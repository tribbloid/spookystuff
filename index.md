---
layout: global
title: Overview - SpookyStuff SPOOKYSTUFF_VERSION documentation
---

* This will become a table of contents (this text will be scraped).
{:toc}

# Overview

**SpookyStuff** is a scalable and lightweight query engine for web scraping/data mashup/acceptance QA. The goal is to allow remote resources to be linked and queried like a relational database. Its currently implementation is influenced by Spark [SQL] and [Machine Learning Pipeline].

SpookyStuff is the fastest big data collection engine in history, with a speed record of querying 330404 dynamic pages per hour on 300 cores.

# Powered by

![Apache Spark](http://spark.apache.org/images/spark-logo.png) ![Selenium](http://docs.seleniumhq.org/images/big-logo.png) ![PhantomJS](http://phantomjs.org/img/phantomjs-logo.png) ![Apache Tika](http://tika.apache.org/tika.png) ![Apache Maven](http://maven.apache.org/images/logos/maven-feather.png) ![Apache Http Components](https://hc.apache.org/images/logos/httpcomponents.png)

# Installation

*Main article: [Installation](installation.html)*

Why so complex? All you need is is a single library in your Java/Scala classpath:

    groupId: org.tribbloid.spookystuff
    artifactId: spookystuff-assembly_2.10
    version: SPOOKYSTUFF_VERSION

You have 3 options: download it, build it or let a dependency manager (Apache Maven, sbt, Gradle etc.) do it for you.

#### Direct Download

| Stable (SPOOKYSTUFF_VERSION) |
| ------------- |
| [Download pre-built JAR for Spark 1.3.1] |
| [Download pre-built JAR for Spark 1.4.1] |

This JAR provide full functionality out-of-the-box, however you need a compatible Apache Spark installation first (including any integrated Spark environment, e.g. Spark notebook in [Apache Zeppelin] or [databricks™ Cloud]). If you haven't done so, please refer to [Apache Spark installation guide](https://spark.apache.org/docs/latest/cluster-overview.html) or documentation of your Spark distribution.

<!---
Alternatively you can download the all-inclusive bundle, this distribution is bundled with Spark and also contains shell scripts to launch examples and spooky-shell (a minimalistic interactive shell that has SpookyStuff pre-loaded):

- [Download all-inclusive bundle for Spark 1.3.1]
- [Download all-inclusive bundle for Spark 1.4.1]
-->

#### As a Dependency

if you want to use SpookyStuff as a library in your source code, the easiest way is to let your dependency manager handle it. By providing the following reference:

    groupId: org.tribbloid.spookystuff
    artifactId: spookystuff-assembly_2.10
    version: SPOOKYSTUFF_VERSION

Your dependency manager (e.g. Apache Maven, sbt, gradle) can download it automatically from the [central maven repository].

Many integrated Spark environments (e.g. Spark-Shell, [Apache Zeppelin] and [databricks™ Cloud]) has built-in dependency manager, which makes deployment much easier by eliminating the necessity of downloading manually. Please refer to [Integration] section for details.

#### Sourcecode Download

If you are good with programming and prefer to build it from scratch:

| Stable (SPOOKYSTUFF_VERSION) | Nightly (master) |
| ------------ | ----------- |
| [Download .zip](https://github.com/tribbloid/spookystuff/zipball/release-SPOOKYSTUFF_VERSION) | [Download .zip](https://github.com/tribbloid/spookystuff/zipball/master) |
| [Download .tar.gz](https://github.com/tribbloid/spookystuff/tarball/release-SPOOKYSTUFF_VERSION) | [Download .tar.gz](https://github.com/tribbloid/spookystuff/tarball/master) |

For how to build from sourcecode, please refer to [Build] section.

# Quick Start

First, make sure Spark is working under your favorite IDE/REPL:

    import org.apache.spark.SparkContext
    import org.apache.spark.SparkConf

    // you don't need these if sc has been initialized
    // val conf = new SparkConf().setAppName("SpookyStuff example").setMaster("local[*]")
    // val sc = new SparkContext(conf)
    assert(sc.parallelize(1 to 100).reduce(_ + _) == 5050)

Next, import and initialize a SpookyContext (this is the entry point of all language-integrated queries, much like SQLContext for Spark SQL):

    import org.tribbloid.spookystuff.actions._
    import org.tribbloid.spookystuff.dsl._

    val spooky = new org.tribbloid.spookystuff.SpookyContext.SpookyContext(sc)
    import spooky.dsl._

From this point you can run queries on public datasets immediately. The following is a minimalistic showcase on cross-site "join", one of the 5 main clauses:

    val rows = spooky.wget("https://ajax.googleapis.com/ajax/services/search/news?v=1.0&q=barack%20obama")
    .select((S\"responseData"\"results"\"content" text) ~ 'news)
    .wgetJoin(x"http://api.mymemory.translated.net/get?q=${'news}!&langpair=en|fr")
    .select((S\"responseData"\"translatedText" text) ~ 'translation)
    rows.toDF().collect().foreach(println)

You will get a list of summaries of English news about BHO and their respective french translations:



<!-- Wondering what it does in 4 lines? Here is a simple breakdown: -->

For more information on query syntax and usage, please go to [Query Guide].

# Web Caching

You may already notice that repeatedly running a query takes much less time than running it for the first time. this is because all web resources are cached: cached resources are loaded directly from a file directory (can be on any Hadoop-supported file system, namely HDD, HDFS, Amazon S3 and Tachyon etc.) if they haven't expired. RDD cache is enabled by default to facilitate repeated data wrangling and dry run. To disable it, simply set **conf.cacheRead** under SpookyContext to false or set **conf.pageExpireAfter** to a very small duration:

    import scala.concurrent.duration._

    spooky.conf.cacheRead = false
    spooky.conf.pageExpireAfter = 1.minute

However, before you run a query, it is recommended to point the web cache directory to a publicly-accessible, high-available storage URL (e.g. starting with ```hdfs://``` or ```s3n://```). Otherwise SpookyStuff will use *<Java working directory>/temp/cache* on local file system by default, which means if your query is running on a cluster, it will have a chance not able to use already cached resources because they are on other machine(s). This directory can be set in SpookyContext by its **conf.dirs._cache**, which affects execution of all queries generated from it:

    spooky.conf.dirs._cache = "hdfs://spooky-cache"

Or you can override the default web cache directory globally by setting **spooky.cache** system property in your Java option:

- if your query is launched from a standalone Java application:

    -Dspooky.cache=hdfs://spooky-cache

- OR, if your query is launched by spark-submit.sh

    --conf spark.driver.extraJavaOptions="-Dspooky.cache=hdfs://spooky-cache"

For more performance optimization options, please go to [Deploying Guide].

# Scaling

SpookyStuff is optimized for running on Spark [cluster mode](cluster-overview.html), which accelerates execution by parallelizing over multiple machine's processing power and network bandwidth.

It should be noted that despite being able to scale up to hundreds of nodes, SpookyStuff can only approximate linear speed gain (proportional to parallelism) if there is no other bottleneck, namely, your concurrent access should be smoothly handled by the web services being queried (e.g. brokered by a CDN or load balancer) and your cluster's network topology. Otherwise blindly increasing the size of your cluster will only yield diminishing return. Please refer to [Deploying Guide] for more recommended options on cluster mode.

#### Performance

# Profiling

SpookyStuff has a metric system based on Spark's [Accumulator](https://spark.apache.org/docs/latest/programming-guide.html#AccumLink), it can be accessed from **metrics** property under the SpookyContext:

    println(rows.spooky.metrics.toJSON)
    ...

By default each query keep track of its own metric, if you would like to have all metrics of queries from the same SpookyContext to be aggregated, simply set **conf.shareMetrics** under SpookyContext to *true*.

# How to contribute

- Issue Tracker: [https://github.com/tribbloid/spookystuff/issues](https://github.com/tribbloid/spookystuff/issues)

- GitHub Repository: [https://github.com/tribbloid/spookystuff](https://github.com/tribbloid/spookystuff)

- Mailing list: Missing

# License

Copyright &copy; 2014 by Peng Cheng @tribbloid, Sandeep Singh @techaddict, Terry Lin @ithinkicancode, Long Yao @l2yao and contributors.

Published under ASF License, see LICENSE.