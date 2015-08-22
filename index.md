---
layout: global
title: Overview
---

* This will become a table of contents (this text will be scraped).
{:toc}

**SpookyStuff** is a scalable and lightweight query engine for web scraping/data mashup/acceptance QA. The goal is to allow remote resources to be linked and queried like a relational database. Its currently implementation is influenced by Spark [SQL](http://spark.apache.org/sql/) and [Machine Learning Pipeline](https://databricks.com/blog/2015/01/07/ml-pipelines-a-new-high-level-api-for-mllib.html).

SpookyStuff is the fastest big data collection engine in history, with a speed record of searching 330404 terms in an hour with 300 browsers.

# Powered by

![Apache Spark](http://spark.apache.org/images/spark-logo.png) ![Selenium](http://docs.seleniumhq.org/images/big-logo.png) ![PhantomJS](http://phantomjs.org/img/phantomjs-logo.png) ![Apache Tika](http://tika.apache.org/tika.png) ![Apache Maven](http://maven.apache.org/images/logos/maven-feather.png) ![Apache Http Components](https://hc.apache.org/images/logos/httpcomponents.png)

# Installation

*Main article: [Installation](deploying.html#installation)*

Why so complex? All you need is is a single library in your Java/Scala classpath:

    groupId: org.tribbloid.spookystuff
    artifactId: spookystuff-assembly_2.10
    version: {{site.STABLE_VERSION}}

You have 3 options: download it, build it or let a dependency manager (Apache Maven, sbt, Gradle etc.) do it for you.

#### Direct Download

<div class="table" markdown="1">

|  | Stable ({{site.STABLE_VERSION}}) | Nightly ({{site.NIGHTLY_VERSION}}) |
| ------------- | ------------------------ | -------------- |
| Library | [Download .jar](https://s3-us-west-1.amazonaws.com/spooky-bin/spookystuff/spark-{{site.SPARK_VERSION0}}-scala-2.10/spookystuff-assembly-{{site.STABLE_VERSION}}.jar) | [Download .jar](https://s3-us-west-1.amazonaws.com/spooky-bin/spookystuff/spark-{{site.SPARK_VERSION0}}-scala-2.10/spookystuff-assembly-{{site.NIGHTLY_VERSION}}.jar) |
| Bundled with Spark {{site.SPARK_VERSION0}} | [Download .zip](https://s3-us-west-1.amazonaws.com/spooky-bin/spookystuff/spark-{{site.SPARK_VERSION0}}-scala-2.10/spookystuff-assembly-{{site.STABLE_VERSION}}-bin-spark{{site.SPARK_VERSION0}}.zip) | [Download .zip](https://s3-us-west-1.amazonaws.com/spooky-bin/spookystuff/spark-{{site.SPARK_VERSION0}}-scala-2.10/spookystuff-assembly-{{site.NIGHTLY_VERSION}}-bin-spark{{site.SPARK_VERSION0}}.zip) |
| Bundled with Spark {{site.SPARK_VERSION1}} | [Download .zip](https://s3-us-west-1.amazonaws.com/spooky-bin/spookystuff/spark-{{site.SPARK_VERSION1}}-scala-2.10/spookystuff-assembly-{{site.STABLE_VERSION}}-bin-spark{{site.SPARK_VERSION1}}.zip) | [Download .zip](https://s3-us-west-1.amazonaws.com/spooky-bin/spookystuff/spark-{{site.SPARK_VERSION1}}-scala-2.10/spookystuff-assembly-{{site.NIGHTLY_VERSION}}-bin-spark{{site.SPARK_VERSION1}}.zip) |

</div>

This pre-built JAR/bundle provide full functionality out-of-the-box, however you need a Apache Spark installation first (including integrated Spark environment, e.g. Notebooks in [databricks™ Cloud](https://databricks.com/product/databricks) or [Apache Zeppelin](https://zeppelin.incubator.apache.org/)). If you haven't done so, please refer to [Apache Spark installation Guide](https://spark.apache.org/docs/latest/cluster-overview.html) or [Integration Section](more.html#integration).

<!---
Alternatively you can download the all-inclusive bundle, this distribution is bundled with Spark and also contains shell scripts to launch examples and spooky-shell (a minimalistic interactive shell that has SpookyStuff pre-loaded):

- [Download all-inclusive bundle for Spark 1.3.1]
- [Download all-inclusive bundle for Spark 1.4.1]
-->

#### As a Dependency

if you want to use SpookyStuff as a library in your source code, the easiest way is to let your dependency manager (e.g. Apache Maven, sbt, gradle) to download it automatically from the [Maven Central Repository](http://search.maven.org/), by adding the following artifact reference into your build definition:

<div class="codetabs">

<div data-lang="Maven">

{% highlight xml %}
<dependency>
    <groupId>org.tribbloid.spookystuff</groupId>
    <artifactId>spookystuff-core_2.10</artifactId>
    <version>{{site.STABLE_VERSION}}</version>
</dependency>
{% endhighlight %}

</div>

<div data-lang="SBT">

{% highlight scala %}
libraryDependencies += "org.tribbloid.spookystuff" % "spookystuff-core_2.10" % "{{site.STABLE_VERSION}}"
{% endhighlight %}

</div>

<div data-lang="Gradle">

{% highlight groovy %}
'org.tribbloid.spookystuff:spookystuff-core_2.10:{{site.STABLE_VERSION}}'
{% endhighlight %}

</div>

<div data-lang="Leiningen">

{% highlight clojure %}
[org.tribbloid.spookystuff/spookystuff-core_2.10 "{{site.STABLE_VERSION}}"]
{% endhighlight %}

</div>

</div>

Many integrated Spark environments (e.g. Spark-Shell, [databricks™ Cloud](https://databricks.com/product/databricks) or [Apache Zeppelin](https://zeppelin.incubator.apache.org/)) has built-in dependency manager, which makes deployment much easier by eliminating the necessity of manual download. This is again covered in [Integration Section](more.html#integration).

#### Sourcecode Download

If you are good with programming and prefer to build it from scratch:

<div class="table" markdown="1">

| Stable ({{site.STABLE_VERSION}}) | Nightly ({{site.NIGHTLY_VERSION}}) |
| ------------ | ----------- |
| [Download .zip](https://github.com/tribbloid/spookystuff/zipball/release-{{site.STABLE_VERSION}}) | [Download .zip](https://github.com/tribbloid/spookystuff/zipball/master) |
| [Download .tar.gz](https://github.com/tribbloid/spookystuff/tarball/release-{{site.STABLE_VERSION}}) | [Download .tar.gz](https://github.com/tribbloid/spookystuff/tarball/master) |

</div>

For how to build from sourcecode, please refer to [Build Section](dev.html#build).

# Quick Start

First, make sure Spark is working under your favorite IDE/REPL:

{% highlight scala %}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

// you don't need these if SparkContext has been initialized
// val conf = new SparkConf().setAppName("SpookyStuff example").setMaster("local[*]")
// val sc = new SparkContext(conf)

assert(sc.parallelize(1 to 100).reduce(_ + _) == 5050)
{% endhighlight %}

Next, import and initialize a SpookyContext (this is the entry point of all language-integrated queries, much like SQLContext for Spark SQL):

{% highlight scala %}
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.dsl._

//this is the entry point of all queries & configurations
val spooky = new org.tribbloid.spookystuff.SpookyContext.SpookyContext(sc)
import spooky.dsl._
{% endhighlight %}

From this point you can run queries on public datasets immediately. The following is a minimalistic showcase on cross-site "join", one of the 5 main clauses:

{% highlight scala %}
spooky.wget("https://news.google.com/?output=rss&q=barack%20obama"
).join(S"item title".texts)(
    Wget(x"http://api.mymemory.translated.net/get?q=${'A}&langpair=en|fr")
)('A ~ 'title, S"translatedText".text ~ 'translated).toDF()
{% endhighlight %}

You will get a list of titles of English news about BHO and their respective french translations:

![news about BHO and their respective french translations](img/BHOnews_translation.png)

<!-- Wondering what it does in 4 lines? Here is a simple breakdown: -->

For more information on query syntax and usage, please go to [Query Guide](query.html).

# Web Caching

You may already notice that repeatedly running a query takes much less time than running it for the first time. this is because all web resources are cached: cached resources are loaded directly from a file directory (can be on any Hadoop-supported file system, namely HDD, HDFS, Amazon S3 and Tachyon etc.) if they haven't expired. Unlike browsers or most search engines, SpookyStuff also caches dynamic and script-generated contents.

RDD cache is enabled by default to facilitate repeated data wrangling and dry run. To disable it, simply set **spooky.conf.cacheRead** = false or set **spooky.conf.pageExpireAfter** to a very small duration:

{% highlight scala %}
import scala.concurrent.duration._

spooky.conf.cacheRead = false // OR
spooky.conf.pageExpireAfter = 1.minute
{% endhighlight %}

However, before you run a query, it is recommended to point the web cache directory to a publicly-accessible, high-available storage URL (e.g. starting with ```hdfs://``` or ```s3n://```). Otherwise SpookyStuff will use *{Java-working-directory}/temp/cache* on local file system by default, which means if your query is running on a cluster, it will have a chance not able to use an already cached resource because it's on another machine. This directory can be set by **spooky.conf.dirs.cache**, which affects execution of all queries derived from it:

{% highlight scala %}
spooky.conf.dirs.cache = "hdfs://spooky-cache"
{% endhighlight %}

Or you can override the default web cache directory globally by setting **spooky.cache** system property in your Java option:

- if your query is launched from a standalone Java application:

{% highlight bash %}
-Dspooky.cache=hdfs://spooky-cache
{% endhighlight %}

- OR, if your query is launched by spark-submit.sh

{% highlight bash %}
--conf spooky.cache=hdfs://spooky-cache
{% endhighlight %}

For more performance optimization options, please go to [Configuration Section](deploying.html#configuration).

# Scaling

SpookyStuff is optimized for running on Spark [cluster mode](cluster-overview.html), which accelerates execution by parallelizing over multiple machine's processing power and network bandwidth.

It should be noted that despite being able to scale up to hundreds of nodes, SpookyStuff can only approximate linear speed gain (speed proportional to parallelism) if there is no other bottleneck, namely, your concurrent access should be smoothly handled by the web services being queried (e.g. brokered by a CDN or load balancer) and your cluster's network topology. Otherwise blindly increasing the size of your cluster will only yield diminishing return. Please refer to [Scaling Section](deploying.html#scaling) for more recommended options on cluster mode.

#### Performance

# Profiling

SpookyStuff has a metric system based on Spark's [Accumulator](https://spark.apache.org/docs/latest/programming-guide.html#AccumLink), which can be accessed with **spooky.metrics**:

{% highlight scala %}
println(rows.spooky.metrics.toJSON)
{% endhighlight %}

By default each query keep track of its own metric, if you would like to have all metrics of queries from the same SpookyContext to be aggregated, simply set **spooky.conf.shareMetrics** = true.

# How to contribute

- Issue Tracker: [https://github.com/tribbloid/spookystuff/issues](https://github.com/tribbloid/spookystuff/issues)

- GitHub Repository: [https://github.com/tribbloid/spookystuff](https://github.com/tribbloid/spookystuff)

- Mailing list: Missing

# License

Copyright &copy; 2014 by Peng Cheng @tribbloid, Sandeep Singh @techaddict, Terry Lin @ithinkicancode, Long Yao @l2yao and contributors.

Supported by [tribbloids®](http://tribbloid.github.io/)

Published under [ASF License v2.0](http://www.apache.org/licenses/LICENSE-2.0).