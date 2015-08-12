---
layout: global
title: Deploying
---

* This will become a table of contents (this text will be scraped).
{:toc}

# Installation

Why so complex? All you need is is a single library in your Java/Scala classpath:

    groupId: org.tribbloid.spookystuff
    artifactId: spookystuff-assembly_2.10
    version: {{site.STABLE_VERSION}}

You have 3 options: download it, build it or let a dependency manager (Apache Maven, sbt, Gradle etc.) do it for you.

#### Direct Download

<div class="table" markdown="1">

|  | Stable ({{site.STABLE_VERSION}}) | Nightly ({{site.NIGHTLY_VERSION}}) |
| ------------- | ------------------------ | -------------- |
| Library | [Download .jar](https://s3-us-west-1.amazonaws.com/spooky-bin/spookystuff-assembly-{{site.STABLE_VERSION}}.jar) | [Download .jar](https://s3-us-west-1.amazonaws.com/spooky-bin/spookystuff-assembly-{{site.NIGHTLY_VERSION}}.jar) |
| Bundled with Spark {{site.SPARK_VERSION0}} | [Download .zip](https://s3-us-west-1.amazonaws.com/spooky-bin/spookystuff-assembly-{{site.STABLE_VERSION}}-bin-spark{{site.SPARK_VERSION0}}.zip) | [Download .zip](https://s3-us-west-1.amazonaws.com/spooky-bin/spookystuff-assembly-{{site.NIGHTLY_VERSION}}-bin-spark{{site.SPARK_VERSION0}}.zip) |
| Bundled with Spark {{site.SPARK_VERSION1}} | [Download .zip](https://s3-us-west-1.amazonaws.com/spooky-bin/spookystuff-assembly-{{site.STABLE_VERSION}}-bin-spark{{site.SPARK_VERSION1}}.zip) | [Download .zip](https://s3-us-west-1.amazonaws.com/spooky-bin/spookystuff-assembly-{{site.NIGHTLY_VERSION}}-bin-spark{{site.SPARK_VERSION1}}.zip) |

</div>

This pre-built JAR/bundle provide full functionality out-of-the-box, however you need a Apache Spark installation first (including integrated Spark environment, e.g. Notebooks in [databricks™ Cloud](https://databricks.com/product/databricks) or [Apache Zeppelin](https://zeppelin.incubator.apache.org/)). If you haven't done so, please refer to [Apache Spark installation Guide](https://spark.apache.org/docs/latest/cluster-overview.html) or [Integration Section](more.html#integration).

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

#### Optional Components

SpookyStuff natively supports 2 headless browers for deep web access: [HtmlUnit](http://htmlunit.sourceforge.net/) and the much more capable [PhantomJS](http://phantomjs.org/). The binary executable of PhantomJS is NOT included in the pre-built JAR, and if it's' not detected on all Spark workers, it will be automatically downloaded from a PhantomJS mirror. To avoid repeated download you can download a permanent copy to each Spark worker:

- on linux [https://s3-us-west-1.amazonaws.com/spooky-bin/phantomjs-linux/phantomjs](https://s3-us-west-1.amazonaws.com/spooky-bin/phantomjs-linux/phantomjs)

And tell SpookyStuff to use them by setting its environment variable **PHANTOMJS_PATH** or system property **phantomjs.binary.path** to its full installation path (you don't need to set it on Spark workers). Next time SpookyStuff will use them instead of download. It should be noted that all PhantomJS paths on workers should be identical, otherwise it is impossible to set SpookyStuff to use all of them.

Another optional component for better deep web compatibility is [TOR client](https://www.torproject.org/about/overview.html.en), it can be used by SpookyStuff as a socks5 proxy (both http and socks proxies are supported by SpookyStuff). Installation of TOR client is OS-specific, users are advised to follow [this instruction](https://www.torproject.org/download/download-easy.html.en) to install and launch TOR client on their Spark workers.

# Configuration

The following options can be set independently and dynamically for each SpookyContext (by changing its **conf.{option-name}** property in scala), or set collectively by environment variables and/or system properties. If multiple values for an option are set by different methods (e.g. environment variable *SPOOKY_CACHE* and system property *spooky.cache* are set to different values), The preceding value in the following list will override others.

<div class="table" markdown="1">

| Dynamic/Programmatic | > | System Property | > | Environment Variable | > | Default |

</div>

#### Web Caching

<div class="table" markdown="1">

| Name | System Property | Environment Variable | Default | Meaning |
| ---- | --------------- | -------------------- | ------- | ------- |
| cacheWrite | N/A | N/A | true | If web resources should be cached. Local resources (from URI starting with ```file://, hdfs:// or s3://```) won't be cached |
| cacheRead | N/A | N/A | true | If web resources should be attempted to be restored from web cache with much lower costs instead of being fetched remotely |
| pageExpireAfter | N/A | N/A | 7.days | Cached resources older than this duration are deemed 'expired' and won't be restored |
| pageNotExpiredSince | N/A | N/A | None | Cached resources with a timestamp older than this are deemed 'expired' and won't be restored |
| dirs.cache | spooky.dirs.cache | N/A | {dirs.root}/cache | URI of the directory for web caching, the URI format should be like ```scheme://authority/path``` (read [this article](https://hadoop.apache.org/docs/r2.4.1/hadoop-project-dist/hadoop-common/FileSystemShell.html) for details). Users may want to set this to a unified location like an HDFS directory so all Spark workers and applications can reuse cached resources |

</div>

#### Query Optimization

<div class="table" markdown="1">

| Name | System Property | Environment Variable | Default | Meaning |
| ---- | --------------- | -------------------- | ------- | ------- |
| defaultQueryOptimizer | N/A | N/A | Wide | Set default Query Optimizer to one of the 3 options: Narrow, Wide, or Wide_RDDWebCache, Query Optimizer affects how duplicate & unnecessary remote access (e.g. Crawling a directory with diamond links) are handled before being distributed and executed, The meaning of these options are described in the following table |
| defaultParallelism | N/A | N/A | 8*{number-of-cores} | Default number of partitions for all RDDs generated by SpookyStuff queries. parallel operations depending on remote access are highly skewed and it is generally a good practice to set the parallelism a few times larger than common Spark tasks |
| defaultStorageLevel | N/A | N/A | MEMORY_ONLY | Default storage level of "temporarily" persisted RDDs during queries' execution. These persisted RDDs takes large chunks of memory to facilitate complex query optimization but are usually evicted immediately beyond their intended usage. If your Spark cluster frequently encounter memory overflow issue, try setting this to *MEMORY_ONLY_SER* or *DISK_ONLY*, please refer to [Spark Storage Level] article for details |
| checkpointInterval | N/A | N/A | 100 | Like [Spark Streaming] and [MLlib], [exploring link graph] in SpookyStuff is an iterative process and relies on periodic RDD checkpointing to recover from failure and avoid very long dependency graph. If this is set to a positive integer, RDD with a dependency chain longer than this will be checkpointed to a directory defined by **dirs.checkpoint** |
| dirs.checkpoint | spooky.dirs.checkpoint | N/A | {dirs.root}/checkpoint | URI of the directory for checkpointing, the URI format should be like ```scheme://authority/path``` (read [this article](https://hadoop.apache.org/docs/r2.4.1/hadoop-project-dist/hadoop-common/FileSystemShell.html) for details) |

</div>

##### Query Optimizer Options

<div class="table" markdown="1">

| Query Optimizer | Meaning |
| --------------- | ------- |
| Narrow | Try to minimize shuffling and number of stages by avoiding "wide" transformations (grouping, cogrouping): this means duplicated resources in different partitions are fetched as-is and efficient execution relies mostly on web caching, only recommended if you know that chance of duplicates across multiple threads are low, or shuffling costs are high due to heavy iterations (e.g. exploring pagination though links) |
| Wide | Aggressively merge duplicate actions before each execution and cast fetched results into multiple rows afterwards: this is often the most efficient tradeoff in terms of total cost, with the only notable caveat being exploring deeply through hyperlinks: merging actions across multiple partitions takes at least one wide transformations which may become expensive in iterative graph exploring. The tradeoff between Wide and Narrow Optimizers becomes tricky if both duplication and heavy iterations co-exist, we will gradually improve our query optimizer to be more adaptive.
| Wide_RDDWebCache | Include all optimization measures in Wide optimizer, plus an indexed RDD is assigned to each query as an in-memory web cache. reading/writing this cache is faster than the web cache on file system but also eats a lot of memory (similar to an L1-cache as opposed to FS-based L2-cache). RDD web caches are NOT shared between queries and are usually scraped after the query finished execution |

</div>

* RDDWebCache is an experimental feature that should be used tentatively, RDDs are designed to be immutable and using this optimizer to fetch big dataset may easily results in memory overflow. In addition, some Hadoop-compatible file systems, e.g. [Tachyon](http://tachyon-project.org/index.html) and [Apache Ignite](https://ignite.incubator.apache.org/), may already achieved in-memory speed which renders RDDWebCache non-competitive.

#### Failover

<div class="table" markdown="1">

| Name | System Property | Environment Variable | Default | Meaning |
| ---- | --------------- | -------------------- | ------- | ------- |
| remoteResourceTimeout | N/A | N/A | 60.seconds | Max waiting duration of fetching a remote resource (including loading dynamic content in browsers) before the connection is retried |
| DFSTimeout | N/A | N/A | 40.seconds | Max waiting duration of a file system I/O (e.g. reading/updating the web cache) before the connection is retried |
| failOnDFSError | N/A | N/A | false | Whether to fail fast if a file system I/O failed many times and can be circumvented otherwise (e.g. web cache access, in worst case SpookyStuff can smoothly fall back to fetching remotely), setting this option to true if you are using an unstable HDFS/S3 |
| errorDump | N/A | N/A | true | Whether to perform a session buffer dump on web client exception |
| errorScreenshot | N/A | N/A | true | Whether to take a screenshot of a browser viewport on its exception, effective only for browsers supporting screenshot, in this version phantomJS is the only option supporting this feature |
| dirs.errorDump | spooky.dirs.errordump | N/A | {dirs.root}/cache | URI of the directory for error dump |
| dirs.errorScreenshot | spooky.dirs.errorscreenshot | N/A | {dirs.root}/cache  URI of the directory for error screenshot |
| dirs.errorDumpLocal | spooky.dirs.errordump.local | N/A | {dirs.root}/cache | if **dirs.errorDump** is not accessible, use this directory as a backup |
| dirs.errorScreenshotLocal | spooky.dirs.errorscreenshot.local | N/A | {dirs.root}/cache | if **dirs.errorScreenshot** is not accessible, use this directory as a backup |

</div>

#### Web Client

<div class="table" markdown="1">

| Name | System Property | Environment Variable | Default | Meaning |
| ---- | --------------- | -------------------- | ------- | ------- |
| driverFactory | N/A | N/A | PhantomJS | Meaning |
| proxy | N/A | N/A | NoProxy | Meaning |
| userAgent | N/A | N/A | null | Meaning |
| headers | N/A | N/A | Map() | Meaning |
| oAuthKeys | N/A | N/A | null | Meaning |
| browserResolution | N/A | N/A | 1920x1080 | Meaning |

</div>

#### Miscellaneous

<div class="table" markdown="1">

| Name | System Property | Environment Variable | Default | Meaning |
| ---- | --------------- | -------------------- | ------- | ------- |
| dirs.root | spooky.dirs.root | N/A | {java working directory}/temp | Meaning |
| autoSave | N/A | N/A | true | Meaning |
| dirs.autoSave | spooky.dirs.autosave | N/A | {dirs.root}/cache | Meaning |
| shareMetrics | N/A | N/A | false | Meaning |
| defaultJoinType | N/A | N/A | LeftOuter | Meaning |
| maxJoinOrdinal | N/A | N/A | 2^31-1 | Meaning |
| maxExploreDepth | N/A | N/A | 2^31-1 | Meaning |

</div>

# Scaling

SpookyStuff is optimized for running on Spark [cluster mode](cluster-overview.html), which accelerates execution by parallelizing over multiple machine's processing power and network bandwidth, in most cases this is highly recommended in production, and the only feasible way for querying/enriching big dataset. However, it is important to understand the following fact and ensure that your query execution's compliance with your web service providers, fail to understand the consequence may results in your API key being banned or being prosecuted.

- Despite being able to scale up to hundreds of nodes, SpookyStuff can only approximate linear speed gain (proportional to parallelism) if there is no other bottleneck, namely, your concurrent access should be smoothly handled by the web services being queried (e.g. brokered by a CDN or load balancer) and your cluster's network topology. Otherwise blindly increasing the size of your cluster will generally yield diminishing return.

- Your API credential will be shared by multiple IP addresses of your cluster for all API calls, this may cause max out web server's connection pool and cause heavy load on their infrastructures,make sure that this is not frown upon by your API provider!

- Web cache and checkpointing directory has to be on a persistent file system, other direcotries under **dirs** setting are recommended to be there as well.

In addition: We also recommend using the following [Spark properties](http://spark.apache.org/docs/latest/configuration.html#spark-properties) for better performance:

- **spark.task.maxFailures=100** (or any sufficiently high number): external web services are less stable than in-house web services, so make sure SpookyStuff can retry many times from multiple machines to overcome service downtime and connection error. It should be noted that retry is partition-wise, so make sure your web cache is enabled to avoid unnecessary fetch.

- **spark.serializer=org.apache.spark.serializer.KryoSerializer**: Shuffling and broadcasting over mutiple machines are much more expensive so its time to enable the more efficient [Kryo serializer].

- **spark.kryoserializer.buffer.max=512m** (or a size enough to handle your largest partition): The default value of 64m may be unable to handle large files, increase it if you ran into KryoException.

- **spark.kryo.registrator=org.tribbloid.spookystuff.SpookyRegistrator**: Not necessary but can be helpful in reducing Serialization size.

# More Information

Like other Spark applications, SpookyStuff can benefits from many other option Spark offers, please refer to please refer to [Spark configuration](https://spark.apache.org/docs/latest/configuration.html) and [Spark Tuning Guide](http://spark.apache.org/docs/latest/tuning.html) for more options.

#### 1-Line installation with [Ansible](http://www.ansible.com/home)

The following section propose a fully automated install routine using [Ansible](http://www.ansible.com/home), its only tested on Debian-like Linux OS. Please ignore this section if your Spark environment is not using Debian, Ubuntu or their variants.