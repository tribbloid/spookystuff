SpookyStuff
===========

... is a scalable query engine for web scraping/data mashup/acceptance QA. The goal is to allow the Web being queried and ETL'ed like a relational database.

**SpookyStuff** is the fastest big data collection engine in history, with a speed record of querying 330404 dynamic pages per hour on 300 cores.

Powered by
-----------
- Apache Spark
- Selenium
    - GhostDriver/PhantomJS
- JSoup
- Apache Tika
- (build by) Apache Maven
    - Scala/ScalaTest plugins
- (deployed by) Ansible
- Current implementation is influenced by Spark SQL and Mahout Sparkbinding.

![Apache Spark](http://spark.apache.org/images/spark-logo.png) ![Selenium](http://docs.seleniumhq.org/images/big-logo.png) ![PhantomJS](http://phantomjs.org/img/phantomjs-logo.png)

![Apache Tika](http://tika.apache.org/tika.png) ![Build by Apache Maven](http://maven.apache.org/images/logos/maven-feather.png) ![Ansible](https://support.ansible.com/system/logos/2070/1448/ansible_logo.png)

Demo
-----------
[Click me](http://ec2-54-183-195-216.us-west-1.compute.amazonaws.com:8888/notebooks/all_inclusive_do_not_create_new_notebook.ipynb) for a quick impression.

This environment is deployed on a Spark cluster with 8+ cores. It may not be accessible during system upgrade or maintenance. Please contact a committer/project manager for a customized demo.

Examples
-----------

For a complete list of examples please refer to [source code page](https://github.com/tribbloid/spookystuff/tree/master/example/src/main/scala/org/tribbloid/spookystuff/example)

Performance
---------------

- In the above University Logo example test run, each single r3.large instance (2 threads, 15g memory) achieved 410k/s download speed in average with 45% CPU usage. Thus, the entire 4-node, 8-thread cluster is able to finish the job in 13 minutes by downloading 1279M of data, including 1271M by browsers (no surprise, GoogleImage shows a hell lot of images on page 1!) and 7.7M by direct HTTP GET.
    - Query speed can be further improved by enabling over-provisioning of executors per thread (since web agents are idle while waiting for responses). For example, allowing 4 executors to be run on each r3.large node can double CPU usage to ~90%, thus potentially doubling your query speed to 820k/s. However, this tuning will be ineffective if network bandwidth has been reached.

- We haven't tested but many others' Spark test run that involves HTTP client (e.g. querying a distributed Solr/ElasticSearch service) and heterogeneous data processing has achieved near-linear scalability under 150 nodes (theoretically, a speedup of x900 comparing to conventional single-browser scrapping! assuming you are just using r3.large instance). Massive Spark clusters (the largest in history being 1000 nodes) has also been experimented in some facilities but their performances are still unknown.

- Using Wget (equivalent to simple HTTP GET) instead of Visit for static/non-interactive pages in your Action Plan can save you a lot of time and network throughput in query as it won't start the browser and download any resources for the page.

- Further optimization options may include switching to [Kryo serializer](https://code.google.com/p/kryo/) (to replace Java serializer) and [YARN (Hadoop 2 component)](http://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html) (to replace Spark Standalone Master), however these options are not tested yet. So we encourage you to test these options and post any performance issue/bug you encountered, but not using them in production.

Usage
-----------

Current implementation only supports Language INtegrated Query (LINQ), APIs are not finalized and may change anytime in the future. Support for SQL is on the roadmap but may be abandoned in favour of simplicity.

Each query is a combination of 3 parts: Context, Action Plan and Extraction.

**Context** represents input and output data of a scraping job in key-value format. They are always created as strings or key-value pairs, being carried by other entities as metadata through a query's lifespan.

Creating of **Context** can use any Spark parallelization or transformation (though this is rarely used), e.g.:
```
- sc.parallelize("Metallica","Megadeth","Slayer","Anthrax")

- sc.parallelize(Map("first name"->"Taylor","last name"=>"Swift"), Map("first name"->"Avril","last name"->"Lavigne"))

- sc.parallelize("Taylor\tSwift", "Avril\tLavigne").csvToMap("first name\tlast name", "\t")

- sc.fromTextFile("products.txt")

- noInput(this creates a query entry point with no context)
```

**Action Plan** always has the following format:
```
(**Context** +> Action1 +> Action2 +> ... +> ActionN !)
```

These are the same actions a human would do to access the data page, their order of execution is identical to that they are defined.

**Actions** have 3 types:

- *Export*: Export a page from a browser or client, the page an be any web resource including HTML/XML file, image, PDF file or JSON string.

- *Interactive*: Interact with the browser (e.g. click a button or type into a search box) to reach the data page, all interactive executed before a page will be logged into that page's backtrace.

- *Container*: Only for complex workflow control, each defines a nested/non-linear subroutine that may or may not be executed once or multiple times depending on situations.

Many Actions supports **Context Interpolation**: you can embed context reference in their constructor by inserting context's keys enclosed by `#{}`, which will be automatically replaced with values they map to in runtime. This is used almost exclusively in typing into a textbox.

For more information on Actions and Action Plan usage, please refer to the scaladoc of ClientAction.scala and ActionPlanRDDFunction.scala respectively.

**Extraction** defines a transformation from Pages (including immediate pages from Action Plans and their link connections -- see *join/left-join*) to relational data output. This is often the goal and last step of data collection, but not always -- there is no constraint on their relative order, you can reuse extraction results as context to get more data on a different site, or feed into another data flow implemented by other components of Apache Spark (Of course, only if you know them).

Functions in **Extraction** have four types:

- *extract*: Extract data from Pages by using data's enclosing elements' HTML/XML/JSON selector(s) and attribute(s) as anchor points.

- *save/dump*: Save all pages into a file system (HDD/S3/HDFS).

- *select*: Extract data from Pages and insert them into the pages' respective context as metadata.

- *join*: This is similar to the notion of join in relational databases, except that links between pages are used as foreign keys between tables. (Technical not just links, but anything that infers a connection between web resources, including frames, iframes, sources and redirection).

For more information on Extraction syntax, please refer to the scaladoc of Page.scala and PageRDDFunction.scala.

Installation & Deployment
---------------

###### Please refer to [SpookyOps readme](https://github.com/tribbloid/spookyops/blob/master/README.md)

Integration Test
----------------

	mvn integration-test

License
-----------

Copyright &copy; 2014 by Peng Cheng @tribbloid, Sandeep Singh @techaddict, Terry Lin @ithinkicancode, Long Yao @l2yao and contributors.

Published under ASF License, see LICENSE.
