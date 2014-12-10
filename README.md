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
[Click me](http://ec2-54-165-231-62.compute-1.amazonaws.com:8888/notebooks/all_inclusive_do_not_create_new_notebook.ipynb) for a quick impression.

This environment is deployed on a Spark cluster with 8+ cores. It may not be accessible during system upgrade or maintenance. Please contact a committer/project manager for a customized demo.

How it works
-----------
- In a nutshell, **SpookyStuff** scales up data collection by distributing web clients to many machines. Each of them receives a portion of heterogeneous tasks and run them independently. After that, their results can either be transformed and reused to dig deeper into the web by visiting more dynamic pages, or be exported into one of many data storage: including local HDD, HDFS, Amazon S3, or simply Memory block in JVM.

- **SpookyStuff** is extremely lightweight by offloading most of the task scheduling & data transformation work to Apache Spark. It doesn't depend on any file system (even HDFS is optional), backend database, or message queue, or any SOA. Your query speed is only bounded by your bandwidth and CPU power.

- **SpookyStuff** use phantomjs/GhostDriver to access dynamic pages and mimic human interactions with them, but it doesn't render them - nor does it download any image embedded in them by default (unless you take a screenshot), which makes it still considerably faster even on a single machine.

- **SpookyStuff**'s query language is an extension of Spark API, there is no problem in mixing it with other Spark-based environments, notably SparkSQL and MLlib.

Examples
-----------

For a complete list of examples please refer to [source code page](https://github.com/tribbloid/spookystuff/tree/master/example/src/main/scala/org/tribbloid/spookystuff/example)

#### 1. Search on LinkedIn
- Goal: Find high-ranking professionals in you area on [http://www.linkedin.com/], whose first name is either 'Sanjay', 'Arun' or 'Hardik', and last name is either 'Gupta' or 'Krishnamurthy', print out their full names, titles and lists of skills
- Query:
```
    (sc.parallelize(Seq("Sanjay", "Arun", "Hardik"))
      +> Visit("https://www.linkedin.com/")
      +> TextInput("input#first", "#{_}")
      +*> (TextInput("input#last", "Gupta") :: TextInput("input#last", "Krishnamurthy") :: Nil)
      +> Submit("input[name=\"search\"]")
      !=!())
      .visitJoin("ol#result-set h2 a")()
      .extract (
      "name" -> (_.text1("span.full-name")),
      "title" -> (_.text1("p.title")),
      "skills" -> (_.text("div#profile-skills li"))
    ).asSchemaRDD()
```
- Result (truncated, finished in 1 minutes on a laptop with ~400k/s wifi):
```
Arun	Arun Gupta	Consultant , Global Canesugar services Pvt.Ltd.	ArrayBuffer(Filtration, Project Planning, Energy, Renewable Energy, Engineering, Power Generation, Procurement, Biofuels, Gas, Instrumentation, DCS, Design & Developments, Boilers, Commissioning, Construction, EPC, Energy Management, Factory, ISO, Maintenance Management, Manufacturing, Materials, Negotiation, Operations Management, PLC, Petrochemical, Power Plants, Process Control, Process Engineering, Product Development, MS Project, Project Engineering, Project Management, Pumps, R&D, Refinery, SAP, Steam Turbines, Supply Chain Management, Turbines, Water Treatment)
Sanjay	Sanjay Gupta	Researcher at REC ltd	ArrayBuffer(Internet Recruiting, Candidate Generation, Passive Candidate Generation, Research, Databases, Data Mining, Data Entry)
Arun	Arun Krishnamurthy	Global Incident Manager at IFF	ArrayBuffer(Service Delivery Management, Management, Service Delivery, Transition Management, IT Service Management, Sla, ITIL, Vendor Management, Team Management, Business Analysis, Incident Management, Technical Support, Business Process, Resource Management)
... -------------------returned 116 rows------------------
```

#### 2. Query the Machine Parts Database of AppliancePartsPros
- Goal: Given a washing machine model 'A210S', search on [http://www.appliancepartspros.com/] for the model's full name,  a list of schematic descriptions (with each one describing a subsystem), for each schematic, search for data of all enumerated machine parts: description, manufacturer, OEM number, and a list of each one's substitutes. Join them all together and print them out.
- Query:
```
    (sc.parallelize(Seq("A210S")) +>
      Visit("http://www.appliancepartspros.com/") +>
      TextInput("input.ac-input","#{_}") +>
      Click("input[value=\"Search\"]") +>
      Delay(10.seconds) !=!() //TODO: change to DelayFor to save time
      )
      .extract(
        "model" -> ( _.text1("div.dgrm-lst div.header h2") )
      )
      .wgetJoin("div.inner li a:has(img)")()
      .extract("schematic" -> {_.text1("div#ctl00_cphMain_up1 h1")})
      .wgetJoin("tbody.m-bsc td.pdct-descr h2 a")()
      .extract(
        "name" -> (_.text1("div.m-pdct h1")),
        "brand" ->  (_.text1("div.m-pdct td[itemprop=brand]")),
        "manufacturer" -> (_.text1("div.m-bsc div.mod ul li:contains(Manufacturer) strong")),
        "replace" -> (_.text1("div.m-pdct div.m-chm p"))
      )
      .asSchemaRDD()
```
- Result (truncated, process finished in 2 minutes on one r3.large instance):
```
A210S	A210S Washer-Top Loading 	Parts for Maytag A210S: Top Cover\console\lid Switch Parts	Moisture Barrier for Switch	Whirlpool	214987	Part Number 214987 (AP4025452) replaces 2-14987, 438912, AH2018922, EA2018922, PS2018922.
A210S	A210S Washer-Top Loading 	Parts for Maytag A210S: Transmissions Parts	Gear, Pwr Kit	Whirlpool	204967	Part Number 204967 (AP4023771) replaces 2-13068, 2-13069, 2-4967, 213068, 213069, 214278, 435155, AH2017141, EA2017141, PS2017141.
A210S	A210S Washer-Top Loading 	Parts for Maytag A210S: Transmissions Parts	Gear, Pwr Kit	Whirlpool	204967	Part Number 204967 (AP4023771) replaces 2-13068, 2-13069, 2-4967, 213068, 213069, 214278, 435155, AH2017141, EA2017141, PS2017141.
... -------------------returned 293 rows------------------
```

#### 3. Download University Logos
- Goal: Search for Logos of all US Universities on Google Image (a list of US Universities can be found at [http://www.utexas.edu/world/univ/alpha/]), download them to one of your s3 bucket.
    - You need to set up your S3 credential through environment variables
    - The following query will visit 4000+ pages and web resources so its better to test it on a cluster
- Query:
```
    ((noInput
      +> Visit("http://www.utexas.edu/world/univ/alpha/")
      !=!())
      .sliceJoin("div.box2 a")()
      .extract(
        "name" -> (_.text1("*"))
      )
      .repartition(400)
      +> Visit("http://images.google.com/")
      +> DelayFor("form[action=\"/search\"]")
      +> TextInput("input[name=\"q\"]","Logo #{name}")
      +> Submit("input[name=\"btnG\"]")
      +> DelayFor("div#search")
      !=!())
      .wgetJoin("div#search img","src")(limit = 1)
      .saveContent(pageRow =>
      "file://"+System.getProperty("user.home")+"/spooky/"+appName+"/images/"+pageRow("name"))
      .extract(
        "path" -> (_.saved)
      )
      .asSchemaRDD()
```
- Result (process finished in 13 mintues on 4 r3.large instances, image files can be downloaded from S3 with a file transfer client supporting S3 (e.g. S3 web UI, crossFTP): 

![Imgur](http://i.imgur.com/ou6pCjO.png)

#### 4.a. ETL the product database of [http://www.iherb.com/].

- Goal: Generate a complete list of all products and their prices offered on [http://www.iherb.com/] and load into designated S3 bucket as a tsv file, also save every product page being visited as a reference.
    - The following query will download 4000+ pages and extract 43000+ items from them so its better to test it on a cluster.
- Query:
```
    (noInput
      +> Wget("http://ca.iherb.com/")
      !=!())
      .wgetJoin("div.category a")()
      .paginate("p.pagination a:contains(Next)")(indexKey = "page")
      .sliceJoin("div.prodSlotWide")(indexKey = "row")
      .extract(
        "description" -> (_.text1("p.description")),
        "price" -> (_.text1("div.price")),
        "saved" -> (_.saved)
      )
      .asSchemaRDD()
```
- Result (process finished in 6.1 mintues on 4 r3.large instances)
```
http.ca.iherb.com.Food-Grocery-Items    St. Dalfour, Wild Blueberry, Deluxe Wild Blueberry Spread, 10 oz (284 g)	$4.49
http.ca.iherb.com.Food-Grocery-Items    Eden Foods, Organic, Wild Berry Mix, Nuts, Seeds & Berries, 4 oz (113 g)	$3.76
http.ca.iherb.com.Food-Grocery-Items    St. Dalfour, Organic, Golden Mango Green Tea, 25 Tea Bags, 1.75 oz (50 g))	$3.32
... -------------------returned 42821 rows------------------
```

#### 4.b. Cross-website price comparison.

- Goal: Use the product-price list generated in 4.a. as a reference and query on [http://www.amazon.com] for possible alternative offers, generate a table containing data of the first 10 matches for each product, the format of the table is defined by:
```
 (From left to right)
 product name on iherb|product name on amazon|original price on iherb|price on amazon|shipping condition|user's rating|number of users that rated|"Do you mean" heuristic|exact match info|reference page
```
Save the table as a tsv file and keep all visited pages as a reference.
    - this query will open 43000+ browser sessions so it's recommended to deploy on a cluster with 10+ nodes, alternatively you can truncate the result in 4.a. for a dry-run.

- Query:
```
    (sc.textFile("iherb.tsv")
      .tsvToMap("url\titem\tiherb-price")
      +> Visit("http://www.amazon.com/")
      +> TextInput("input#twotabsearchtextbox", "#{item}")
      +> Submit("input.nav-submit-input")
      +> DelayFor("div#resultsCol")
      !=!())
      .extract(
        "DidYouMean" -> {_.text1("div#didYouMean a") },
        "noResultsTitle" -> {_.text1("h1#noResultsTitle")},
        "savePath" -> {_.saved}
      )
      .sliceJoin("div.prod[id^=result_]:not([id$=empty])")(limit = 10)
      .extract(
        "item_name" -> (page => Option(page.attr1("h3 span.bold", "title")).getOrElse(page.text1("h3 span.bold"))),
        "price" -> (_.text1("span.bld")),
        "shipping" -> (_.text1("li.sss2")),
        "stars" -> (_.attr1("a[alt$=stars]", "alt")),
        "num_rating" -> (_.text1("span.rvwCnt a"))
      )
      .asSchemaRDD()
```
- Result (process finished in 2.1 hours on 11 r3.large instances)
```
MusclePharm Assault Fruit Punch	Muscle Pharm Assault Pre-Workout System Fruit Punch, 0.96 Pound	$33.11	FREE Shipping on orders over $35	3.8 out of 5 stars	484	muscle pharm assault fruit punch	null	http.www.amazon.com.s.ie=UTF8&page=1&rh=i%3Aaps%2Ck%3AMusclePharm%20Assault%20Fruit%20Punch
Paradise Herbs, L-Carnosine, 60 Veggie Caps	Paradise Herbs L-Carnosine Cellular Rejuvenation, Veggie Caps 60 ea	$50.00	null	null	null	null	null	http.www.amazon.com.s.ie=UTF8&page=1&rh=i%3Aaps%2Ck%3AParadise%20Herbs%5Cc%20L-Carnosine%5Cc%2060%20Veggie%20Caps
Nature's Bounty, Acetyl L-Carnitine HCI, 400 mg, 30 Capsules	Nature's Bounty Acetyl L-Carnitine 400mg, with Alpha Lipoic Acid 200mg, 30 capsules	$15.99	FREE Shipping on orders over $35	3.6 out of 5 stars	7	null	null	http.www.amazon.com.s.ie=UTF8&page=1&rh=i%3Aaps%2Ck%3ANature%27s%20Bounty%5Cc%20Acetyl%20L-Carnitine%20HCI%5Cc%20400%20mg%5Cc%2030%20Capsules
Lansinoh, Breastmilk Storage Bags, 25 Pre-Sterilized Bags	Lansinoh Breastmilk Storage Bags, 25-Count Boxes (Pack of 3)	$13.49	FREE Shipping on orders over $35	4 out of 5 stars	727	null	null	http.www.amazon.com.s.ie=UTF8&page=1&rh=i%3Aaps%2Ck%3ALansinoh%5Cc%20Breastmilk%20Storage%20Bags%5Cc%2025%20Pre-Sterilized%20Bags
... -------------------returned 35737 rows------------------
```

#### 5 Download comments and ratings from [http://www.resellerratings.com/]

- Goal: Given a list of vendors, download their ratings and comments with timestamp from customers.

- Query:
```
    (sc.parallelize(Seq("Hewlett_Packard")) +>
      Wget( "http://www.resellerratings.com/store/#{_}") !=!())
      .paginate( "div#survey-header ul.pagination a:contains(next)")(indexKey = "page")
      .sliceJoin("div.review")()
      .extract(
        "rating" -> (_.text1("div.rating strong")),
        "date" -> (_.text1("div.date span")),
        "body" -> (_.text1("p.review-body"))
      )
      .asSchemaRDD()
```

- Result:
```
1/5	2013-09-08	"Extremely pained by the service and behaviour of HP. My Envy touch screen Ultrabook crashed 3 weeks back, which I bought in January this year. It came with preloaded Windows 8 OS and they refuse to load the OS without charging me for it. The laptop is under warranty. Best was that the reason given for the crash by the service engineer is 'Monsoons'. Never buy an HP machine. "
1/5	2013-09-04	"I can not believe how bad the service was. I wanted our company to resell HP products. I have been calling HP for weeks. Every time I call I feel like they push me to tears. When I first called the women didn't know English, she had no one else to speak to me. I called another number, an American answered, but he had no understanding of how to make a new contract but suggested he could transfer me. He did and I got a live person, until he put me on hold and the call was disconnected. I called the number on the HP partners website and they said that they are a whole other company that works for HP and don't have any idea of what to do or who to call. This continued on. It was so stressful. I mean I am so sad and stressed. I may be better off finding another company to work with. I have wasted many valuable days of work and so has my assistant. It is time consuming and costly dealing with them."
1/5	2013-09-03	"I bought a HP CM-1015 multi-function printer/scanner/copier several years ago. It worked fine when connected to a WindowsXP print server. When I upgraded the XP to Windows 7 (about three years ago), I could not find a driver for it, so I waited, waited, and waited more. Today is September 3, 2013, I installed the newest posted driver that I downloaded from the HP’s website, and it still does not work: It only prints black-and-white, not color. I am not even asking to have all the functions of the machine to work, just the printer part, is this too much for HP? I am very disappointed at the HP product and its service. I bought the HP brand because I thought I would get a great product with great service. What I got is the opposite. Now, I am looking at this almost new machine (it has not been used much in the past years) and wondering: Should I get rid of it ($500+ when purchased) and get another brand? It is really a waste of money and time."
... -------------------returned 189 rows------------------
```

#### 6 Download comments and ratings from [http://www.youtube.com/]

- Goal: Visit Metallica's youtube channel, click 'show more' button repeatedly until all videos are displayed. Collect their respective titles, descriptions, date of publishing, number of watched users, number of positive votes, number of negative votes, number of comments. Finally, click 'show more comments' button repeatedly and collect all top-level comments (not replies), also, save each fully expanded comment iframe for validation.

- Query:
```
    (((sc.parallelize(Seq("MetallicaTV")) +>
      Visit("http://www.youtube.com/user/#{_}/videos") +>
      Loop(
        Click("button.load-more-button span.load-more-text")
          :: DelayFor("button.load-more-button span.hid.load-more-loading").in(10.seconds)
          :: Nil
      ) !=!())
      .sliceJoin("li.channels-content-item")()
      .extract("title" -> (_.text1("h3.yt-lockup-title")))
      .visit("h3.yt-lockup-title a.yt-uix-tile-link")()
      .repartition(400) +>
      ExeScript("window.scrollBy(0,500)") +>
      Try(DelayFor("iframe[title^=Comment]").in(50.seconds) :: Nil)
      !><()).extract(
        "description" -> (_.text1("div#watch-description-text")),
        "publish" -> (_.text1("p#watch-uploader-info")),
        "total_view" -> (_.text1("div#watch7-views-info span.watch-view-count")),
        "like_count" -> (_.text1("div#watch7-views-info span.likes-count")),
        "dislike_count" -> (_.text1("div#watch7-views-info span.dislikes-count"))
      )
      .visit("iframe[title^=Comment]", attr = "abs:src")() +>
      Loop(
        Click("span[title^=Load]") :: DelayFor("span.PA[style^=display]").in(10.seconds) :: Nil
      ) !=!(joinType = LeftOuter))
      .extract("num_comments" -> (_.text1("div.DJa")))
      .sliceJoin("div[id^=update]")()
      .extract(
        "comment1" -> (_.text1("h3.Mpa")),
        "comment2" -> (_.text1("div.Al"))
      )
      .asSchemaRDD()
```
- Result (finished in 9 minutes on 19 r3.large instances):
```
MetallicaTV	Metallica: Welcome Home (Sanitarium) & Sad But True (MetOnTour - Asunción, Paraguay - 2014)	Fly on the wall footage shot by the MetOnTour reporter on March 24, 2014 in Asunción, Paraguay. Footage includes the band warming up in the Tuning Room as well as both "Welcome Home (Sanitarium)" and "Sad But True" from the show. Download the full audio from the show at LiveMetallica.com: http://talli.ca/Asuncion2014 Follow Metallica: http://www.metallica.com http://www.livemetallica.com http://www.facebook.com/metallica http://www.twitter.com/metallica http://www.instagram.com/metallica http://www.youtube.com/metallicatv	Published on Apr 1, 2014	null	null	null	All comments (453)	Max Galvan	  james: what sound do u want? guy: SED BOT TRU!!!!! lol ﻿ Read more Show less
MetallicaTV	Metallica: Welcome Home (Sanitarium) & Sad But True (MetOnTour - Asunción, Paraguay - 2014)	Fly on the wall footage shot by the MetOnTour reporter on March 24, 2014 in Asunción, Paraguay. Footage includes the band warming up in the Tuning Room as well as both "Welcome Home (Sanitarium)" and "Sad But True" from the show. Download the full audio from the show at LiveMetallica.com: http://talli.ca/Asuncion2014 Follow Metallica: http://www.metallica.com http://www.livemetallica.com http://www.facebook.com/metallica http://www.twitter.com/metallica http://www.instagram.com/metallica http://www.youtube.com/metallicatv	Published on Apr 1, 2014	null	null	null	All comments (453)	J	  lol@ 14:41 James telling the fan not to bow before him XD﻿ Read more Show less
MetallicaTV	Metallica: Welcome Home (Sanitarium) & Sad But True (MetOnTour - Asunción, Paraguay - 2014)	Fly on the wall footage shot by the MetOnTour reporter on March 24, 2014 in Asunción, Paraguay. Footage includes the band warming up in the Tuning Room as well as both "Welcome Home (Sanitarium)" and "Sad But True" from the show. Download the full audio from the show at LiveMetallica.com: http://talli.ca/Asuncion2014 Follow Metallica: http://www.metallica.com http://www.livemetallica.com http://www.facebook.com/metallica http://www.twitter.com/metallica http://www.instagram.com/metallica http://www.youtube.com/metallicatv	Published on Apr 1, 2014	null	null	null	All comments (453)	Damian Sobik	  Lol Kirk at 3:30 hahah WTF ?!﻿ Read more Show less
... -------------------returned 47236 rows------------------
```

#### 7 Download forward citations from [Google Scholar](http://scholar.google.com)

- Goal: Given a list of titles of papers, search them on Google Scholar, collect the first result of each and information (namely titles and abstracts) of its forward citations (all publications that cites them) download all of them.

- Query:
```
    (sc.parallelize(Seq("Large scale distributed deep networks"))
      +> Visit("http://scholar.google.com/")
      +> DelayFor("form[role=search]")
      +> TextInput("input[name=\"q\"]","#{_}")
      +> Submit("button#gs_hp_tsb")
      +> DelayFor("div[role=main]")
      !=!())
      .extract(
        "title" -> (_.text1("div.gs_r h3.gs_rt a")),
        "citation" -> (_.text1("div.gs_r div.gs_ri div.gs_fl a:contains(Cited)"))
      )
      .visitJoin("div.gs_r div.gs_ri div.gs_fl a:contains(Cited)")(limit = 1)
      .paginate("div#gs_n td[align=left] a")()
      .sliceJoin("div.gs_r")()
      .extract(
        "citation_title" -> (_.text1("h3.gs_rt a")),
        "citation_abstract" -> (_.text1("div.gs_rs"))
      )
      .wgetJoin("div.gs_md_wp a")()
      .saveContent(select = _("citation_title"))
      .asSchemaRDD()
```

- Result (finished in 1.5 minute on my laptop with ~450k download speed):
```
Large scale distributed deep networks	Large scale distributed deep networks	Cited by 119	Good practice in large-scale learning for image classification	Abstract—We benchmark several SVM objective functions for large-scale image classification. We consider one-versus-rest, multiclass, ranking, and weighted approximate ranking SVMs. A comparison of online and batch methods for optimizing the objectives ...
Large scale distributed deep networks	Large scale distributed deep networks	Cited by 119	Deep learning with cots hpc systems	Abstract Scaling up deep learning algorithms has been shown to lead to increased performance in benchmark tasks and to enable discovery of complex high-level features. Recent efforts to train extremely large networks (with over 1 billion parameters) have ...
Large scale distributed deep networks	Large scale distributed deep networks	Cited by 119	A reliable effective terascale linear learning system	Abstract: We present a system and a set of techniques for learning linear predictors with convex losses on terascale datasets, with trillions of features,{The number of features here refers to the number of non-zero entries in the data matrix.} billions of training examples ...
... -------------------returned 119 rows------------------
```

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

Deployment
---------------
### ... to Local Computer/Single Node
1. Install Apache Spark 1.0.0 from [http://spark.apache.org/downloads.html]
2. (Optional, highly recommended otherwise you have to set it everytime before running the shell or application) Edit your startup script to point the environment variable of Spark to your Spark installation directory:
    - export SPARK_HOME=*your Spark installation directory*
3. Install PhantomJS 1.9.7 from [http://phantomjs.org/download.html]
    - recommended to install to '/usr/lib/phantomjs', otherwise please change *phantomJSRootPath* in *org.tribbloid.spookystuff.Const.scala* to point to your PhantomJS directory and recompile.
    - also provided by Ubuntu official repository (so you can apt-get it) but current binary is severely obsolete (1.9.0), use of this binary is NOT recommended and may cause unpredictable error.
4. git clone this repository.
5. Run `MAVEN_OPTS="-Xmx2g -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=512m" mvn package -DskipTests=true` or run  `./mvn-package.sh`
    - increasing jvm heapspace size for Apache Maven is mandatory as 2 modules (example and shell) will generate uber jars.
6. That's it! Now you have 3 options to use it:
    - (easiest) launch spooky-shell and improvise your query: `./bin/spooky-shell.sh`
    - give any example a test run: bin/submit-example.sh *name of the example*
    - write your own application by importing spooky-core into your dependencies.

### ... to Cluster/Amazon EC2
1. Setup a cluster and assure mutual connectivity
2. Install Ubuntu 12+ on all nodes.
    - scripts to autodeploy on other Spark-compatible OS is currently NOT under active development. Please vote on the issue tracker if you demand it.
    - the easiest way to set it up is on Amazon EC2, AMI with pre-installed environment and autoscaling ability will be made public shortly
3. Install Ansible on your client and make sure you can ssh into all your nodes with a valid private key (id_rsa).
4. Edit files in ops/ansible/inventories.template to include ip/dns of your master node and all worker nodes. Change the directory name to /ops/ansible/inventories
5. cd into ops/ansible and:
    - deploy master: ./ansible-playbook deploy-master.yml -i ./inventories --private-key=*yor private key (id_rsa)*
    - deploy workers: .ansible-ploybook deploy-worker.yml -i ./inventories --private-key=*yor private key (id_rsa)*
    - this will install oracle-java7 and do step 1,2,3, automatically on all nodes. You can do it manually but that's a lot of work!
6. Do step 4,5 on master node and run any of the 3 options
    - you can download and run it on any node in the same subnet of the cluster, but expect heavy traffic between your client node and master node.

### alternatively ...
you can use scripts in $SPARK_HOME/ec2 to setup a Spark cluster with transient HDFS support. But this has 2 problems:
    - Autoscaling is currently not supported.
    - Spark installation directory is hardcoded to '/root/spark', if your client has a different directory it may cause some compatibility issue.


Integration Test
----------------

	mvn clean test-compile failsafe:integration-test

License
-----------

Copyright &copy; 2014 by Peng Cheng @tribbloid, Sandeep Singh @techaddict, Terry Lin @ithinkicancode, Long Yao @l2yao and contributors.

Published under ASF License, see LICENSE.
