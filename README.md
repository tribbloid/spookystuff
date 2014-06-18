spookystuff
===========

(OR: how to turn 21st century into an open spreadsheet) is a scalable and distributed web scrapping/data collection/acceptance QA environment based on Apache Spark. The goal is to allow the Web being queried and ETL'ed as if it is a database.

Dependencies
-----------
- Apache Spark
- Selenium
    - GhostDriver/PhantomJS (for Google Chrome client simulation)
- JSoup
- Apache Tika (only for non-html parsing)
- (build tool) Apache Maven
    - Scala/ScalaTest plugins
- Current implementation is influenced by Spark SQL and Mahout Sparkbinding.

A one minute showcase
-----------
#### 1. Headhunting on LinkedIn
- Goal: Find highest-ranking professionals in you area on LinkedIn. whose full name is either 'Sanjay Gupta', 'Arun Gupta' or 'Hardik Gupta', print their respective full name, title and list of skill
- Query:
```
(sc.parallelize(Seq("Sanjay", "Arun", "Hardik"))
    +> Visit("https://www.linkedin.com/")
    +> TextInput("input#first", "#{_}")
    +> TextInput("input#last", "Gupta")
    +> Submit("input[name=\"search\"]") ! )
    .fork("ol#result-set h2 a")
    .select( page => (
        page.textFirst("span.full-name"),
        page.textFirst("p.title"),
        page.textAll("div#profile-skills li")
        )
    ).collect().foreach(println(_))
```
- Result (truncated, query finished in 1 minutes, test on wifi with ~400k/s download speed):
```
(Abhishek Arun Gupta,President & Senior IT Expert / Joint Chairman - IT Cell at Agra User Group / National Chamber of Industries & Commerce,ArrayBuffer(Requirements Analysis, SQL, Business Intelligence, Unix, Testing, President & Senior IT Expert, Joint Chairman - IT Cell, Quality Assurance (QA) & Automation Systems, Senior Automation Testing Expert, Senior Executive, Industry Interface))
(hardik gupta,--,ArrayBuffer())
(Arun Gupta,Sales at adjust by adeven,ArrayBuffer(Mobile, Business Strategy, Digital Media, Advertising Sales, Direct Sales, New Business Development, Mobile Marketing, Mobile Advertising, Publishing, Mobile Devices, Strategic Partnerships, Start-ups, Online Marketing, Mobile Applications, SEO, SEM, Business Development, Social Networking, Digital Marketing, Management, Digital Strategy))
(Dr. Sanjay Gupta,Co-Founder & Director at IMPACT PSD Private Limited,ArrayBuffer(Computer proficiency, Secondary Research, Operations Management, Qualitative Research, Research and M&E, Data Management, Data Interpretation, M&E, Research, Report Writing, Data Analysis, Proposal Writing, Program Management, Capacity Building, NGOs, Leadership, Market Research, Policy, Civil Society, International Development, Nonprofits, Public Policy, Corporate Social Responsibility, Training, Program Evaluation, Analysis, Business Development, Sustainable Development, Data Collection, Technical Assistance, Organizational Development, Fundraising, Community Development, Quantitative Research, Government, Program Development, Policy Analysis, Reproductive Health))
(Dr. Arun Kumar Gupta,Chief Executive Officer,ArrayBuffer())
... (75 lines)
```

#### 2. Find interchangeable parts of a washing machine on appliancepartspros.com
- Goal: Find all parts on model 'A210S', print the full name of the model, time to find the model on website (in ms), schematic description, manufacturer's part number, and all substitutes
- Query:
```
(sc.parallelize(Seq("A210S"))
    +> Visit("http://www.appliancepartspros.com/")
    +> TextInput("input.ac-input","#{_}")
    +> Click("input[value=\"Search\"]")
    +> Delay(10) !)
    .addToContext(
        "model" -> { _.textFirst("div.dgrm-lst div.header h2") },
        "time1" -> { _.backtrace.last.timeline.asInstanceOf[Serializable] }
    ).fork("div.inner li a:has(img)")
    .addToContext(
        "schematic" -> {_.textFirst("div#ctl00_cphMain_up1 h1 span")}
    ).fork("tbody.m-bsc td.pdct-descr h2 a")
    select(
        page => (
        page.context.get("_"),
        page.context.get("time1"),
        page.context.get("model"),
        page.context.get("schematic"),
        page.textFirst("div.m-bsc div.mod ul li:contains(Manufacturer) strong"),
        page.textFirst("div.m-pdct div.m-chm p")
        )
    ).collect().foreach(println(_))
```
- Result (truncated, query finished in 10 minutes, test on wifi with ~400k/s download speed):
```
(A210S,A210S Washer-Top Loading ,14789,01-Base\pump\motor Parts for Maytag A210S,Y015627,Part Number Y015627 (AP4277222) replaces 014526, 015627, 1239310, 15627, 24001310, 3400300, 3400502, 488266, 488293, 488398, 488577, 488594, 866821, 9415810, AH2191209, EA2191209, PS2191209, Y014526.)
(A210S,A210S Washer-Top Loading ,14789,01-Base\pump\motor Parts for Maytag A210S,202718,Part Number 202718 (AP4023504) replaces 2-11946, 2-2718, 211946, 22001442, 434716, AH2016844, EA2016844, PS2016844.)
(A210S,A210S Washer-Top Loading ,14789,04-Control Panel, Timer & Switches Parts for Maytag A210S,205611,Part Number 205611 (AP4023851) replaces 2-5611, 435339, AH2017212, EA2017212, PS2017212.)
... (311 lines)
```

### Showcase environment
- The easiest way is to test locally on you scala IDE or REPL.
- You need to install [PhantomJS](http://phantomjs.org/) on you computer. The default installation directory is '/usr/lib/phantomjs'.
    - (If your PhantomJS is installed to a different directory, please change *phantomJSRootPath* in *org.tribbloid.spookystuff.Conf.scala* to point to your PhantomJS directory.)
- You DON"T need to install Apache Spark to test it in local simulation mode (where masterURL = "local[...]"), the spark jar in you local Maven repository has everything you need.
    - You DO need to do this to deploy your query or application to a cluster.
1. Git clone me: git clone https://github.com/tribbloid/spookystuff.git
2. Create your scala project. Add spookystuff-core to its dependency list
    - If you project is managed by Maven, add this to your .pom dependency list:
    ```
        <dependency>
            <groupId>org.tribbloid.spookystuff</groupId>
            <artifactId>spookystuff-core</artifactId>
        </dependency>
    ```
    - Alternatively you can just run Spark-shell or Scala REPL within the project environment.
3. Import scala packages and context, including:
    - org.apache.spark.{SparkContext, SparkConf} (entry point for all Spark applications)
    - org.tribbloid.spookystuff.SpookyContext._ (enable implicit operators for web scrapping)
4. Initialize your SparkContext ```val sc = new SparkContext(new SparkConf().setMaster("local[*]"))```
    - if some of your target websites are unstable and may need some reloading, use this:
    ```val sc = new SparkContext(new SparkConf().setMaster("local[${MaxConcurrency},${MaxRetry}]"))```
5. That's it, you can start writing queries, run them and see the results immediately. 

Query/Programming Guide
-----------
[This is a stub]

So far spookystuff only supports LINQ style query language, APIs are not finalized (in fact, still far from that) and may change in the future.

I'm trying to make the query language similar to the language-integrated query of Spark SQL. However, as organizations of websites are fundamentally different from relational databases, it may gradually evolve to attain maximum succinctness.

Cluster Deployment
-----------
[This is a stub] Theoretically all Spark application that runs locally can be submitted to cluster by only changing its masterURL parameter in SparkConf. However I haven't test it myself. Some part of the code may not be optimized for cluster deployment.

- Make sure PhantomJS is installed on all cluster nodes.

- If you want to write extension for this project, MAKE SURE you don't get *NotSerializableException* in a local run (it happens when Spark cannot serialize data when sending to another node), and keep all RDD entity's serialization footprint small to avoid slow partitioning over the network.

Maintainer
-----------
(if you see a bug these are the guys/girls to blame for)

- @tribbloid (pc175@uow.edu.au)