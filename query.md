---
layout: global
title: Query
---

* This will become a table of contents (this text will be scraped).
{:toc}

# Overview

SpookyStuff's' Query Language is a short and powerful LINQ (Language-Integrated Query) that abstracts away the complexity of unstructured document parsing/traversing, link crawling and parallel computing/optimization, leaving only two things for you to consider: The data you want and the process to discover them. This leads to our first rule in syntax design: identical queries should expect identical results, and vice versa.

SpookyStuff is built on Scala, it borrows some concepts and most of its syntax from [Spark SQL] (e.g. inner & left join, explode, alias and select), notably its own [Language-Integrated Query]. This is for easier understanding and memorizing by SQL users, rather than draw an analogy between relational databases and linked/unstructured datasets. In fact, they have very different specifications: In linked data, join by expression and filter by predicate ("WHERE" clause in SQL) are prohibitively expensive if executed on the client side, which makes URI and web service-based lookup its only standard.

The following diagram illustrates the elements of SpookyStuff queries: context, clauses, actions and expressions.

![img]

To run it in any Scala environment, import all members of the package org.tribbloid.spookystuff.dsl:

{% highlight scala %}
import org.tribbloid.spookystuff.dsl._
{% endhighlight %}

# Context

SpookyContext is the entry point of all queries, it can be constructed from a [SparkContext] or [SQLContext], with SpookyConf as an optional parameter:

{% highlight scala %}
val spooky = new SpookyContext(sc: SparkContext) // OR
val spooky = new SpookyContext(sql: SQLContext) // OR
val spooky = new SpookyContext(sql: SQLContext, conf: SpookyConf)
{% endhighlight %}

SpookyConf contains configuration options that are enumerated in [Configuration Section](deploying.html#configuration), these won't affect the result of the query, but have an impact on execution efficiency and resiliency. all these options can be set statically and changed dynamically, even in the middle of a query, e.g.

{% highlight scala %}
import scala.concurrent.duration.Duration._

spooky.conf.pageExpireAfter = 1.day // OR
spooky.{clause(s)}.setConf(_.pageExpireAfter = 1.day).{clause(s)}.... // in the middle of a query
{% endhighlight %}

Multiple SpookyContext can co-exist and their configurations will only affect their derived queries respectively.

# Clauses

Clauses are building blocks of a query, each denotes a specific pattern to discover new data from old data's references (e.g. through hyperlinks). Clauses can be applied to any of the following Scala objects:

- PageRowRDD: this is the default data storage optimized for unstructured and linked data operations, like DataFrame optimized for relation data. Internally, PageRowRDD is a distributed and immutable row-storage that contains both unstructured "raw" documents and key-value pairs. it can be created from a compatible Spark dataset type by using SpookyContext.create():

{% highlight scala %}
// if source only contains 1 datum per row and its field name is missing, create a PageRowRDD with 1 field: '_
val frame1 = spooky.create(1 to 10)

//create a PageRowRDD with 2 fields: 'name and 'type
val frame2 = spooky.create(
    Map("name" -> "Cloudera", "type" -> "company") ::
    Map("name" -> "Hadoop", "type" -> "Software") :: Nil
)
{% endhighlight %}

And to those types by the following functions:

{% highlight scala %}
frame1.toStringRDD('_)  // convert one field to RDD[String]
frame2.toMapRDD()       // convert to MapRDD
frame2.toDF()           // convert to DataFrame
frame2.toJSON()         // equal to .toDF().JSON()
frame2.toCSV()          // convert to CSV
frame2.toTSV()          // convert to TSV
{% endhighlight %}

PageRowRDD is also the output of another clause so you can chain multiple clauses easily to form a long data valorization pipeline. In addition, it inherits SpookyContext from its source or predecessor, so settings in SpookyConf are persisted for all queries derived from it (unless you change them halfway, this is common if a query uses mutliple web services, each with its own Authentication credential or proxy requirement).

- SpookyContext: equivalent to applying to an empty PageRowRDD with one row.

- Any compatible Spark dataset type: equivalent to applying to a PageRowRDD initialized from SpookyContext.create(). To use this syntax you need to import context-aware implicit conversions:

{% highlight scala %}
import spooky.dsl._ // only one SpookyContext can be imported like this
{% endhighlight %}

The following 5 main clauses covered most data reference patterns in websites, documents and APIs, including, but not limited to, one-to-one, one-to-many, link graph and paginations.

#### fetch

{% highlight scala %}
.fetch(Action(s), [parameters])
{% endhighlight %}

Fetch is used to remotely fetch unstructured document(s) per row according to provided actions and load them into each row's document buffer, which flush out previous document(s). Fetch can be used to retrieve an URI directly or combine data from different sources based on one-to-one relationship, e.g.

<a name="fetch-example"></a>

{% highlight scala %}
// this loads the landing page of Wikipedia into a PageRowRDD with cardinality 1.
spooky.fetch(Wget("https://www.wikipedia.com")).toStringRDD(S.text).collect().foreach(println)

//this loads 20 wikidata pages, 1 page per row
spooky.create(1 to 20).fetch(Wget("https://www.wikidata.org/wiki/Q'{_}")).flatMap(S.text).collect().foreach(println)
{% endhighlight %}

Where ```Wget``` is a simple [action] that retrieves a document from a specified URI. Notice that the second URI is incomplete: part of it is denoted by ```'{_}'```, this is a shortcut for **string interpolation**: during execution, any part enclosed in ```'{key}'``` will be replaced by a value in the same row the {key} identifies. String interpolation is part of a rich expression system covered in [Expressions Section](#expressions).

##### fetch parameters

<div class="table" markdown="1">

| Name | Default | Means |
| ---- | ------- | ----- |
| joinType |
| flattenPagesPattern |
| flattenPagesOrdinalKey |
| numPartitions |
| optimizer |

</div>

#### select & remove

{% highlight scala %}
.select(
    expression [~ 'alias],
    expression [~+ 'alias],
    expression [~! 'alias]
    ...
)
{% endhighlight %}

Select is used to extract data from unstructured documents and persist into the key-value store, unlike SQL, select won't discard existing data:

<a name="select-example"></a>

{% highlight scala %}
//this load the featured article text of Wikipedia (English) into one row
spooky.fetch(
    Wget("https://en.wikipedia.com")
).select(S"table#mp-left p" ~ 'featured_article).toDF().collect().foreach(println)
{% endhighlight %}

If the alias of an expression already exist in the store it will throw a QueryException and refuse to execute. In this case you can use ```~! 'alias``` to replace old value or ```~+ alias``` to append the value to existing ones as an array. To discard data from the key-value store, use **remove**:

{% highlight scala %}
.remove('alias, 'alias ...)
{% endhighlight %}

#### flatten/explode

{% highlight scala %}
.flatten/explode(expression [~ alias], [parameters])
{% endhighlight %}

Flatten/Explode is used to transform each row into a sequence of rows, each has an object of the array selected from the original row:

{% highlight scala %}
//select and explode all titles in google RSS feed into multiple rows
spooky.fetch(
    Wget("https://news.google.com/news?q=apple&output=rss")
).flatten(S"item title" ~ 'title).toDF().collect().foreach(println)
{% endhighlight %}

##### flatten/explode parameters

<div class="table" markdown="1">

| Name | Default | Means |
| ---- | ------- | ----- |
| joinType |

</div>

SpookyStuff also provides **flatSelect** as a shorthand for **flatten + select**:

<a name="flatselect-example"></a>

{% highlight scala %}
.flatSelect(flatten-expression, [parameters])(
    select-expression [~ 'alias] ...
)
{% endhighlight %}

which is equivalent to:

{% highlight scala %}
.flatten(flatten-expression ~ 'A), [parameters]
).select(
    select-expression> [~ 'alias] ...
)
{% endhighlight %}

One of its common usages is extracting multiple attributes and fields from DOM elements in a list or tree, e.g.

{% highlight scala %}
// flatten articles in google RSS feed and select titles and publishing date
spooky.fetch(
    Wget("https://news.google.com/news?q=apple&output=rss")
).flatSelect(S"item")(
    A"title" ~ 'title
    A"pubDate" ~ 'date
).toDF().collect().foreach(println)
{% endhighlight %}

You may notice that the first parameter of flatSelect has no alias - SpookyStuff automatically assigns an alias 'A to the flatten DOM elements as intermediate data, which also enables a few shorthand expressions for traversing deeper into them. This syntax will be used more often in join and explore.

#### join

{% highlight scala %}
.join(flatten-expression [~ alias], [flatten-parameters]
)(Action(s), [fetch-parameters])(
    select-expression> [~ 'alias]
    ...
)
{% endhighlight %}

Join is used to horizontally combine data from different sources based on one-to-many/many-to-many relationship, e.g. combining links on a search/index page with fulltext contents:

{% highlight scala %}
// this search for movies named "Gladiator" on dbpedia Lookup API (http://wiki.dbpedia.org/projects/dbpedia-lookup) and link to their entity pages to extract their respective abstracts
spooky.create("Gladiator" :: Nil).fetch(
    Wget("http://lookup.dbpedia.org/api/search/KeywordSearch?QueryClass=film&QueryString='{_}")
).join(S"Result")(
    Wget(A"URI".text),2
)(
    A"Label".text ~ 'label,
    S"span[property=dbo:abstract]".text ~ 'abstract
).toDF().collect().foreach(println)
{% endhighlight %}

Join is a shorthand for flatten/explode + fetch, in which case the data/elements being flatten is similar to a foreign key in relational database.

##### join parameters

(See [flatten/explode parameters](#flattenexplode-parameters) and [fetch parameters](#fetch-parameters))

#### explore

{% highlight scala %}
.explore(expression [~ alias], [parameters]
)(Action(s), [fetch-parameters])(
    select-expression> [~ 'alias]
    ...
)
{% endhighlight %}

Explore defines a parallel graph exploring pattern that can be best described as recursive join with deduplication: In each iteration, each row is joined with one or more documents, they are compared and merged with existing documents that has been traversed before, the iteration stops when maximum exploration depth has been reached, or no more new documents are found. Finally, all documents and their rows are concatenated vertically. Explore is used for conventional web crawling or to uncover "deep" connections in a graph of web resources, e.g. pagination, multi-tier taxonomy/disambiguation pages, "friend-of-a-friend" (FOAF) reference in a social/semantic network:

{% highlight scala %}
// this retrieve all documents that are related to (rel) or reverse-related to (rev) dbpedia page of Bob Marley
spooky.fetch(
    Wget("http://dbpedia.org/page/Bob_Marley")
).explore(
    S"a[rel^=dbo],a[rev^=dbo]",
    maxDepth = 2
)(Wget('A.href)
)('A.text ~ 'name).toDF().collect().foreach(println)
{% endhighlight %}

##### explore parameters

<div class="table" markdown="1">

| Name | Default | Means |
| ---- | ------- | ----- |
| joinType |

</div>

#### others

# Actions

Each action defines a web client command, such as content download, loading a page in browser, or clicking a button.

Action can be chained to define a series of command in a client session, e.g:

{% highlight scala %}
// in a browser session, search "Deep Learning" on Google's landing page
spooky.fetch(
    Visit("http://www.google.com/")
      +> TextInput("input[name=\"q\"]","Deep Learning")
      +> Submit("input[name=\"btnG\"]")
      +> Snapshot()
).flatMap(S.text).collect().foreach(println)
{% endhighlight %}

In addition to the "chaining" operator **+>**, the following 2 can be used for branching:

- ```Action(s) || Action(s)``` : Combine multiple actions and/or chains in parallel, each executed in its own session and in parallel. This multiplies the cardinality of output with a fixed factor, e.g.

{% highlight scala %}
// load french and german translation of a sentence in 2 rows.
spooky.create("I'm feeling lucky"
).fetch(
    Wget("http://api.mymemory.translated.net/get?q='{_}&langpair=en|fr") ||
    Wget("http://api.mymemory.translated.net/get?q='{_}&langpair=en|de")
).select((S \ "responseData" \ "translatedText" text) ~ 'translated).toDF().collect().foreach(println)
{% endhighlight %}

- ```Action(s) *> Parallel-Actions(s)``` : Cross/Cartesian join and concatenate a set of actions/chains with another set of actions/chains, resulting in all their combinations being executed in parallel. e.g.

{% highlight scala %}
// in 2 browser session, search "Mont Blanc" in English and French on Wikipedia's landing page
fetch(
    Visit("https://www.wikipedia.org/")
      +> TextInput("input#searchInput","Mont Blanc")
      *> DropDownSelect("select#searchLanguage","en") || DropDownSelect("select#searchLanguage","fr")
      +> Submit("input[type=submit]")
      +> Snapshot()
).flatMap(S.text).collect().foreach(println)
{% endhighlight %}

The following tables list all out-of-the-box actions. parameters enclosed in square brackets are *[optional]* and are explained in [Optional Parameter Section](#optional-parameters). If you are still not impressed and would like to write your own action, please refer to [Writing Extension Section](dev.html#writing-extensions).

#### Web Client & System

<div class="table" markdown="1">

| Syntax | Means |
| ---- | ------- |
| Wget(URI-expression, [filter]) [~ 'alias] | retrieve a document by its universal resource identifier (URI), whether its local or remote and the client protocol is determined by schema of the URI, currently, supported schema/protocals are http, https, ftp, file, hdfs, s3, s3n and all Hadoop-compatible file systems.* |
| OAuthSign(Wget(...)) | sign the http/https request initiated by Wget by OAuth, using credential provided in SpookyContext, not effective on other protocols |
| Delay([duration]) | hibernate the client session for a fixed duration |
| RandomDelay([min, max]) | hibernate the client session for a random duration between {min} and {max} |

</div>

* local resources (file, hdfs, s3, s3n and all Hadoop-compatible file systems) won't be cached: their URIs are obviously not "universal", plus fetching them directly is equally fast.

* http and https clients are subjective to proxy option in SpookyContext, see [Configuration Section](deploying.html#configuration) for detail.

* Wget is the most common action as well as the quickest. In many cases its the only action resolved in a fetch, join or explore, for which cases 3 shorthands clauses can be used:

{% highlight scala %}
.wget(URI-expression, [parameters])
.wgetJoin(selector, [parameters])
.wgetExplore(selector, [parameters])
{% endhighlight %}

#### Browser

The following actions are resolved against a browser launched at the beginning of the session. The exact type and specifications (screen resolution, image/javascript supports) of the browser being launched and its proxy setting are subjective to their respective options in SpookyContext. Launching the browser is optional and necessity driven: the session won't launch anything if there is no need.

<div class="table" markdown="1">

| Syntax | Means |
| ---- | ------- |
| Visit(URI-expression, [delay, blocking]) | go to the website denoted by specified URI in the browser, usually performed by inserting the URI into the browser's address bar and press enter |
| Snapshot([filter]) [~ 'alias] | export current document in the browser, the document is subjective to all DOM changes caused by preceding actions, exported documents will be loaded into the document buffer of the resulted PageRowRDD, if no SnapShot, Screenshot or Wget is defined in a chain, Snapshot will be automatically appended as the last action|
| Screenshot([filter]) [~ 'alias] | export screenshot of the browser as an image document in PNG format |
| Assert([filter]) | Same as Snapshot, but this is only a dry-run which ensures that current document in the browser can pass the {filter}, nothing will be exported or cached |
| Click(selector, [delay, blocking]) | perform 1 mouse click on the 1st visible element that qualify the jQuery {selector} |
| ClickNext(selector, [delay, blocking]) | perform 1 mouse click on the 1st element that hasn't been clicked before within the session, each session keeps track of its own history of interactions and elements involved |
| Submit(selector, [delay, blocking]) |  submit the form denoted by the 1st element that qualify the jQuery {selector}, this is usually performed by clicking, but can also be done by other interactions, like pressing enter on a text box |
| TextInput(selector, text-expression, [delay, blocking]) | focus on the 1st element (usually but not necessarily a text box) that qualify the jQuery {selector}, then insert/paste the specified text |
| DropDownSelect(selector, text-expression, [delay, blocking]) | focus on the 1st selectable list that qualify the jQuery {selector}, then select the item with specified value |
| ExeScript(javascript-expression, [selector, delay, blocking]) | run javascript program against the 1st element that qualify the jQuery {selector}, or against the whole document if {selector} is set to null or missing |
| DragSlider(selector, percentage, handleSelector, [delay, blocking]) | drag a slider handle denoted by {handleSelector} to a position defined by a specified percentage and an enclosing bounding box denoted by {selector} |
| WaitFor(selector) | wait until the 1st element that qualify the jQuery {selector} appears |

</div>

* Visit +> Snapshot is the second most common action that does pretty much the same thing as Wget on HTML resources - except that it can render dynamic pages and javascript. Very few formats other than HTML/XML are supported by browsers without plug-in so its mandatory to fetch images and PDF files by Wget only. In addition, its much slower than Wget for obvious reason. If Visit +> Snapshot is the only chained action resolved in a fetch, join or explore, 3 shorthand clauses can be used:

{% highlight scala %}
.visit(URI-expression, [parameters])
.visitJoin(selector, [parameters])
.visitExplore(selector, [parameters])
{% endhighlight %}

* Multiple Snapshots and Screenshots can be defined in a chain of actions to persist different states of a browser session, their exported documents can be specifically referred by their aliases.

#### Flow Control

<div class="table" markdown="1">

| Syntax | Means |
| ---- | ------- |
| Loop(Action(s), [limit]) | Execute the specified {Action(s)} repeatedly until max iteration is reached or an exception is thrown during the execution (can be triggered by Assert) |
| Try(Action(s), [retries, cacheError]) | By default, if an exception is thrown by an action, the entire session is retried, potentially by a different Spark executor on a different machine, this failover handling process continues until **spark.task.maxFailures** property has been reached, and the entire query failed fast. However, if the {Action(s)} throwing the exception is enclosed in the **Try** block, its last failure will be tolerated and only results in an error page being exported as a placeholder. Make sure to set **spark.task.maxFailures** to be larger than {retries}, or you query will fail fast before it even had a chance to interfere! |
| If(condition, Action(s)-if-true, Action(s)-if-false) | perform a condition check on current document in the browser, then execute different <Action(s)> based on its result |

</div>

* Loop(ClickNext +> Snapshot) is generally used for turning pages when its pagination is implemented in AJAX rather than direct hyperlinks, in which case a shorthand is available:

#### Optional Parameters

<div class="table" markdown="1">

| Name | Default | Means |
| ---- | ------- | ----- |
| filter | ExportFilters.MustHaveTitle |
| alias | Action.toString |
| timeout | null |
| delay | 1.second |
| blocking | true |
| limit | 500 |
| retries | 3 |
| cacheError | false |

</div>

# Expressions

In previous examples, you may already see some short expressions being used as clause or action parameters, like the [string interpolation](#fetch-example) in URI templating, or the ["Big S"](#select-example) / ["Big A"](#flatselect-example) notations in many select clauses for unstructured information refinement. These are parts of a much more powerful (and equally short) expression system. It allows complex data reference to be defined in a few words and operators (words! not lines), while other frameworks may take scores of line. E.g. the following query uses an URI expression (in the second Wget), which pipes all titles on Google News RSS feed into a single MyMemory translation API call:

{% highlight scala %}
spooky.wget("https://news.google.com/?output=rss"
).wget(
    //how short do you want it? considering its complexity
    x"http://api.mymemory.translated.net/get?q=${S"item title".texts.slice(0,5).mkString(". ")}&langpair=en|fr"
).toStringRDD(S"translatedText".text).collect().foreach(println)
{% endhighlight %}

Internally, each expression is a function from a single row in PageRowRDD to a nullable data container. Please refer to [Writing Extension Section](dev.html#writing-extensions) if you want to use customized expressions.

The following is a list of basic symbols and functions/operators that are defined explicitly as shorthands:

#### symbols

Scala symbols (identifier preceded by tick/') has 2 meanings: it either returns a key-value pair by its key, or a subset of documents in the unstructured document buffer filtered by their aliases. SpookyStuff's execution engine is smart enough not to throw an error unless there is a conselectorflict - In which case you need to use different names for data and documents.

The following 2 variables are also treated as symbols with special meaning:

- ```S``` : Returns the only document in the unstructured document buffer, error out if multiple documents exist.

- ```S_*``` : Returns all documents in the unstructured document buffer.

#### string interpolation

You have seen the [basic form] of string interpolation early in some examples, which inserts only key-value pair(s) to a string template. The [above example] demonstrates a more powerful string interpolation, which inserts all kinds of expressions:

{% highlight scala %}
x"segment ${expression} segment ${expression} segment ..."
{% endhighlight %}

Any non-expression, non-string typed identifier is treated as literal.

#### unstructured document traversing

These are operators that does the most important things: traversing DOM of unstructured documents to get data you want (potentially structured or semi-structured).

SpookyStuff supports a wide range of documents including HTML, XHTML, XML, JSON, and [all other formats supported by Apache Tika](https://tika.apache.org/1.9/formats.html) (PDF, Microsoft Office and much more). In fact, most of the following operators are **format-agnostic**: they use different parsers on different formats to get the same DOM tree. Not all these formats have equivalent DOM representation (e.g. JSON doesn't have annotated text), so using a few operators on some formats doesn't make sense and always returns null value.

The exact format of a document is dictated by its mime-type indicated by the web resource, if it is not available, SpookyStuff will auto-detect it by analysing its extension name and binary content.

All operators that fits into this category are listed in the following table, with their compatible formats and explanation.

<div class="table" markdown="1">

| Operator | Formats | Means |
| .findAll(selector) | All | returns all elements that qualify the selector provided, which should be exact field name for JSON type and jQuery selector for all others |
|  \\ selector | All | Same as above |
| .findFirst(selector) | All | first element returned by **findAll** |
| .children(selector") | All | returns only the direct children that qualify by the selector provided, which should be exact field name for JSON type and jQuery selector for all others |
|  \ selector | All | Same as above |
| .child(selector) | All | first element returned by **children** |
| S"selector" | All | Big S selector: equivalent to S.findAll(selector) |
| A"selector" | All | Big A selector: equivalent to 'A.findAll(selector), 'A is the default symbol for the elements/data pivoted in flatten, flatSelect, join or explore |
| S_*"selector" | All | Equivalent to S_*.findAll(selector) |
| .uri | All | URI of the document or DOM element, this may be different from the URI specified in Wget or Visit due to redirection(s) |
| .code | All | returns the original HTML/XML/JSON code of the parsed document/DOM element as string |
| .text | All | returns raw text of the parsed document/DOM element stripped of markups, on JSON this strips all field names and retains only their values |
| .ownText | All | returns raw text of the parsed document/DOM element excluding those of its children, on JSON this returns null if the element is an object |
| .attr("attribute-name", [nullable?]) | All | returns an attribure value of the parsed document/DOM element, on JSON this returns a property preceded by "@", parameter {nullable?} decides whether a non-existing attribute should be returned as null or an empty string |
| .href | All | same as .attr("href") on HTML/XML, same as .ownText on JSON |
| .src | All | same as .attr("src") on HTML/XML, same as .ownText on JSON |
| .boilerpipe | Non-JSON | use [boilerpipe](https://code.google.com/p/boilerpipe/) to extract the main textual content of a HTML file, this won't work for JSON |

</div>

#### others

Many other functions are also supported by the expression system but they are too many to be listed here. Scala users are recommended to refer to source code and scaladoc of ```org.tribbloid.spookystuff.dsl``` package, as these functions are built to resemble Scala functions and operators, rather than the less capable SQL LINQ syntax. In fact, SpookyStuff even use Scala reflective programming API to handle functions it doesn't know.

# Profiling

SpookyStuff has a metric system based on Spark's [Accumulator](https://spark.apache.org/docs/latest/programming-guide.html#AccumLink), it can be accessed from **metrics** property under the SpookyContext:

{% highlight scala %}
println(rows.spooky.metrics.toJSON)
{% endhighlight %}

By default each query keep track of its own metric, if you would like to have all metrics of queries from the same SpookyContext to be aggregated, simply set **conf.shareMetrics** property of the SpookyContext to *true*.

# Examples

Interactive examples are maintained on [tribbloid® product page](http://tribbloid.github.io/product/)

More examples can be found under [spookystuff-example package](https://github.com/tribbloid/spookystuff/tree/master/example/src/main/scala/org/tribbloid/spookystuff/example)