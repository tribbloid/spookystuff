---
layout: global
title: Query - SpookyStuff SPOOKYSTUFF_VERSION documentation
---

* This will become a table of contents (this text will be scraped).
{:toc}

# Overview

SpookyStuff's' Query Language is a short and powerful LINQ (Language-Integrated Query) that abstracts away the complexity of unstructured document parsing/traversing, link crawling and parallel computing/optimization, leaving only two things for you to consider: The data you want and the process to discover them. This leads to our first rule in syntax design: identical queries should expect identical results, and vice versa.

SpookyStuff is built on Scala, it borrows some concepts and most of its syntax from [Spark SQL] (e.g. inner & left join, explode, alias and select), notably its own [Language-Integrated Query]. This is for easier understanding and memorizing by SQL users, rather than draw an analogy between relational databases and linked/unstructured datasets. In fact, they have very different specifications: In linked data, join by expression and filter by predicate ("WHERE" clause in SQL) are prohibitively expensive if executed on the client side, which makes URI and web service-based lookup its only standard.

The following diagram illustrates the elements of SpookyStuff queries: context, clauses, actions and expressions.

![img]

To run it in any Scala environment, import all members of the package org.tribbloid.spookystuff.dsl:

    import org.tribbloid.spookystuff.dsl._

# Context

SpookyContext is the entry point of all queries, it can be constructed from a [SparkContext] or [SQLContext], with SpookyConf as an optional parameter:

    val spooky = new SpookyContext(sc: SparkContext) // OR
    val spooky = new SpookyContext(sql: SQLContext) // OR
    val spooky = new SpookyContext(sql: SQLContext, conf: SpookyConf)

SpookyConf contains configuration options that are enumerated in [Configuration Section], these won't affect the result of the query, but have an impact on execution efficiency and resiliency. all these options can be set statically and changed dynamically, even in the middle of a query, e.g.

    import scala.concurrent.duration.Duration._

    spooky.conf.pageExpireAfter = 1.day // OR
    spooky.<clauses>.setConf(_.pageExpireAfter = 1.day).<clauses>.... // in the middle of a query

Multiple SpookyContext can co-exist and their configurations will only affect their derived queries respectively.

# Clauses

Clauses are programming blocks of a query, each denotes a specific pattern to discover new data from references in old data (e.g. hyperlinks). Clauses can be applied to a SpookyContext, a PageFrame (the data structure returned by a clause as the result enriched dataset, this makes chaining clauses possible), or a compatible common Spark dataset type (e.g. [DataFrame] or String RDD, need to enable implicit conversion first, this will be covered in [I/O section] in detail)

Most of the clauses returns a PageFrame as output, PageFrame is an abstraction of a distributed and immutable row-storage that contains both unstructured "raw" documents and key-value pairs. it can be easily converted to other Spark dataset types (covered in [I/O section]). In addition, it inherits SpookyContext from its predecessor, so settings in SpookyConf are persisted for all queries and clauses derived from them (unless you change them halfway, this is common if a query uses mutliple web services, each with its own Authentication credential or proxy requirement).

The following 5 main clauses covered most linking patterns in websites, documents and APIs, including one-to-one link, one-to-many link, graph link and pagination.

#### fetch

Syntax:

    <Source>.fetch(<Action(s)>, <parameters>)

Fetch is used to remotely fetch unstructured document(s) per row according to provided actions and load them into each row's document buffer, which flush out previous document(s). Fetch can be used resolve actions directly or combine data from different sources based on one-to-one relationship:

    spooky.fetch(Wget("https://www.wikipedia.com")).flatMap(S.text).collect().foreach(println) // this loads the landing page of Wikipedia into a PageFrame with cardinality 1.

    spooky.create(1 to 20).fetch(Wget("https://www.wikidata.org/wiki/Q'{_}")).flatMap(S.text).collect().foreach(println) //this loads 20 wikidata pages, 1 page per row

Parameters:

| Name | Default | Means |
| ---- | ------- | ----- |
| joinType |
| flattenPagesPattern |
| flattenPagesOrdinalKey |
| numPartitions |
| optimizer |

#### select

Syntax:

    <Source>.select(
        <expression> [~ '<alias>],
        <expression> [~+ '<alias>],
        <expression> [~! '<alias>]
        ...
    )

Select is used to extract data from unstructured documents and persist into its key-value store, unlike SQL, select won't discard existing data from the store:

    spooky.fetch(
        Wget("https://en.wikipedia.com")
    ).select(S"table#mp-left p" ~ 'featured_article).toDF().collect().foreach(println) //this load the featured article text of Wikipedia(English) into one row

If the alias of an expression already exist in the store it will throw a QueryException and refuse to execute. In this case you can use ```~! <alias>``` to replace old value or ```~+ <alias>``` to append the value to existing ones to form an array.

#### flatten/explode

Syntax:

    <Source>.flatten/explode(<expression> [~ alias], <parameters>)

Flatten/Explode is used to transform each row into a sequence of rows, each has an object of the array selected from the original row:

    spooky.fetch(
        Wget("https://news.google.com/news?q=apple&output=rss")
    ).flatten(S"item title" ~ 'title).toDF().collect().foreach(println) //select and explode all titles in google RSS feed into multiple rows

Parameters:

| Name | Default | Means |
| ---- | ------- | ----- |
| joinType |

SpookyStuff has a shorthand for flatten + select, which is common for extracting multiple attributes and fields from objects in a list or tree:

    <Source>.flatSelect(<expression>)(
        <expression> [~ '<alias>]
        ...
    )

This is equivalent to:

    <Source>.flatten(<flatten expression> ~ 'A), <flatten parameters>
    ).select(
        <select expression> [~ '<alias>]
        ...
    )

e.g.:

    spooky.fetch(
        Wget("https://news.google.com/news?q=apple&output=rss")
    ).flatSelect(S"item")(
        A"title" ~ 'title
        A"pubDate" ~ 'date
    ).toDF().collect().foreach(println) //explode articles in google RSS feed and select titles and publishing date

You may notice that the first parameter of flatSelect has no alias - SpookyStuff can automatically assign a temporary alias 'A to this intermediate selector, which enables a few shorthand expressions for traversing deeper into objects being flattened. This "Big A Notation" will be used more often in join and explore.

#### join

Syntax:

    <Source>.join(<flatten expression> [~ alias], <flatten parameters>
    )(<Action(s)>, <fetch parameters>)(
        <select expression> [~ '<alias>]
        ...
    )

Join is used to horizontally combine data from different sources based on one-to-many/many-to-many relationship, e.g. join can combine horizontally data on search/index page with fulltext contents:

    spooky.create("Gladiator" :: Nil).fetch(
        Wget("http://lookup.dbpedia.org/api/search/KeywordSearch?QueryClass=film&QueryString='{_}")
    ).join(S"Result")(
        Wget(A"URI".text),2
    )(
        A"Label".text ~ 'label,
        S"span[property=dbo:abstract]".text ~ 'abstract
    ).toDF().collect().foreach(println) //this search for movies named "Gladiator" on dbpedia Lookup API (http://wiki.dbpedia.org/projects/dbpedia-lookup) and link to their entity pages to extract their respective abstracts

Join is a shorthand for flatten/explode + fetch.

Parameters:

(See [flatten/explode parameters] and [fetch parameters])

#### explore

Syntax:

    <Source>.explore(<expression> [~ alias], <flatten parameters>, <explore parameters>
    )(<Action(s)>, <fetch parameters>)(
        <select expression> [~ '<alias>]
        ...
    )

Explore defines a parallel graph exploring pattern that can be best described as recursive join with deduplication: In each iteration, each row is joined with one or more documents, they are compared and merged with existing documents that has been traversed before, the iteration stops when maximum exploration depth has been reached, or no more new documents are found. Finally, all documents and their rows are concatenated vertically. Explore is used for conventional web crawling or to uncover "deep" connections in a graph of web resources, e.g. pagination, multi-tier taxonomy/disambiguation pages, "friend-of-a-friend" (FOAF) reference in a social/semantic network:

    spooky.fetch(
        Wget("http://dbpedia.org/page/Bob_Marley")
    ).explore(
        S"a[rel^=dbo],a[rev^=dbo]",
        maxDepth = 2
    )(Wget('A.href)
    )('A.text ~ 'name).toDF().collect().foreach(println) // this retrieve all documents that are related to (rel) or reverse-related to (rev) dbpedia page of Bob Marley

Parameters:

| Name | Default | Means |
| ---- | ------- | ----- |
| joinType |



#### etc.

# Actions

Each action defines a web client command, such as content download, loading a page in browser, or clicking a button.

Action can be chained to define a series of command in a client session, e.g:

    spooky.fetch(
        Visit("http://www.google.com/")
          +> TextInput("input[name=\"q\"]","Deep Learning")
          +> Submit("input[name=\"btnG\"]")
          +> Snapshot()
    ).flatMap(S.text).collect().foreach(println) //in a browser session, search "Deep Learning" on Google's landing page

In addition to the "chaining" operator **+>**, the following 2 can be used for branching:

- **||**: Combine multiple actions and/or chains in parallel, each executed in its own session and in parallel. This multiplies the cardinality of output with a fixed factor, e.g.:

    spooky.create("I'm feeling lucky"
    ).fetch(
        Wget("http://api.mymemory.translated.net/get?q='{_}&langpair=en|fr") ||
        Wget("http://api.mymemory.translated.net/get?q='{_}&langpair=en|de")
    ).select((S \ "responseData" \ "translatedText" text) ~ 'translated).toDF().collect().foreach(println) // load french and german translation of a sentence in 2 rows.

- **\*>**: Cross/Cartesian join and concatenate a set of actions/chains with another set of actions/chains, resulting in all their combinations being executed in parallel. e.g.:

    fetch(
        Visit("https://www.wikipedia.org/")
          +> TextInput("input#searchInput","Mont Blanc")
          *> DropDownSelect("select#searchLanguage","en") || DropDownSelect("select#searchLanguage","fr")
          +> Submit("input[type=submit]")
          +> Snapshot()
    ).flatMap(S.text).collect().foreach(println) // in 2 browser session, search "Mont Blanc" in English and French on Wikipedia's landing page

All out-of-the-box actions are categorized and listed in the following sections. parameters enclosed in square brackets are **[optional]** and are explained in [Optional Parameter Section]. If you are still not impressed and would like to write your own action, please refer to [Writing Extension Section].

#### Client & Thread

| Syntax | Means |
| ---- | ------- |
| Wget(<URI>, [<filter>]) [~ '<alias>] | retrieve a document by its universal resource identifier (URI), whether its local or remote and the client protocol is determined by schema of the URI, currently, supported schema/protocals are http, https, ftp, file, hdfs, s3, s3n and all Hadoop-compatible file systems.* |
| OAuthSign(<Wget>) | sign the http/https request initiated by Wget by OAuth, using credential provided in SpookyContext, not effective on other protocols |
| Delay([<delay>]) | hibernate the client session for a fixed duration |
| RandomDelay([<delay>, <max>]) | hibernate the client session for a random duration between <delay> and <max> |

* local resources (file, hdfs, s3, s3n and all Hadoop-compatible file systems) won't be cached: their URIs are obviously not "universal", plus fetching them directly is equally fast.

* http and https clients are subjective to proxy option in SpookyContext, see [Configuration Section] for detail.

* Wget is the most common action as well as the quickest. In many cases its the only action resolved in a fetch, join or explore, for which cases 3 shorthands clauses can be used:

        <Source>.wget(<URI>, <>)

        <Source>.wgetJoin(<selector>, <>)

        <Source>.wgetExplore(<>)

#### Browser

The following actions are resolved against a browser launched at the beginning of the session. The exact type and specifications (screen resolution, image/javascript supports) of the browser being launched and its proxy setting are subjective to their respective options in SpookyContext. Launching the browser is optional and necessity driven: the session won't launch anything if there is no need.

| Syntax | Means |
| ---- | ------- |
| Visit(<URI>, [<delay>, <blocking>]) | go to the website denoted by specified URI in the browser, usually performed by inserting the URI into the browser's address bar and press enter |
| Snapshot([<filter>]) [~ '<alias>] | export current document in the browser, the document is subjective to all DOM changes caused by preceding actions, all exported documents will be added into the document buffer of each row of the resulted PageFrame in sequence |
| Screenshot([<filter>]) [~ '<alias>] | export screenshot of the browser as an image document in PNG format |
| Assert([<filter>]) | Same as Snapshot, but this is only a dry-run which ensures that current document in the browser can pass the <filter>, nothing will be exported or cached |
| Click(<selector>, [<delay>, <blocking>]) | perform 1 mouse click on the 1st visible element that satisfy the jQuery <selector> |
| ClickNext(<selector>, [<delay>, <blocking>]) | perform 1 mouse click on the 1st element that hasn't been clicked before within the session, each session keeps track of its own history of interactions and elements involved |
| Submit(<selector>, [<delay>, <blocking>]) |  submit the form denoted by the 1st element that satisfy the jQuery <selector>, this is usually performed by clicking, but can also be done by other interactions, like pressing enter on a text box |
| TextInput(<selector>, <text>, [<delay>, <blocking>]) | focus on the 1st element (usually but not necessarily a text box) that satisfy the jQuery <selector>, then insert/paste the specified text |
| DropDownSelect(<selector>, <text>, [<delay>, <blocking>]) | focus on the 1st selectable list that satisfy the jQuery <selector>, then select the item with specified value |
| ExeScript(<script>, [<selector>, <delay>, <blocking>]) | run javascript program against the 1st element that satisfy the jQuery <selector>, or against the whole document if <selector> is set to null or missing |
| DragSlider(<selector>, <percentage>, <handleSelector>, [<delay>, <blocking>]) | drag a slider handle denoted by <handleSelector> to a position defined by a specified percentage and an enclosing bounding box denoted by <selector> |
| WaitFor(<selector>) | wait until the 1st element that satisfy the jQuery <selector> appears |

* Visit +> Snapshot is the second most common action that does pretty much the same thing as Wget on HTML resources - except that it can render dynamic pages and javascript. Very few formats other than HTML/XML are supported by browsers without plug-in so its mandatory to fetch images and PDF files by Wget only. In addition, its much slower than Wget for obvious reason. If Visit +> Snapshot is the only chained action resolved in a fetch, join or explore, 3 shorthand clauses can be used:

        <Source>.visit(<URI>, <>)

        <Source>.visitJoin(<selector>, <>)

        <Source>.visitExplore(<>)

* Multiple Snapshots and Screenshots can be defined in a chain of actions to persist different states of a browser session, their exported documents can be specifically referred by their aliases.

#### Flow Control

| Syntax | Means |
| ---- | ------- |
| Loop(<Action(s)>, [<limit>]) | Execute the specified <Action(s)> repeatedly until max iteration is reached or an exception is thrown during the execution (can be triggered by Assert) |
| Try(<Action(s)>, [<retries>, <cacheFailed>]) | By default, if an exception is thrown by an action, the entire session is retried, potentially by a different Spark executor on a different machine, this failover handling process continues until **spark.task.maxFailures** property has been reached, and the entire query failed fast. However, if the <Action(s)> throwing the exception is enclosed in the **Try** block, its last failure will be tolerated and only results in an error page being exported as a placeholder. Make sure to set **spark.task.maxFailures** to be larger than <retries>, or you query will fail fast before it even had a chance to interfere! |
| If(<condition>, <Action(s) if true>, <Action(s) if false>) | perform a condition check on current document in the browser, then execute different <Action(s)> based on its result |

* Loop(ClickNext +> Snapshot) is generally used for turning pages when its pagination is implemented in AJAX rather than direct hyperlinks, in which case a shorthand is available:

#### Optional Parameters

| Name | Default | Means |
| ---- | ------- | ----- |
| <filter> | org.tribbloid.spookystuff.dsl.ExportFilters.MustHaveTitle |
| <alias> | <same as Action> |
| <timeout> | null |
| <delay> | 1.second |
| <blocking> | true |
| <limit> | 500 |
| <retries> | 3 |
| <cacheError> | false |

# Expressions



// columns

#### unstructured document parsing and traversing

#### string interpolation

#### functions and operators

# I/O



# More



# Profiling



# Examples

