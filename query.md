---
layout: global
title: Query - SpookyStuff SPOOKYSTUFF_VERSION documentation
---

* This will become a table of contents (this text will be scraped).
{:toc}

# Overview

Built on top of Scala, SpookyStuff's' Query Language is a short and powerful LINQ (Language-Integrated Query) that abstracts away the complexity of unstructured document parsing/traversing, link crawling and parallel computing/optimization, leaving only two things for you to consider: The data you want and the process to discover them. This leads to our first rule in syntax design: identical queries should expect identical results, and vice versa.

SpookyStuff borrows some concepts and most of its syntax from [Spark SQL] (e.g. inner & left join, explode, alias and select), notably its own [Language-Integrated Query]. This is for easier understanding and memorizing by SQL users, rather than draw an analogy between relational databases and linked/unstructured datasets. In fact, they have very different specifications: In linked data, join by expression and filter by predicate ("WHERE" clause in SQL) are prohibitively expensive if executed on the client side, which makes URI and web service-based lookup its only standard.

The following diagram illustrates the elements of SpookyStuff queries: context, clauses, actions and expressions.

![img]

To run it in any Scala environment, import all members of the package org.tribbloid.spookystuff.dsl:

    import org.tribbloid.spookystuff.dsl._

# Context

SpookyContext is the entry point of all queries, it can be constructed from a [SparkContext] or [SQLContext], with SpookyConf as an optional parameter:

    val spooky = new SpookyContext(sc: SparkContext) // OR
    val spooky = new SpookyContext(sql: SQLContext) // OR
    val spooky = new SpookyContext(sql: SQLContext, conf: SpookyConf)

SpookyConf contains configuration options that are enumerated in [Configuration Section], these won't affect the result of the query, but have an impact on execution efficiency and resiliency. all these options can be set statically and later changed dynamically, even in the middle of a query, e.g.

    import scala.concurrent.duration.Duration._

    spooky.conf.pageExpireAfter = 1.day // OR
    spooky.<clauses>.setConf(_.pageExpireAfter = 1.day).<clauses>.... // in the middle of a query

Multiple SpookyContext can co-exist and their configurations will only affect their derived queries respectively.

# Clauses

Clauses are the building block of queries, each denotes a specific pattern to discover new data from references in old data (e.g. hyperlinks). Clauses can be applied to a SpookyContext, a PageFrame (the data structure returned by a clause as the result enriched dataset, this makes chaining clauses possible), or a compatible common Spark dataset type (e.g. [DataFrame] or String RDD, need to enable implicit conversion first, this will be covered in [I/O section] in detail)

Most of the clauses returns a PageFrame as output, PageFrame is an abstraction of a distributed and immutable row-storage that contains both unstructured "raw" documents and key-value pairs. it can be easily converted to other Spark dataset types (covered in [I/O section]). In addition, it inherits SpookyContext from its predecessor, so settings in SpookyConf are persisted for all queries and clauses derived from them (unless you change them halfway, this is common if a query uses mutliple web services, each with its own Authentication credential or proxy requirement).

The following 5 main clauses covered most linking patterns in websites, documents and APIs, including one-to-one link, one-to-many link, graph link and pagination.

#### fetch

Syntax:

    <Source>.fetch(<Action(s)>, <parameters>)

Fetch is used to remotely fetch unstructured document(s) per row according to provided actions and load them into each row's document buffer, which flush out previous document(s). Fetch can be used resolve URI directly or combine data from different sources based on one-to-one relationship:

    spooky.fetch(Wget("https://www.wikipedia.com")) // this loads the landing page of Wikipedia into a PageFrame with cardinality 1.

    spooky.create(1 to 20).fetch(Wget("https://www.wikidata.org/wiki/Q'{_}")) //this loads 20 wikidata pages, 1 page per row

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
    ).select(S"table#mp-left p" ~ 'featured_article) //this load the featured article text of Wikipedia(English) into one row

If the alias of an expression already exist in the store it will throw a QueryException and refuse to execute. In this case you can use ```~! <alias>``` to replace old value or ```~+ <alias>``` to append the value to existing ones to form an array.

#### flatten/explode

Syntax:

    <Source>.flatten/explode(<expression> [~ alias], <parameters>)

Flatten/Explode is used to transform each row into a sequence of rows, each has an object of the array selected from the original row:

    spooky.fetch(
        Wget("https://news.google.com/news?q=apple&output=rss")
    ).flatten(S"item title" ~ 'title) //select and explode all titles in google RSS feed into multiple rows

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
    )//explode articles in google RSS feed and select titles and publishing date

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
    ) //this search for movies named "Gladiator" on dbpedia Lookup API (http://wiki.dbpedia.org/projects/dbpedia-lookup) and link to their entity pages for their abstracts

Join is a shorthand for flatten/explode + fetch.

Parameters:

(See [flatten/explode parameters] and [fetch parameters])

#### explore

Syntax:

    <Source>.explore(<expression> [~ alias], <explore parameters>
    )(<Action(s)>, <fetch parameters>)(
        <select expression> [~ '<alias>]
        ...
    )

Explore defines a parallel graph-traversing pattern that can be best described as recursive join with deduplication: In each iteration, each row is joined with one or more documents, they are compared and merged with existing documents that has been traversed before, the iteration stops when maximum exploration depth has been reached, or all fetched documents already exist. Finally, all documents and their rows are concatenated vertically. Explore is used for conventional web crawling or to uncover "deep" connections in a graph of web resources, e.g. paginationn, multi-tier taxonomy/disambiguation pages, "friend-of-a-friend" (FOAF) reference in a social/semantic network:

    spooky.fetch(

    )

#### etc.

# Actions

#### Wget

#### Browser

// Chaining

#### Flow Control

# Expressions

// columns

#### document selector

#### string interpolation

#### functions

# I/O



# More



# Profiling



# Examples

