---
layout: global
title: Query - SpookyStuff SPOOKYSTUFF_VERSION documentation
---

* This will become a table of contents (this text will be scraped).
{:toc}

# Overview

Built on top of Scala, SpookyStuff's' Query Language is a short and powerful LINQ (Language-Integrated Query) that abstracts away the complexity of unstructured info traversing, link crawling and parallel computing/optimization, leaving only two things for you to consider: The data you want and the process to discover them. This leads to the first rule in terms of language design: multiple queries that return identical intended results should also be identical, or at least as close to each other as possible.

SpookyStuff borrows some concepts and most of its syntax from [Spark SQL] (e.g. inner & left join, explode, alias), notably its own Scala [Language-Integrated Query]. The purpose of which is easier understanding for SQL users, rather than draw an analogy between relational database and linked data/unstructured data. In fact, they have very different specifications: In linked data, handling expression-based reference and predicate by a client are prohibitively expensive, making URI reference and web service-integrated lookup its only standard.

The following diagram illustrates the building blocks of SpookyStuff Query: context, clauses, actions and expressions.

![img]

To run it in any Scala environment, import all members of the package org.tribbloid.spookystuff.dsl:

    import org.tribbloid.spookystuff.dsl._

# Context

SpookyContext provides a possible entry point of queries, it can be constructed from a [SparkContext] or [SQLContext], with SpookyConf as an optional parameter:

    val spooky = new SpookyContext(sc: SparkContext) // OR
    val spooky = new SpookyContext(sql: SQLContext) // OR
    val spooky = new SpookyContext(sql: SQLContext, conf: SpookyConf)

SpookyConf contains many configuration options that are enumerated in [Configuration Section], they don't affect the result of the query, but does have an impact on query's efficiency and chance of success. 

Multiple SpookyContext with different SpookyConf can be constructed and each configuration will only affect queries

    spooky.conf.

# Clauses



#### fetch

#### select

#### flatten/explode

#### join

#### explore

#### etc.

# Actions

// Chaining

#### Wget

#### Browser

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

