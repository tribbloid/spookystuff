---
layout: global
title: Development - SpookyStuff SPOOKYSTUFF_VERSION documentation
---

* This will become a table of contents (this text will be scraped).
{:toc}

# Build

The core functionality of Datapassports is to expose data collections in different "facets" based on the intention and function
of the login user, this covers the following 2 scenarios:

- Protection: If the login user's intention is to send data to another user with a different function
(senderID != targetID),
the data in question will be transformed into Data Passports, trusted objects that can only be opened by authorized users
with the right credential.

- Enforcement: If the login user\s intention is to use the data herself/himself or to send data to another user
with identical function (senderID == targetID),
The data in question will be transformed in a way that satisfies the principle of least privilege: namely, only information
necessary for the user's intended function will be provided, based on the need to know of the function.

The active senderID and targetID for a login user can be queried by the following function:

    @DP show session;

In addition, Passport Controller provides several ETL functions for concurrent moving of data between Passport Controller
and other sources. In these functions, the same protection and enforcement can be injected at any point to transform the data
before the loading, given that user has enough credential to access the other data source.

#### mvn options

# Writing Extensions

#### Actions

#### Elements

# ScalaDoc