email component ---

proxy!proxy!........................................................done!

load page from cloud drive! +++ ...............................................done!

integrate occam's machete's boilerpipe!.......................................................done!

xpath become necessary +++ ...........................................................Long!

restful api support: namely wpost, wput, wupdate & wdelete +++ .......................................Long!

support proxy in wget & restful --.....................................................done!

spike/read document on tika, or UIMA?...................UIMA is too heavyweight, hell no.................

ispark - set up blocking intepret to avoid racing condition & startup ---

implement slice(expand) ---

handle cookie! --- very likely doesn't work in multi-browser situation! remember spring security?

try StringContext ---....................................its for simplified reflective variable interpolation, not useful in our case.

Chinese tokenization.................................................Spike on Friday! Kyde: do you want to pair on this?.....................outsourced

throw exception on ClientAction that compress information and store path of the error page...................................done!

move pageLoadDeadline from const to context so it can be extended when using proxies...................................done!

filter proxy, find the useful ones............................................................................on hold!

lazy execution! wait until point of result-resolving!.........................................................done!

lazy exectuion model ----> export will render all previous interactions.......................................................done!

documentation! documentation! ewwwwwwww ++

merge terry and sandeep........................................................................................done

now extract will preserve orders of columns as identical to they are imported!...................................done

add back the old examples +++.....................................................................................done

bug1: cannot restore from cache, throw ClassNotFoundException...........................................................................done

bug2: cannot use dot in column names, cannot used as symbol...................................................won't fix until https://issues.apache.org/jira/browse/SPARK-2775 is resolved

bug3: cannot extract from other components of PageRow except Page: incomplete solution, will be completely solved after select expression is fully implemented......................done

add timeout for pages.......................................................................done

dual licensing!!!.......................................................................done

deprecate sliceJoin! delegate to flatten/explode +++ .............................................................done

element interpolation! Thanks Liu!.........................................................................................done

collect feedback! ++++ pending for too long................................................................................done

TDD -- depends on previous ---

check if pageExpireAfter is functioning +++......................................................................................done, sporadic deserialization failure, no big deal

ergodic pagination ...........................................................................................done

reference to query lambda, ask Liu for suggestion................................................................................done

support proxies in wget................................................................done, test finished

upgrade ansible PhantomJS config to 1.9.8! ..................................................................done

finish BioCompare.......................................................done, 6944245 rows

support persisting cookies in visit +

non-constructor field (metadata) of actions are lost upon interpolate, fix it!.........................done

merge reusable block & Seq[Action] operations into ActionLike trait ......................done

PageUID contains leaf that denotes the named action that generates the page................done

aggregate some action & Seq[action]'s functions to ActionLike & Trace.......................done

refactor all action chaining & flow control to Trace, this is to simplify query and enable query lambda..........................................................................................done

refactor join by Unix philosophy ..........................................done

DAAAAAAATAAAAAAAABRIIIIIIIIIIICKS certificates! +++....................................................................................DOOOOOONNNNNEEEEE!

1. Fill out a general questionnaire about the product (e.g., integration dependencies, product information, versions of Spark supported).......................done

2. Provide log files demonstrating successful build / operation of application on top of Spark...................................................done

Is Spark listed as a supported platform in the product documentation? If so, can you please provide a copy of the relevant section?.....................done

Are instructions provided as part of the product documentation for installing / configuring the product to work with Spark? If so can you please provide a copy of the relevant section?.........done

Integration Test Output:
The product must provide test output that demonstrates successful operation according to the parameters described above:
Provide a test plan description for the integration test
Describe the particular environment that the test was executed in
Provide log files from the output of the test along with a brief description.....................................................................done

need complete documentation and certificates +++........................................................................done

browser pool to avoid repeating launch and deadlock -......0.4

fix the bug where restore doesn't rename Page's name to new ones, and test it!...............done

move wget headers and content-type to SpookyContext..........................................done

integrate simple accumulator for realtime dashboard.........................................done

move all integration test to spookystuff-example.............................................done

all actions should take Expr parameters....................................................................done

investigate vmf and find their security measures...............................................done

deprecate and clean up paginate, slice & extract after their subsitutes are finished ..............................done

set functions of pages to two-stage: page -> elements -> attributes/text.......................done

...also revise Expr[_] to maximize parsimony in join....................................done

fix a bug where missing key in all elements of RDD will cause SchemaRDD.select to try to select non-existing column +++

change fetch to use distinct -> join instead of groupByKey ..................................................useless, won't preserve order anyway

preserve order by SQL automatically!.....................................................done

determine numPartitions automatically!....0.4

officially make PageSchemaRDD an RDD + ...........................................................................done

enable alias ~ for last inserted key ---..........................................useless, replaced by default key

reduce remote resource waiting time but get more driver initialization waiting time

break sc and sql from SpookyContext as they are determined by RDD......0.4

fix repl........even its useless ...............................................................................done

accumulators showing that lazy load may still initialize driver. Investigate that!.........................................not a bug: accumulator increment at wrong position

pageIndexKey should be added into keySet..................................................done

GraphX integration: supports export as Graph.....0.4

fix example....................................................................................done

...and documents.

split spooky intp from ISpark into an independent project...........................done

parsehub/kimino-ish API(probably won't be optimal, size is too big)....................................done

explore is still defective: need to remove added key that cause duplicates.................done

fetch need to read cache from self by doing a dry run on trace and join with all backtraces so explore wont' have high load.............................................................need test

point demo uri to notebook............................................................................................................done

abandon lazy execution in Session and use dryrun to try to load from cache, this can avoid latest snapshot being abandoned due to their earlier version being found in cache..........done

checkpointing seems not working properly, investigate that!...............................................................done

additional 'narrow' engine for explore.................................................................done

PageRowRDD switching to pairRDD to carry metadata and allow more efficient implementation

switch ot key-value in lookup table to leverage partitioner......................................................................done

add condition parameter to loop.........................................................................................partially, Assert can handle it

add feature to combine 2 cells to a map...................................................................................done

convert old query and run

fix macys

add html unit driver & chrome server driver, refer to Selenide if necessary........................................done

add support for Sizzle css selector to avoid breakdown, refer to Selenide if necessary.............................done

query optimization becomes an argument in fetch, join and explore. it only impacts efficiency/redundancy and has no impact on the result........................................done

bug: empty pageRow.expore will still have depthkey being inserted!.................................................................not a bug

lookup & Backtrace:
the problem is that results from 2 execution of a trace may looks consistent on UID but have incoherent content.
And lookup is too complex at the moment
solution:
	put session creation time into UID: this makes it a real "UID" as pages from 2 sessions won't share it.
	to lookup an entire exe output seq of a given Trace: one just need an extra condition to check all page's session creation times are identical
	- or aggregate by session creation time, find the latest, and do a integrity/sanity check.
	
how to avoid long delay in loops? is it possible?

add operator into/~+ for operator that do + if no field exist in cells, and append the result at the end of the cell if cell is a collection of valid type!.........................done

try different lookup table configuration and optimize to the core: should it be [Trace, Seq[PageLike]]?

metrics/accumulator is still defective, fix it! .......................................................................done

metrics.toJson to allow quick review of stats...............................................................................done

tuning smartExplore to avoid imbalanced partitioning from 1 seed page...........................................not a bug?

stage dumbExplore periodically to avoid memory overflow.........................................................................done

fix the apache org.apache.http.ProtocolException: Invalid redirect URI: bug....................................................done

recalculate the page stats & find a way to fix/circumvent saveAsTextFile error...................................................................done

appliancePartsPros always get 83 fetch from DFS failure? Investigate!.................................done

sigmaAldrich has memory/disk overflow issue in shuffle spill even without lookup, why, is it due to persist or checkpoint?...............shuffle in union, and wrong selector is used in SigmaAldrich!

add support for smartNoLookup optimizer..........................................................................................done

ISpark handling of special charater in generated md is weak, need to fix that by manual escaping or encoding them: use raw HTML element.toString for sanity test

unknown MIME type no longer cause NullPointerException but generates an empty page..................................................done

defaultParallelism now becomes configurable and is twice as large..................................................done

test of data from ambiguous pages joined from identical links has been disabled, enable them again............................................done

enable joinKey & joinLimit in explore again...................................................................................................done

pagination & index tree in all integration test are not deep enough, need a more stressful environment..............................tested on Abcam

backtrace of output by driverless should only be itself...........................................done

try Kryo serializer ....................................................................done

use nutch CrawlDB-style service to avoid low efficiency of lookup table? is it possible?................................too late to change!

the only possible way to do stability test is on extremely deep site (Abcam) and extremely wide site (Sigma-Aldrich)..............................done

install PhantomJS by maven plugin to use TravisCI .........................................not necessary

when Expr returns an array, all actions should only use the first element of it .....................................done

build a hive thrift server that can convert SPARKQL into query plan based on pre-defined prefix and RDF template

add test cases to make sure consecutive flatSelect & join won't use previous temporary values, these includes:
- flatSelect -> flatSelect
- join -> flatSelect
- explore -> flatSelect................................................................................................done

service pipeline: conformity with ML-pipeline:Transformations to enable it to be easily integrated and used

pipeline stage as a service: download pipeline stage from a public hosting service: e.g. GitHub, that is constantly maintained and versioned to upgrade with the website.

define pipeline stage as SPARQL pragma in Hive Server

OAuthIntegration in wget:

full SparkSQL integration: since its graduated from alpha it is safe to use Row and DataFrame to allow more fluent