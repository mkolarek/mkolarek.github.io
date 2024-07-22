+++
title = 'Asking questions (pt. 1)'
date = 2024-07-19T08:00:00+02:00
tags = ['data engineering', 'trivia']
ShowReadingTime = true

[cover]
image = 'mindmaze.jpg'
alt = "Mindmaze - Copyright Microsoft 1993"

# publishDate =
# lastmod = 
# showtoc = 
+++

I've always loved quizzes and trivia, and, when I was a kid, one of my favorite games was [MindMaze](https://www.kotaku.com.au/2020/07/encarta-mindmaze-94-95-the-kotaku-australia-review/). For those of you who aren't familiar, MindMaze was a trivia game that was published as a part of Microsoft's Encarta, a digital multimedia encyclopedia. Ever since Wikipedia launched though, encyclopedias like Encarta became less popular, since they were published in fixed, yearly iterations, were published on CDs, and, well, they cost money. The worst part about it though was that meant no more MindMaze!

So this got me thinking, why couldn't we have a Wikipedia-based MindMaze clone? The most important part of this project are, of course, the questions.

With the advent of LLMs, generating all kinds of text, including questions, has become easy and accessible. For this project, we will be leveraging LLMs but we will feed our own, curated data into them in order to try and diminish the effect of hallucinations.

This is the rough plan:

1. Fetch and prepare Wikipedia data (the focus of today's post)
2. Prompt LLM for a question based on a passed-in Wikipedia article

While getting the contents of a Wikipedia article is easy, Wikipedia has politely asked its users to not scrape their pages (to avoid increased load, bot traffic, etc.). What they recommend is that users download [dumps](https://dumps.wikimedia.org/). These dumps are generated regularly and fit our use-case nicely, since we don't care that our data is super recent, and as an added benefit it allows us to completely decouple our system from Wikipedia.

One small problem with the dump is, is that it is quite large. For example, `enwiki-20240601-pages-articles-multistream.xml.bz2` takes up around 22GB of disk space (and that's compressed!). And while we can say that that's expected (there's a lot of information on Wikipedia, after all), what does pose a problem is that this whole dump is _one file_. This definitely doesn't fit our use case well. We want to be be able to easily fetch a single article's content and include it in our prompt.

So, how do we tackle this? There are many different ways we can slice up one large XML file, but since this task sounds a lot like an ETL job, I decided to use Apache Spark. The benefit of using Spark is that there are multiple integrations already available and we don't need to worry too much about how to read and write our data, while the slicing up and cleaning can be easily done with the standard DataFrame API. Another (and usually important) benefit is that, if we have the hardware available, we can run our ETL job on a Spark cluster and process all of that data more quickly.

Our plan is:

1. Extract: read in the compressed XML file into a DataFrame
2. Transform: clean up the data as necessary (drop certain rows/columns, clean up strings, etc.)
3. Load: write the data into a data store that allows easy access to individual articles (e.g., a relational database) 

You can find the full code examples in [this GitHub repository](https://github.com/mkolarek/questions).

## Data pre-processing

Before we start our work on this ETL job though, it would be a good idea to speed up our feedback cycle by getting as much information we can about our data. The goal is to not have to load all of the data every time we want to try to e.g., figure out how to access a nested column. And what can help us here the most? Knowing the schema of our data is a good start, while for speeding up our feedback cycle we can sample our data and use that sample instead of the whole thing. 

For the schema part, it's good to understand how Spark works to see how this is beneficial. When we read data into a Spark DataFrame, Spark "magically" processes and parses everything for us so we can access all columns and rows. There's no magic of course, but what Spark does do for us it tries to infer the schema of our data before it allows us to do any further processing. This step is quite resource intensive, since it requires a thorough pass through our whole dataset, but when it's done, we can store the schema separately and reuse it. This allows us to later skip the schema inferring and speeds up our full-blown ETL job.

For the sampling part, the Spark DataFrame API exposes a `.sample()` function that allows us to do exactly what we want, sample our data and generate a much smaller (and hopefully representative) data subset. A small caveat here is that we need to be careful and not fully depend on our sample, since it depends very much on the attributes of our data - if our data is very skewed and we create a very small sample, it could happen that we miss some very important outliers that are going to break our ETL job down the line. The sample is still very useful though, because we can develop our ETL job much faster and it saves us a lot of time in our initial development.

While Spark has a lot of integrations built-in, XML still isn't one of them. For Spark to be able to read in XML data, we need to use the external `spark-xml` package developed by Databricks. 

When reading files, the API has several options but the two most interesting for us are:

- `rowTag`: Which tag to treat as a row.
- `samplingRatio`: Sample ratio for schema inferring. Allows for speed up of the inferring itself.

While this looks simple enough, we've encountered a problem - how do we know which tag to treat as a row? It's kind of a "chicken-and-egg" problem, because we need to know the schema before we try to infer it. To get around this issue, we need to peek into our data a little, and we can do it with a couple of terminal tools. In our `bash` command line we run the following:

`bzip2 -dkc enwiki-20240601-pages-articles-multistream.xml.bz2 | head -n 100 | less`

What this does is:

1. `bzip2` decompresses our `.bz2` archive with `-d`, keeps our original file with `-k` and pipes the decompressed output to `stdout` with `-c`
2. `head` keeps the first `-n` rows out the piped input
3. `less` keeps the piped input in a buffer we can navigate

With a trial-and-error number of 100 for `-n` we get a nice glimpse into our data, and here we have two big discoveries:

- it looks like `page` is an XML tag that could nicely split our data on a per row basis
- there's an XML schema linked!

We could, in theory, use the linked XML schema instead of trying to infer it, but unfortunately the Python API of our `spark-xml` package doesn't nicely support passing in XSD files yet. 

Still, we now have a good `rowTag` candidate and we can start with our schema inferring and sampling. Here's the schema inferring script:

```python
import argparse
import json

from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("--input", type=str, help="Input XML file path.")
parser.add_argument("--output_schema", type=str, help="Output schema file path.")

args = parser.parse_args()

spark = SparkSession.builder.appName("Infer schema").getOrCreate()

df = (
    spark.read.format("xml")
    .options(rowTag="page")
    .options(samplingRatio=0.0001)
    .load(args.input)
)

schema = df.schema.json()
schema_json = json.loads(schema)

with open(args.output_schema, "w") as fp:
    json.dump(schema_json, fp)
```

And this is the sampling script:

```python
import argparse
import json

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

parser = argparse.ArgumentParser()
parser.add_argument("--input", type=str, help="Input XML file path.")
parser.add_argument("--input_schema", type=str, help="Input schema file path.")
parser.add_argument("--output_sample", type=str, help="Output sample directory path.")

args = parser.parse_args()

spark = SparkSession.builder.appName("Sample data").getOrCreate()

with open(args.input_schema) as f:
    schema_json = json.load(f)

schema = StructType.fromJson(schema_json)

df = (
    spark.read.format("xml")
    .options(rowTag="page")
    .schema(schema)
    .options(samplingRatio=0.0001)
    .load(args.input)
)

sample = df.sample(0.0001)

(
    sample.write.format("xml")
    .options(rowTag="page")
    .options(rootTag="pages")
    .options(compression="bzip2")
    .save(args.output_sample)
)
```

If you're running these scripts on your local machine, make sure to give your Spark driver lots of memory, since it's doing all of the work (there are no executors in a local setup).

## Data exploration

Let's fire up a Spark shell and have a look at what we got until now:

`pyspark --packages com.databricks:spark-xml_2.12:0.18.0 --driver-memory 7g`

We can start by loading our `schema.json` from disk and having a look:

```python
>>> import json
>>> with open("schema.json") as f:
...     schema_json = json.load(f)
...
>>> from pprint import pprint
>>> pprint(schema_json)
{'fields': [{'metadata': {}, 'name': 'id', 'nullable': True, 'type': 'long'},
            {'metadata': {}, 'name': 'ns', 'nullable': True, 'type': 'long'},
            {'metadata': {},
             'name': 'redirect',
             'nullable': True,
             'type': {'fields': [{'metadata': {},
                                  'name': '_VALUE',
                                  'nullable': True,
                                  'type': 'string'},
                                 {'metadata': {},
                                  'name': '_title',
                                  'nullable': True,
                                  'type': 'string'}],
                      'type': 'struct'}},
            {'metadata': {},
             'name': 'revision',
             'nullable': True,
             'type': {'fields': [{'metadata': {},
                                  'name': 'comment',
                                  'nullable': True,
                                  'type': 'string'},
                                 {'metadata': {},
                                  'name': 'contributor',
                                  'nullable': True,
                                  'type': {'fields': [{'metadata': {},
                                                       'name': 'id',
                                                       'nullable': True,
                                                       'type': 'long'},
                                                      {'metadata': {},
                                                       'name': 'ip',
                                                       'nullable': True,
                                                       'type': 'string'},
                                                      {'metadata': {},
                                                       'name': 'username',
                                                       'nullable': True,
                                                       'type': 'string'}],
                                           'type': 'struct'}},
                                 {'metadata': {},
                                  'name': 'format',
                                  'nullable': True,
                                  'type': 'string'},
                                 {'metadata': {},
                                  'name': 'id',
                                  'nullable': True,
                                  'type': 'long'},
                                 {'metadata': {},
                                  'name': 'minor',
                                  'nullable': True,
                                  'type': 'string'},
                                 {'metadata': {},
                                  'name': 'model',
                                  'nullable': True,
                                  'type': 'string'},
                                 {'metadata': {},
                                  'name': 'origin',
                                  'nullable': True,
                                  'type': 'long'},
                                 {'metadata': {},
                                  'name': 'parentid',
                                  'nullable': True,
                                  'type': 'long'},
                                 {'metadata': {},
                                  'name': 'sha1',
                                  'nullable': True,
                                  'type': 'string'},
                                 {'metadata': {},
                                  'name': 'text',
                                  'nullable': True,
                                  'type': {'fields': [{'metadata': {},
                                                       'name': '_VALUE',
                                                       'nullable': True,
                                                       'type': 'string'},
                                                      {'metadata': {},
                                                       'name': '_bytes',
                                                       'nullable': True,
                                                       'type': 'long'},
                                                      {'metadata': {},
                                                       'name': '_sha1',
                                                       'nullable': True,
                                                       'type': 'string'},
                                                      {'metadata': {},
                                                       'name': '_xml:space',
                                                       'nullable': True,
                                                       'type': 'string'}],
                                           'type': 'struct'}},
                                 {'metadata': {},
                                  'name': 'timestamp',
                                  'nullable': True,
                                  'type': 'timestamp'}],
                      'type': 'struct'}},
            {'metadata': {},
             'name': 'title',
             'nullable': True,
             'type': 'string'}],
 'type': 'struct'}
```

So far so good! Next, we need to convert it from a Python dictionary object into a Spark `StructType`:


```python

>>> from pyspark.sql.types import StructType
>>> schema = StructType.fromJson(schema_json)
>>> schema
StructType([StructField('id', LongType(), True), StructField('ns', LongType(), True), StructField('redirect', StructType([StructField('_VALUE', StringType(), True), StructField('_title', StringType(), True)]), True), StructField('revision', StructType([StructField('comment', StringType(), True), StructField('contributor', StructType([StructField('id', LongType(), True), StructField('ip', StringType(), True), StructField('username', StringType(), True)]), True), StructField('format', StringType(), True), StructField('id', LongType(), True), StructField('minor', StringType(), True), StructField('model', StringType(), True), StructField('origin', LongType(), True), StructField('parentid', LongType(), True), StructField('sha1', StringType(), True), StructField('text', StructType([StructField('_VALUE', StringType(), True), StructField('_bytes', LongType(), True), StructField('_sha1', StringType(), True), StructField('_xml:space', StringType(), True)]), True), StructField('timestamp', TimestampType(), True)]), True), StructField('title', StringType(), True)])
```

And finally we can use it to load our sample:

```python
>>> df = spark.read.format("xml").schema(schema).load("sample")
>>> df.show()
+--------+---+--------------------+--------------------+--------------------+
|      id| ns|            redirect|            revision|               title|
+--------+---+--------------------+--------------------+--------------------+
| 1213819|  0|{NULL, Human Righ...|{fix double redir...|China's Assessmen...|
| 1224425|  0|{NULL, Symphony N...|{Redirect, {53329...|              KV 183|
| 1226555|  0|{NULL, Rod (optic...|{Bot: Fixing doub...|       Rosswell rods|
| 1228412|  0|                NULL|{NULL, {40431968,...|Geki (racing driver)|
| 1236094|  0|{NULL, Kingsford ...|{#REDIRECT [[King...|   Kingsford Heights|
| 3743075| 14|                NULL|{Commonscat templ...|Category:Child si...|
| 3761201|  0|                NULL|{Fixed typos (via...|     Kamikawa, Hyōgo|
|33572252|  0|  {NULL, The Singer}|{[[WP:AES|←]]Redi...|  The Singer (album)|
|33576804|  0|    {NULL, Tanghulu}|{Bot: Fixing doub...|       Bingtang Hulu|
|21832459|  0|{NULL, Schürzenjä...|{+{{[[Template:Au...|Treff' ma uns in ...|
|21835799| 14|                NULL|{Removing from [[...|Category:Tullamor...|
|21885085| 10|                NULL|{-fixed-, {107928...|Template:Gmina Wi...|
|21909541| 14|                NULL|{Action complete,...|Category:FL-Class...|
|21911312|  0|{NULL, Cantons of...|{+{{[[Template:Re...|         Half canton|
|21934089|  0|                NULL|{Format administr...|            Ubiedrze|
|   78453|  0|{NULL, Elliptic o...|{NULL, {4388, NUL...|    Elliptical orbit|
|   79069|  0|{NULL, Foreign re...|{/* top */R from ...|Oman/Transnationa...|
|  925352|  0|                NULL|{Source added, {4...|        Jackie Lance|
| 4688933|  0|   {NULL, Rise time}|{replace by redir...|     Transition time|
| 4704638|  0|{NULL, Cisplatine...|{Double redirect,...|   War of Cisplatina|
+--------+---+--------------------+--------------------+--------------------+
only showing top 20 rows
```

Since we've both cut down our data set by a factor of 10,000, and we're providing the schema, loading the sample should be very quick. The top 20 rows will obviously be very different for each sample, but what's important is that we can see some useful data here.

One thing that's really interesting here is the `redirect` column. It seems that that these rows don't have the actual article content, so we can safely filter them out. Another interesting thing is that `redirect` and `revision` are nested columns. Thankfully, the `spark-xml` package is clever enough that it can handle this kind of structured data for us, so we can access the internal fields of these columns.

We can filter the redirect rows with `.filter()`:

```python
>>> df.filter(df.redirect._title.isNull()).show()
+--------+---+--------+--------------------+--------------------+
|      id| ns|redirect|            revision|               title|
+--------+---+--------+--------------------+--------------------+
| 1228412|  0|    NULL|{NULL, {40431968,...|Geki (racing driver)|
| 3743075| 14|    NULL|{Commonscat templ...|Category:Child si...|
| 3761201|  0|    NULL|{Fixed typos (via...|     Kamikawa, Hyōgo|
|21835799| 14|    NULL|{Removing from [[...|Category:Tullamor...|
|21885085| 10|    NULL|{-fixed-, {107928...|Template:Gmina Wi...|
|21909541| 14|    NULL|{Action complete,...|Category:FL-Class...|
|21934089|  0|    NULL|{Format administr...|            Ubiedrze|
|  925352|  0|    NULL|{Source added, {4...|        Jackie Lance|
| 4739745|  0|    NULL|{/* David Cameron...|     The Eton Rifles|
|67557001|  4|    NULL|{/* Another Day, ...|Wikipedia:Article...|
|67586080| 10|    NULL|{NULL, {45098938,...|Template:Ranks an...|
|67607720| 14|    NULL|{NULL, {3006008, ...|Category:April 20...|
|67642649|  0|    NULL|{expanded, {12149...| Grevillea polyacida|
|59459561| 10|    NULL|{Hamilton scored ...|Template:2019 CFL...|
|59468771| 14|    NULL|{[[WP:AES|←]]Crea...|Category:Churches...|
|40819993|  4|    NULL|{Fixed [[WP:LINT|...|Wikipedia:Article...|
|40887931|  0|    NULL|{Change Taxobox t...|Pseudatteria marm...|
|40911554|  0|    NULL|{NULL, {36336112,...|   Pusté Žibřidovice|
|26327965|  0|    NULL|{Adding local [[W...|Gongyi South rail...|
|  585838|  0|    NULL|{Added date. | [[...|List of English w...|
+--------+---+--------+--------------------+--------------------+
only showing top 20 rows
```

And we can add the nested column fields with `.withColumns()`:

```python
>>> df.withColumns({"text": df.revision.text}).show()
+--------+---+--------------------+--------------------+--------------------+--------------------+
|      id| ns|            redirect|            revision|               title|                text|
+--------+---+--------------------+--------------------+--------------------+--------------------+
| 1213819|  0|{NULL, Human Righ...|{fix double redir...|China's Assessmen...|{#REDIRECT [[Huma...|
| 1224425|  0|{NULL, Symphony N...|{Redirect, {53329...|              KV 183|{#REDIRECT [[Symp...|
| 1226555|  0|{NULL, Rod (optic...|{Bot: Fixing doub...|       Rosswell rods|{#REDIRECT [[Rod ...|
| 1228412|  0|                NULL|{NULL, {40431968,...|Geki (racing driver)|{{{Short descript...|
| 1236094|  0|{NULL, Kingsford ...|{#REDIRECT [[King...|   Kingsford Heights|{#REDIRECT [[King...|
| 3743075| 14|                NULL|{Commonscat templ...|Category:Child si...|{{{Commonscat|Chi...|
| 3761201|  0|                NULL|{Fixed typos (via...|     Kamikawa, Hyōgo|{{{Refimprove|dat...|
|33572252|  0|  {NULL, The Singer}|{[[WP:AES|←]]Redi...|  The Singer (album)|{#REDIRECT [[The ...|
|33576804|  0|    {NULL, Tanghulu}|{Bot: Fixing doub...|       Bingtang Hulu|{#REDIRECT [[Tang...|
|21832459|  0|{NULL, Schürzenjä...|{+{{[[Template:Au...|Treff' ma uns in ...|{#REDIRECT [[Schü...|
|21835799| 14|                NULL|{Removing from [[...|Category:Tullamor...|{[[Gaelic footbal...|
|21885085| 10|                NULL|{-fixed-, {107928...|Template:Gmina Wi...|{{{Navbox\n|name=...|
|21909541| 14|                NULL|{Action complete,...|Category:FL-Class...|{{{WikiProject Te...|
|21911312|  0|{NULL, Cantons of...|{+{{[[Template:Re...|         Half canton|{#REDIRECT [[Cant...|
|21934089|  0|                NULL|{Format administr...|            Ubiedrze|{{{Infobox settle...|
|   78453|  0|{NULL, Elliptic o...|{NULL, {4388, NUL...|    Elliptical orbit|{#REDIRECT [[Elli...|
|   79069|  0|{NULL, Foreign re...|{/* top */R from ...|Oman/Transnationa...|{#REDIRECT [[Fore...|
|  925352|  0|                NULL|{Source added, {4...|        Jackie Lance|{{{short descript...|
| 4688933|  0|   {NULL, Rise time}|{replace by redir...|     Transition time|{#REDIRECT [[Rise...|
| 4704638|  0|{NULL, Cisplatine...|{Double redirect,...|   War of Cisplatina|{#redirect [[Cisp...|
+--------+---+--------------------+--------------------+--------------------+--------------------+
only showing top 20 rows
```

## Data processing

We're quite close now! We have our basic article data, like the title and the text, and we've removed some unnecessary rows. The last building block for our ETL job is loading the data into some kind of data store. 

There are really a lot of options for storing our data, but for our use-case it makes sense to stick to something that's simple to set up and reliable. That's why I've decided for PostgreSQL. I won't go much into details on how I've it set it up, since I don't think that's too interesting. If you are interested, you can look it up in the source code of this project.

Finally, let's put everything together into our full-blown Spark job:

```python
import argparse
import json

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, ArrayType

parser = argparse.ArgumentParser()
parser.add_argument("--input", type=str, help="Input XML file path.")
parser.add_argument("--input_schema", type=str, help="Input schema file path.")

args = parser.parse_args()

spark = SparkSession.builder.appName("Load to Postgres").getOrCreate()


def flatten(schema, prefix=None):
    fields = []
    for field in schema.fields:
        name = (
            prefix + "." + "`" + field.name + "`" if prefix else "`" + field.name + "`"
        )
        dtype = field.dataType
        if isinstance(dtype, ArrayType):
            dtype = dtype.elementType
        if isinstance(dtype, StructType):
            fields += flatten(dtype, prefix=name)
        else:
            fields.append(
                name
                + " AS "
                + name.replace("`", "").replace(".", "_").replace(":", "_")
            )
    return fields


with open(args.input_schema) as f:
    schema_json = json.load(f)

schema = StructType.fromJson(schema_json)

df = spark.read.format("xml").options(rowTag="page").schema(schema).load(args.input)

df.filter(df.redirect._title.isNull()).createOrReplaceTempView("wiki")

flattened = spark.sql("SELECT {} FROM wiki".format(", ".join(flatten(schema))))

(
    flattened.write.format("jdbc")
    .option("url", "jdbc:postgresql://localhost/wiki")
    .option("dbtable", "wiki.wiki")
    .option("user", "wiki")
    .option("password", "wiki")
    .option("driver", "org.postgresql.Driver")
    .save()
)
```

The reading in of the data and the filtering should be familiar, but we've switched to the Spark SQL API instead of using the DataFrame API for exposing the nested columns. The reason for this is that there is no built-in way to "flatten" a nested structure like we have, so we needed to resort to a custom recursion. This gives us an automated way to discover all nested fields and to name them appropriately, cleaning up any unsupported characters we might encounter. And since we're working with strings, it's easy to build a SQL query that targets the nested fields and assigns aliases. 

## Wrapping up

This is a good place to stop for now, since we have our data in a nice and workable state. Stay tuned for the second part, where we will continue with querying our data against our locally deployed LLM!

Cheers,  
Marko
