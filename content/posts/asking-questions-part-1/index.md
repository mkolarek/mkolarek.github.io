+++
title = 'Asking questions (pt. 1)'
date = 2024-07-19T08:00:00+02:00
tags = ['Data Engineering', 'Trivia']
+++

I've always loved quizzes and trivia. As a kid, one of my favorite games was [MindMaze](https://www.kotaku.com.au/2020/07/encarta-mindmaze-94-95-the-kotaku-australia-review/). For those unfamiliar, MindMaze was a trivia game included in Microsoft's Encarta, a digital multimedia encyclopedia. However, with the advent of Wikipedia, encyclopedias like Encarta became less popular. They were published in fixed, yearly iterations, distributed on CDs, and, most importantly, they cost money. The decline of Encarta also meant the end of MindMaze, which was disappointing for trivia enthusiasts like me.

This got me thinking: why not create a Wikipedia-based MindMaze clone? The most crucial part of such a project is, of course, the questions.

With the rise of LLMs, generating various types of text, including questions, has become both easy and accessible. For this project, we will leverage LLMs but feed them curated data to minimize hallucinations.

Here’s the rough plan:

1. Fetch and prepare Wikipedia data (the focus of today’s post).
2. Prompt the LLM to generate a question based on a given Wikipedia article.

While accessing the contents of a Wikipedia article is straightforward, Wikipedia explicitly requests users not to scrape their pages to avoid increased load and bot traffic. Instead, they recommend downloading [dumps](https://dumps.wikimedia.org/). These dumps are generated regularly and suit our use case well since we don’t need the most up-to-date data. Additionally, using dumps allows us to completely decouple our system from Wikipedia.

One challenge with these dumps is their size. For instance, `enwiki-20240601-pages-articles-multistream.xml.bz2` is approximately 22GB compressed. While this size is expected given the vast amount of information on Wikipedia, the real issue is that the entire dump is contained in a single file. This format doesn’t align well with our use case, as we need to easily fetch the content of individual articles to include in our prompts.

So, how do we address this? There are many ways to split a large XML file, but since this task resembles an ETL job, I decided to use Apache Spark. Spark offers multiple integrations, simplifying the process of reading and writing data. Additionally, the slicing and cleaning can be efficiently handled using the standard DataFrame API. If the necessary hardware is available, we can even run the ETL job on a Spark cluster to process the data more quickly.

Our plan is as follows:

1. Extract: Read the compressed XML file into a DataFrame.
2. Transform: Clean the data as needed (e.g., drop certain rows/columns, clean up strings).
3. Load: Write the data into a datastore that allows easy access to individual articles (e.g., a relational database).

You can find the full code examples in [this GitHub repository](https://github.com/mkolarek/questions).

## Data Pre-Processing

Before diving into the ETL job, it’s a good idea to speed up our feedback cycle by gathering as much information as possible about our data. The goal is to avoid loading the entire dataset every time we need to, for example, figure out how to access a nested column. Two strategies can help here: understanding the schema of our data and using a sample instead of the full dataset.

Understanding the schema is crucial. When Spark reads data into a DataFrame, it infers the schema, allowing us to access all columns and rows. However, schema inference is resource-intensive as it requires a thorough pass through the entire dataset. Once inferred, we can store the schema separately and reuse it, skipping the inference step and speeding up the ETL job.

Sampling is another useful strategy. The Spark DataFrame API provides a `.sample()` function, enabling us to create a smaller, representative subset of the data. However, we must be cautious not to rely entirely on the sample, as it may miss important outliers, especially if the data is skewed. Despite this limitation, sampling significantly accelerates initial development and saves time.

While Spark includes many built-in integrations, XML is not one of them. To read XML data, we need the external `spark-xml` package developed by Databricks.

When reading files, the API offers several options, but the two most relevant for us are:

- `rowTag`: Specifies which tag to treat as a row.
- `samplingRatio`: Defines the sample ratio for schema inference, speeding up the process.

This seems straightforward, but there’s a catch: how do we determine which tag to treat as a row? It’s a classic “chicken-and-egg” problem because we need to know the schema before inferring it. To resolve this, we can peek into the data using a few terminal tools. Running the following command in the `bash` terminal provides a glimpse into the data:

`bzip2 -dkc enwiki-20240601-pages-articles-multistream.xml.bz2 | head -n 100 | less`

Here’s what each part of the command does:

1. `bzip2` decompresses the `.bz2` archive (`-d`), keeps the original file (`-k`), and pipes the decompressed output to `stdout` (`-c`).
2. `head` extracts the first `-n` rows from the piped input.
3. `less` buffers the piped input for navigation.

Using a trial-and-error approach with `-n 100`, we can inspect the data. Two key observations emerge:

- The `page` tag appears to be a suitable candidate for splitting the data into rows.
- An XML schema is linked within the data.

In theory, we could use the linked XML schema instead of inferring it. However, the Python API of the `spark-xml` package doesn’t yet support passing XSD files. Nevertheless, we now have a good `rowTag` candidate and can proceed with schema inference and sampling. Here's the schema inferring script:

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