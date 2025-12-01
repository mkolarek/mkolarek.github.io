+++
title = 'Asking questions (pt. 2)'
date = 2024-08-16T10:00:00+02:00
+++

Last time, we discussed how to prepare the data for our question-generating system. To recap, here was the rough plan:

1. Fetch and prepare Wikipedia data (see more [in my previous post](https://mkolarek.github.io/posts/asking-questions-pt-1/))
2. Use an LLM to generate questions based on a given Wikipedia article (the focus of today's post)

There are numerous LLM services available online, but for our purposes, we'll use an open-source solution that can be run locally on our machine: [ollama](https://ollama.com). This tool acts as a wrapper around popular, freely available models, providing a uniform API and CLI while managing model downloads for us.

Our approach to using the LLM involves:

1. Deploying a model locally (e.g., `llama3`)
2. Querying our database for a random Wikipedia article
3. Passing the contents of the Wikipedia article as part of a prompt to the model
4. Receiving a multiple-choice question based on the article

## Deploying a model using ollama

To ensure a clean and reproducible setup, we'll use the official Docker image of `ollama`.

Running the container:

```
docker run -d --gpus=all -v ollama:/root/.ollama -p 11434:11434 --name ollama ollama/ollama
```

Accessing the prompt interface:

```
docker exec -it ollama ollama run llama3
```

(Note: The first run may take longer as the model file needs to be downloaded, which can be quite large.)

Here, we can test a few prompts to ensure everything works as expected. If you have a compatible GPU, model inference should be faster.

```
>>> Why is the sky blue? Answer briefly, please.
The sky appears blue because shorter wavelengths of light (like blue and violet) are scattered
more efficiently by tiny molecules in the atmosphere than longer wavelengths (like red and
orange).
```

Let's also test the API:

```bash
$ curl http://localhost:11434/api/generate -d '{
  "model": "llama3",
  "prompt": "Why is the sky blue? Answer briefly, please.",
  "stream": false
}' | jq .
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100  1028  100   926  100   102    126     13  0:00:07  0:00:07 --:--:--   295
{
  "model": "llama3",
  "created_at": "2024-08-12T13:03:44.398010891Z",
  "response": "The sky appears blue because of a phenomenon called Rayleigh scattering, where shorter (blue) wavelengths of light are scattered more than longer (red) wavelengths by tiny molecules of gases in the atmosphere, like nitrogen and oxygen. This scattering effect gives the sky its blue color!",
  "done": true,
  "done_reason": "stop",
  "context": [
    128006,
    882,
    128007,
    271,
    10445,
    374,
    279,
    13180,
    6437,
    30,
    22559,
    27851,
    11,
    4587,
    13,
    128009,
    128006,
    78191,
    128007,
    271,
    791,
    13180,
    8111,
    6437,
    1606,
    315,
    264,
    25885,
    2663,
    13558,
    64069,
    72916,
    11,
    1405,
    24210,
    320,
    12481,
    8,
    93959,
    315,
    3177,
    527,
    38067,
    810,
    1109,
    5129,
    320,
    1171,
    8,
    93959,
    555,
    13987,
    35715,
    315,
    45612,
    304,
    279,
    16975,
    11,
    1093,
    47503,
    323,
    24463,
    13,
    1115,
    72916,
    2515,
    6835,
    279,
    13180,
    1202,
    6437,
    1933,
    0
  ],
  "total_duration": 7280177133,
  "load_duration": 16705909,
  "prompt_eval_count": 21,
  "prompt_eval_duration": 326095000,
  "eval_count": 55,
  "eval_duration": 6880157000
}
```

Here, we've sent a request via `curl` containing the same prompt. However, we've requested a non-streaming response (by setting `stream` to false), so we receive the entire response at once.

## Getting a random Wikipedia article from our database

The simplest approach is to pick a random number (X) between 1 and N, where N is the total number of Wikipedia articles in our database, and then query the contents of the X-th article.

Before doing this, we need to assign each article an incremental ID. Although our original dataset includes an ID, it isn't sequential, and some articles (and their IDs) were removed during filtering.

To address this, launch the `psql` interface and run:

```SQL
wiki=>ALTER TABLE wiki.wiki ADD row_id SERIAL;
ALTER
```

(This process may take some time.)

We have now added a new column `row_id` to our `wiki` table, which is of the (pseudo-)type `SERIAL`, meaning it auto-increments.

To query a random article's title:

```SQL
wiki=>SELECT title 
FROM wiki.wiki 
WHERE row_id = (
    SELECT floor(random() * COUNT(title) + 1)::int 
    FROM wiki.wiki
);
         title
------------------------
 Simply Red discography
(1 row)
```

Running this query multiple times confirms that we get different random articles. However, some article titles start with the prefix `Category:`, which are meta-articles rather than "true" articles. These aren't useful for our use case, so we should remove them from our database.

### Some ad-hoc data cleaning

Let's check for other similar meta-articles:

```SQL
SELECT DISTINCT(SUBSTRING(title from '.*\:[^ ]')) 
FROM wiki.wiki 
WHERE title LIKE '%:%';
```

This query looks for article titles starting with a string followed by a colon and another string, e.g., `Category:Women of Belgium by activity`. It extracts the part before the colon, e.g., `Category:`.

The results show valid article titles as well as meta-articles:

```
Wikipedia:
Template:
Draft:
Category:
Portal:
File:
```

To clean these up:

```SQL
wiki=>DELETE FROM wiki.wiki 
WHERE title LIKE 'Wikipedia:%' 
    OR title LIKE 'Template:%' 
    OR title LIKE 'Draft:%' 
    OR title LIKE 'Category:%' 
    OR title LIKE 'Portal:%' 
    OR title LIKE 'File:%';
```

Unfortunately, this breaks our `row_id` sequence due to gaps in the IDs. To fix this:

```SQL
wiki=>ALTER TABLE wiki.wiki DROP COLUMN row_id;
ALTER
```

Recreate the `row_id`:

```SQL
wiki=>ALTER TABLE wiki.wiki ADD row_id SERIAL;
ALTER
```

To add this filtering to our ETL job from the previous post:

```python
...
(
    df.filter(df.redirect._title.isNull())
    .filter(~df.title.contains("Wikipedia:"))
    .filter(~df.title.contains("Template:"))
    .filter(~df.title.contains("Draft:"))
    .filter(~df.title.contains("Category:"))
    .filter(~df.title.contains("Portal:"))
    .filter(~df.title.contains("File:"))
    .createOrReplaceTempView("wiki")
)
...
```

***

Great! Now that we have all the components ready, let's put them together.

## Question generator API

Instead of writing a script to generate a new question each time it's run, I decided to build a small API service. This allows it to become a component of a larger system (e.g., a game like MindMaze) while being easily accessible via a web browser. To keep things simple, we'll use `flask`, which supports both API endpoints and HTML templating (for a nicer UI).

Let's go through the web app piece by piece. Aside from the imports, the interesting part is the example question response we'd like our LLM to return. Basic (and insecure!) secrets and connection strings are also initialized here.

```python
from flask import Flask, render_template, request, redirect, url_for
import psycopg
import requests

import json
import random
import time

app = Flask(__name__)
app.config["SECRET_KEY"] = "your secret key"

DB_CONN_STRING = "host=localhost dbname=wiki user=wiki password=wiki"
OLLAMA_HOST = "http://localhost:11434"

EXAMPLE_QUESTION = {
    "question_text": "Question text?",
    "answers": [
        {"selection": "a", "answer_text": "First answer"},
        {"selection": "b", "answer_text": "Second answer"},
        {"selection": "c", "answer_text": "Third answer"},
        {"selection": "d", "answer_text": "Fourth answer"},
    ],
    "correct_answer": "a",
}
```

Previously, we combined the number of articles and fetching a single random article into one SQL query. Here, we've split it into two steps since the number of articles only needs to be fetched once.

```python
app.logger.info("Getting number of articles...")

tic = time.perf_counter()
with psycopg.connect("host=127.0.0.1 dbname=wiki user=wiki password=wiki") as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(title) FROM wiki.wiki;")
        for record in cur:
            app.logger.info("Number of articles: %s", record[0])
            NUMBER_OF_ROWS = record[0]
toc = time.perf_counter()
app.logger.info("Number of articles fetched in %0.4f seconds.", toc - tic)
```

Fetching a random article and its title is straightforward.

```python
def get_random_article():
    row_id = random.randrange(NUMBER_OF_ROWS)

    with psycopg.connect(DB_CONN_STRING) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT title, \"revision_text__VALUE\"\n                FROM wiki.wiki \n                WHERE row_id = {};""".format(row_id)
            )
            for record in cur:
                app.logger.info("Article title and row_id: %s, %s", record[0], row_id)
                return record
```

Generating the question is also straightforward, with the prompt being the most interesting part. Key aspects of the prompt include:

- Limiting question generation **only** to the provided article content
- Responding in JSON format, following the provided example

We've added string cleaning to the prompt to handle cases where the LLM includes additional text, e.g., "Here's the JSON response: ".

```python
def generate_question(article_content):
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
    }

    data = {
        "model": "llama3",
        "prompt": "Please generate a multiple-choice question with four possible answers, based on the following article: {}. Base your question only on the provided article content. Only respond with a JSON response. Base your JSON response on the following example {}".format(
            article_content, EXAMPLE_QUESTION
        ),
        "stream": False,
    }

    response = requests.post(
        OLLAMA_HOST + "/api/generate", headers=headers, data=json.dumps(data)
    )

    app.logger.info("Response: %s", response.json())

    question = (
        "{" + response.json()["response"].split("{", 1)[1].rsplit("}", 1)[0] + "}"
    )

    app.logger.info("Question: %s", question)

    return question
```

Here's the main endpoint where everything comes together. A `GET` request triggers the random article fetch and question generation. The JSON response is extended with the article title and Wikipedia link. A `POST` request checks whether the answer is correct and forwards the answer/correct answer pair to the result page.

```python
@app.route("/question", methods=["GET", "POST"])
def question():
    if request.method == "POST":
        if request.form["answer"] == request.form["correct_answer"]:
            app.logger.info("Correct answer!")
        else:
            app.logger.info("Wrong answer.")
        return redirect(
            url_for(
                "result",
                answer=request.form["answer"],
                correct_answer=request.form["correct_answer"],
            )
        )

    app.logger.info("Getting random article...")

    tic = time.perf_counter()
    random_article = get_random_article()
    toc = time.perf_counter()
    app.logger.info("Article fetched in %0.4f seconds.", toc - tic)

    app.logger.info("Generating question...")
    tic = time.perf_counter()
    question_string = generate_question(article_content=random_article)
    toc = time.perf_counter()
    app.logger.info("Question generated in %0.4f seconds.", toc - tic)

    question_json = json.loads(question_string)
    question_json["article_title"] = random_article[0]
    question_json["article_link"] = "https://en.wikipedia.org/wiki/{}".format(
        random_article[0].replace(" ", "_")
    )
    random.shuffle(question_json["answers"])

    return render_template("question.html", question=question_json)
```

And here's the accompanying template:

```html
<!doctype html>
<html>
<title>Question generator</title>

<body>
  <form method="post">
    <fieldset>
      <legend><a href="{{ question["article_link"] }}" target="blank">{{ question["article_title"] }}</a></legend>

      <div>
        <p>{{ question["question_text"] }}</p>
      </div>

      {% for answer in question["answers"] %}
      <div>
        <input type="radio" id="{{ answer["selection"] }}" name="answer" value="{{ answer["selection"] }}" />
        <label for="{{ answer["selection"] }}">{{ answer["answer_text"] }}</label>
      </div>
      {% endfor %}

      <div>
        <input type="hidden" id="correct_answer" name="correct_answer" value="{{ question["correct_answer"] }}" />
      </div>

      <button>Submit</button>

    </fieldset>
  </form>
  <div class="content">
    {% for message in get_flashed_messages() %}
    <div class="alert">{{ message }}</div>
    {% endfor %}
    {% block content %} {% endblock %}
  </div>
</body>

</html>
```

Finally, here's the result page rendering endpoint and its template.

```python
@app.route("/question/result", methods=["GET"])
def result():
    app.logger.info(
        "Request content on result page: %s, %s",
        request.args["answer"],
        request.args["correct_answer"],
    )
    return render_template(
        "result.html",
        answer=request.args["answer"],
        correct_answer=request.args["correct_answer"],
    )
```

```html
<!doctype html>
<html>
<title>Result page</title>

<body>
  <h1>Result page</h1>
  {% if answer == correct_answer %}
  <p>Congratulations! You got it right!</p>
  {% else %}
  <p>Sorry, you got it wrong!</p>
  {% endif %}
  <form action={{ url_for("question") }}>
    <input type="submit" value="New question!" />
  </form>
  <div class="content">
    {% for message in get_flashed_messages() %}
    <div class="alert">{{ message }}</div>
    {% endfor %}
    {% block content %} {% endblock %}
  </div>
</body>

</html>
```

## Answering questions

Now we come to the fun part! Let's run our `flask` web app and try to answer a couple of questions:

```bash
flask --app=app.py run --debug
```

And we should see a question eventually show up at `http://127.0.0.1/question`:

![question_generator](screenshot.png "Question generator")

The web app is quite rough around the edges, but it allows us to generate and answer questions with ease. There's certainly a lot that can be improved:

- **Question quality**: It quickly becomes apparent that picking a random Wikipedia article doesn't always generate a "good" question, i.e. a question that an average person could know the answer to.
- **Performance**: While the LLM is relatively quick (on my machine it takes ~30 seconds to generate a response), it could be quicker, and the interface could look nicer while the user awaits their question.
- **Question categories**: Similar to the first point, there's no way to control the category of question we'd like (sports, science, etc.) because the random article fetching always uses the whole dataset.

We'll wrap it up for now though, and enjoy some trivia!

Cheers,  
Marko