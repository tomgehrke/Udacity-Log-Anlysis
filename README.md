# Udacity Log Analysis

This Python application will connect to a database that has logged web
requests. It will return pertinent information on article and author
popularity as well as noting which days generated a large number of
errors in response to web requests.

## Requirements

The following requirements must be met:

- The version of Python being used must be *3.0* or greater.
- Connections to the PostgresSQL database will require the *psycopg2* driver be installed.
- The generated output relies on several views that compile information from the database. The program will check for the existence of these views and create them if necessary. If the program is unable to create these views or you wish to do this manually, the view definitions can be found below.

## Supporting Views

Each of the questions that the log analysis program answers is provided by an associated view. As noted previously, the views should be created automatically when the program is first run, however the following will allow you to create them yourself or to see how the numbers are being compiled at the very least.

### View - v_top3articles

This view provides a list of the three (3) most popular articles.

```sql
    CREATE VIEW v_top3articles AS (
        SELECT articles.title,
            count(*) AS article_count
        FROM log,
            articles
        WHERE concat('/article/', articles.slug) = log.path
        GROUP BY articles.title
        ORDER BY (count(*)) DESC
        LIMIT 3)
```

### View - v_topauthors

This view provides article authors listed by the number of views associated with each.

```sql
    CREATE VIEW v_topauthors AS (
        SELECT authors.name,
            count(*) AS article_count
        FROM articles,
            authors,
            log
        WHERE articles.author = authors.id
            AND concat('/article/', articles.slug) = log.path
        GROUP BY authors.id
        ORDER BY (count(*)) DESC)
```

### View - v_toperrordays

This view provides a list of the days where the error rate exceeded 1%.

```sql
    CREATE VIEW {0} AS (
        SELECT to_char(date, 'FMMonth DD, YYYY') as log_date,
                    ROUND(error_percent, 2) as error_rate
        FROM (
            SELECT time::date AS date,
            100 * (COUNT(*) FILTER (WHERE status NOT LIKE '200%') /
                COUNT(*)::numeric) AS error_percent
            FROM log
            GROUP BY time::date
        ) a
        WHERE error_percent > 1)
```

## Usage

If all of the requirements have been met, you can execute the program by running the following from your terminal:

`python3 log-analysis.py`
