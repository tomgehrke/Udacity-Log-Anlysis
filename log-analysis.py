#!/usr/bin/env python3
"""Udacity course project.

This Python application will connect to a database that has logged web
requests. It will return pertinent information on article and author
popularity as well as noting which days generated a large number of
errors in response to web requests.
"""

import psycopg2
import psycopg2.extras
import os

# DATABASE-RELATED
DATABASE_NAME = "news"
TOP3ARTICLES_VIEW = "v_top3articles"
TOPAUTHORS_VIEW = "v_topauthors"
TOPERRORDAYS_VIEW = "v_toperrordays"

# REPORTING
TOP3ARTICLES_HEAD = "What are the most popular three articles of all time?"
TOP3ARTICLES_ROW = " * \"{title}\" -- {count} views"
TOPAUTHORS_HEAD = "Who are the most popular authors of all time?"
TOPAUTHORS_ROW = " * {author} -- {count} views"
TOPERRORDAYS_HEAD = "On which days did more than 1% of requests " \
    "lead to errors?"
TOPERRORDAYS_ROW = " * {day:%B %d, %Y} -- {error_rate:.1f}% errors"

# APPLICATION MESSAGES
CHECKING_VIEWS = "Checking for supporting views..."
VIEW_DOES_NOT_EXIST = "- View '{0}' does NOT exist! Creating it..."
VIEW_ISSUE = "There are problems with the supporting views!"
CREATE_VIEW_EXCEPTION = "- Unable to create view {0}!"
RUN_COMPLETE = "[END]"

# SQL
SELECT_VIEW = '''
    SELECT table_name
    FROM information_schema.views
    WHERE table_name='{0}'
    '''
CREATE_TOP3ARTICLES_VIEW = '''
    CREATE VIEW {0} AS (
        SELECT articles.title,
            count(*) AS article_count
        FROM log,
            articles
        WHERE concat('/article/', articles.slug) = log.path
        GROUP BY articles.title
        ORDER BY (count(*)) DESC
        LIMIT 3)
    '''
CREATE_TOPAUTHORS_VIEW = '''
    CREATE VIEW {0} AS (
        SELECT authors.name,
            count(*) AS article_count
        FROM articles,
            authors,
            log
        WHERE articles.author = authors.id
            AND concat('/article/', articles.slug) = log.path
        GROUP BY authors.id
        ORDER BY (count(*)) DESC)
    '''
CREATE_TOPERRORDAYS_VIEW = '''
    CREATE VIEW {0} AS (
        SELECT aq.log_date,
            aq.error_rate
        FROM (
            SELECT fl.log_date,
                fl.log_count,
                el.error_count,
                el.error_count::float8
                    / fl.log_count::float8
                    * 100 AS error_rate
            FROM (
                SELECT date_trunc('day'::text, log."time") AS log_date,
                    count(*) AS log_count
                FROM log
                GROUP BY (date_trunc('day'::text, log."time"))) fl
            JOIN (
                SELECT date_trunc('day'::text, log."time") AS log_date,
                    count(*) AS error_count
                FROM log
                WHERE log.status NOT LIKE '200%'
                GROUP BY (date_trunc('day', log.time))) el
            ON fl.log_date = el.log_date) aq
        WHERE aq.error_rate >= 1
        ORDER BY aq.log_date)
    '''


def views_exist():
    """Check for the existence of supporting views."""
    return (
        check_view(TOP3ARTICLES_VIEW)
        and check_view(TOPAUTHORS_VIEW)
        and check_view(TOPERRORDAYS_VIEW)
    )


def check_view(name):
    """Check for the existence of a view and creates it if necessary.

    If the view exists or was successfully created, the function returns True.
    """
    view_exists = True
    news_connection = psycopg2.connect(database=DATABASE_NAME)
    news_cursor = news_connection.cursor()
    # Looks for the view by querying the database's information_schema
    news_cursor.execute(SELECT_VIEW.format(name))
    news_cursor.fetchall()

    # As long as we got something back, we should be good.
    if news_cursor.rowcount > 0:
        return view_exists
    else:
        # The view does not exist (probably because it is the app's first run).
        print(VIEW_DOES_NOT_EXIST.format(name), end=" ")

        # Let's try to create that view!
        try:
            if name == TOP3ARTICLES_VIEW:
                news_cursor.execute(CREATE_TOP3ARTICLES_VIEW.format(name))
            elif name == TOPAUTHORS_VIEW:
                news_cursor.execute(CREATE_TOPAUTHORS_VIEW.format(name))
            elif name == TOPERRORDAYS_VIEW:
                news_cursor.execute(CREATE_TOPERRORDAYS_VIEW.format(name))

            news_connection.commit()
        except psycopg2.Error as error:
            print(CREATE_VIEW_EXCEPTION.format(name))
            print(error)
            view_exists = False
        finally:
            if view_exists:
                print("SUCCESS!\n")
            news_connection.close()
        return view_exists


def get_top3articles():
    """Function provides list of the three most popular articles."""
    print(TOP3ARTICLES_HEAD + "\n")
    news_connection = psycopg2.connect(database=DATABASE_NAME)
    # Return a dictionary cursor to allow referencing collumns by name
    # instead of by index.
    news_cursor = news_connection.cursor(
        cursor_factory=psycopg2.extras.DictCursor)
    news_cursor.execute("SELECT * FROM {0}".format(TOP3ARTICLES_VIEW))
    records = news_cursor.fetchall()
    for record in records:
        print(TOP3ARTICLES_ROW.format(
            title=record["title"],
            count=record["article_count"])
            )
    print("\n")
    news_connection.close


def get_topauthors():
    """Function provides list of authors sorted by popularity."""
    print(TOPAUTHORS_HEAD + "\n")
    news_connection = psycopg2.connect(database=DATABASE_NAME)
    # Return a dictionary cursor to allow referencing collumns by name
    # instead of by index.
    news_cursor = news_connection.cursor(
        cursor_factory=psycopg2.extras.DictCursor)
    news_cursor.execute("SELECT * FROM {0}".format(TOPAUTHORS_VIEW))
    records = news_cursor.fetchall()
    for record in records:
        print(TOPAUTHORS_ROW.format(
            author=record["name"],
            count=record["article_count"])
            )
    print("\n")
    news_connection.close


def get_toperrordays():
    """Function provides answer to which days were the most error-prone."""
    print(TOPERRORDAYS_HEAD + "\n")
    news_connection = psycopg2.connect(database=DATABASE_NAME)
    # Return a dictionary cursor to allow referencing collumns by name
    # instead of by index.
    news_cursor = news_connection.cursor(
        cursor_factory=psycopg2.extras.DictCursor)
    news_cursor.execute("SELECT * FROM {0}".format(TOPERRORDAYS_VIEW))
    records = news_cursor.fetchall()
    for record in records:
        print(TOPERRORDAYS_ROW.format(
            day=record["log_date"],
            error_rate=record["error_rate"])
            )
    print("\n")
    news_connection.close


os.system('clear')
if views_exist() is not True:
    # The supporting views don't exist so no point in continuing.
    print(VIEW_ISSUE)
else:
    get_top3articles()
    get_topauthors()
    get_toperrordays()
    print(RUN_COMPLETE)
