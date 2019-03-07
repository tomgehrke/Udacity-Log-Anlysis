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
TOPERRORDAYS_ROW = " * {day} -- {error_rate}% errors"

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


def print_results(view_name):
    """Print query results to the terminal console."""
    news_connection = psycopg2.connect(database=DATABASE_NAME)
    # Return a dictionary cursor to allow referencing collumns by name
    # instead of by index.
    news_cursor = news_connection.cursor(
        cursor_factory=psycopg2.extras.DictCursor)
    news_cursor.execute("SELECT * FROM {0}".format(view_name))
    records = news_cursor.fetchall()

    if view_name == TOPERRORDAYS_VIEW:
        print(TOPERRORDAYS_HEAD + "\n")
        for record in records:
            print(TOPERRORDAYS_ROW.format(
                day=record["log_date"],
                error_rate=record["error_rate"])
                )
    elif view_name == TOP3ARTICLES_VIEW:
        print(TOP3ARTICLES_HEAD + "\n")
        for record in records:
            print(TOP3ARTICLES_ROW.format(
                title=record["title"],
                count=record["article_count"])
                )
    elif view_name == TOPAUTHORS_VIEW:
        print(TOPAUTHORS_HEAD + "\n")
        for record in records:
            print(TOPAUTHORS_ROW.format(
                author=record["name"],
                count=record["article_count"])
                )
    print("\n")
    news_connection.close


if __name__ == '__main__':
    os.system('clear')
    if views_exist() is not True:
        # The supporting views don't exist so no point in continuing.
        print(VIEW_ISSUE)
    else:
        print_results(TOP3ARTICLES_VIEW)
        print_results(TOPAUTHORS_VIEW)
        print_results(TOPERRORDAYS_VIEW)
        print(RUN_COMPLETE)
