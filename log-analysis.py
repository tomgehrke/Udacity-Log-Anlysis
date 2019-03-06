#!/usr/bin/env python3

import psycopg2
import os

DATABASE_NAME = "news"
TOP3ARTICLES_VIEW = "v_top3articles"
TOP3ARTICLES_HEAD = "What are the most popular three articles of all time?"
TOP3ARTICLES_ROW = " * \"{title}\" -- {count} views"
TOPAUTHORS_VIEW = "v_topauthors"
TOPAUTHORS_HEAD = "Who are the most popular authors of all time?"
TOPAUTHORS_ROW = " * {author} -- {count} views"
TOPERRORDAYS_VIEW = "v_toperrordays"
TOPERRORDAYS_HEAD = "On which days did more than 1% of requests " \
    "lead to errors?"
TOPERRORDAYS_ROW = " * {day:%B %d, %Y} -- {error_rate:.1f}% errors"
CHECKING_VIEWS = "Checking for supporting views..."
VIEW_EXISTS = "+ View '{0}' exists!"
VIEW_DOES_NOT_EXIST = "- View '{0}' does NOT exist! Creating it..."
CREATE_VIEW_EXCEPTION = "- Unable to create view {0}!"
SELECT_VIEW = '''
    SELECT table_name
    FROM information_schema.views
    WHERE table_name='{0}'
    '''
CREATETOP3ARTICLES_VIEW = '''
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
CREATETOPAUTHORS_VIEW = '''
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
CREATETOPERRORDAYS_VIEW = '''
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
    return (
        check_view(TOP3ARTICLES_VIEW)
        and check_view(TOPAUTHORS_VIEW)
        and check_view(TOPERRORDAYS_VIEW)
    )


def check_view(name):
    view_exists = True
    news_connection = psycopg2.connect(database=DATABASE_NAME)
    news_cursor = news_connection.cursor()
    news_cursor.execute(SELECT_VIEW.format(name))
    news_cursor.fetchall()

    if news_cursor.rowcount > 0:
        # print(VIEW_EXISTS.format(name))
        return view_exists
    else:
        print(VIEW_DOES_NOT_EXIST.format(name))

        try:
            if name == TOP3ARTICLES_VIEW:
                news_cursor.execute(CREATETOP3ARTICLES_VIEW.format(name))
            elif name == TOPAUTHORS_VIEW:
                news_cursor.execute(CREATETOPAUTHORS_VIEW.format(name))
            elif name == TOPERRORDAYS_VIEW:
                news_cursor.execute(CREATETOPERRORDAYS_VIEW.format(name))

            news_connection.commit()
        except psycopg2.Error:
            print(CREATE_VIEW_EXCEPTION.format(name))
            view_exists = False
        finally:
            print("SUCCESS!\n")
            news_connection.close()

        return view_exists


def get_top3articles():
    print(TOP3ARTICLES_HEAD + "\n")
    news_connection = psycopg2.connect(database=DATABASE_NAME)
    news_cursor = news_connection.cursor()
    news_cursor.execute("SELECT * FROM {0}".format(TOP3ARTICLES_VIEW))
    records = news_cursor.fetchall()
    for record in records:
        print(TOP3ARTICLES_ROW.format(
            title=record[0],
            count=record[1])
            )
    print("\n\n")


def get_topauthors():
    print(TOPAUTHORS_HEAD + "\n")
    news_connection = psycopg2.connect(database=DATABASE_NAME)
    news_cursor = news_connection.cursor()
    news_cursor.execute("SELECT * FROM {0}".format(TOPAUTHORS_VIEW))
    records = news_cursor.fetchall()
    for record in records:
        print(TOPAUTHORS_ROW.format(
            author=record[0],
            count=record[1])
            )
    print("\n\n")


def get_toperrordays():
    print(TOPERRORDAYS_HEAD + "\n")
    news_connection = psycopg2.connect(database=DATABASE_NAME)
    news_cursor = news_connection.cursor()
    news_cursor.execute("SELECT * FROM {0}".format(TOPERRORDAYS_VIEW))
    records = news_cursor.fetchall()
    for record in records:
        print(TOPERRORDAYS_ROW.format(
            day=record[0],
            error_rate=record[1])
            )
    print("\n\n")


os.system('clear')
# print(CHECKING_VIEWS)
if views_exist() is not True:
    print("There are problems with the supporting views!")
else:
    get_top3articles()
    get_topauthors()
    get_toperrordays()
    print("[END]")
