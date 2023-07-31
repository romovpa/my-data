"""
Parse events from data exports and local files.
"""
import pickle
from typing import Optional
from dataclasses import dataclass
from datetime import datetime, timedelta
from collections import namedtuple
import uuid

import pandas
import sqlite3


@dataclass
class Event:
    id: str
    time: datetime
    duration: Optional[timedelta]


Email = namedtuple('Email', ['address'])
Phone = namedtuple('Phone', ['phone'])


@dataclass
class WebVisit(Event):
    url: str
    title: str
    browser: str

    # TODO: add links between visits
    # TODO: add device information


def parse_chrome_history(db_file):
    conn = sqlite3.connect(db_file)

    df_chrome_visits = pandas.read_sql('''
        SELECT 
            DATETIME((visit_time/1000000)-11644473600, 'unixepoch', 'localtime') AS time,
            CAST(visit_duration AS FLOAT)/1000000 AS duration,
            urls.url AS url,
            urls.title AS title
        FROM visits
        LEFT JOIN urls ON visits.url = urls.id
    ''', conn)

    for _, row in df_chrome_visits.iterrows():
        yield WebVisit(
            id=str(uuid.uuid4()),
            time=row['time'],
            duration=None,
            url=row['url'],
            title=row['title'],
            browser='chrome',
        )


def parse_safari_history(db_file):
    conn = sqlite3.connect(db_file)

    df_safari_visits = pandas.read_sql('''
        SELECT 
            DATETIME(visit_time + 978307200, 'unixepoch', 'localtime') AS time, 
            url AS url,
            title AS title
        FROM history_items 
        LEFT JOIN history_visits ON history_items.id = history_visits.history_item
    ''', conn)

    for _, row in df_safari_visits.iterrows():
        yield WebVisit(
            id=str(uuid.uuid4()),
            time=row['time'],
            duration=None,
            url=row['url'],
            title=row['title'],
            browser='safari',
        )


def prepare_web_events():
    events = []

    events += list(parse_chrome_history('exports/Chrome_History.db'))
    events += list(parse_safari_history('exports/Safari_History.db'))

    events.sort(key=lambda event: event.time)

    df_events = pandas.DataFrame.from_records([event.__dict__ for event in events]).set_index('id')

    print('Total events:', len(events))

    with open('cache/events.pkl', 'wb') as f:
        pickle.dump(events, f)
    df_events.to_csv('cache/events.csv')


if __name__ == '__main__':
    prepare_web_events()