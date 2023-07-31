"""
Parser for Oura ring data.
"""

import json
import pickle
from dateutil.parser import parse as parse_date


def parse_oura_sleep_events(data_file):
    with open(data_file) as fin:
        sleep_data = json.load(fin)

    events = []

    for record in sleep_data['sleep']:
        start_time = parse_date(record['bedtime_start'])
        end_time = parse_date(record['bedtime_end'])

        event = {
            'type': 'bedtime',
            'timestamp': start_time,
            'duration': end_time - start_time,
        }
        events.append(event)

    return events

if __name__ == '__main__':
    events = parse_oura_sleep_events('exports/oura/oura_data_YYYY-MM-DDThh-mm-ss.json')
    with open('cache/oura_sleep_events.pkl', 'wb') as f:
        pickle.dump(events, f)