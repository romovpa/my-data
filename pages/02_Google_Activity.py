import datetime
import os
import pickle

import pandas
import streamlit as st

from mydata.google_events import read_entries

st.set_page_config(
    page_title='Google Activity',
    page_icon='📊',
)


## Loading the data

@st.experimental_memo
def prepare_entries(takeout_dir):
    cache_file = 'cache/google_entries.pkl'
    records = None
    if os.path.exists(cache_file):
        with open(cache_file, 'rb') as fin:
            records = pickle.load(fin)
    else:
        records = read_entries(takeout_dir)
        with open(cache_file, 'wb') as fout:
            pickle.dump(records, fout)

    df = pandas.DataFrame([
        {
            'timestamp': record.timestamp.replace(tzinfo=None),
            'product': record.product,
            'title': record.title,
            'action': record.action,
            'params': record.params,
            'urls': record.urls,
        }
        for record in records
    ])
    return df


df_records = prepare_entries('exports')

## Sidebar & Filters

st.sidebar.title('Google Activity')

total_records = df_records.shape[0]
st.sidebar.metric('Total Records', total_records)


def process_activity(df_records):
    product, _ = st.sidebar.selectbox(
        'Product',
        [(None, df_records.shape[0])] + [(product, cnt) for product, cnt in
                                         df_records['product'].value_counts().items()],
        format_func=lambda x: f'{x[0]} ({x[1]}, {x[1] / df_records.shape[0] * 100:.0f}%)',
    )

    if product is not None:
        df_records = df_records.query('product == @product')

    start_date = st.sidebar.date_input('Start date', datetime.datetime(1990, 1, 1))
    end_date = st.sidebar.date_input('End date', datetime.datetime.now())
    df_records = df_records.query('timestamp > @start_date & timestamp < @end_date')

    action_keyword = st.sidebar.text_input('Action')
    if action_keyword:
        df_records = df_records.loc[df_records.action.str.contains(action_keyword, case=False), :]

    st.sidebar.metric('Selected', df_records.shape[0])

    hour_stats = df_records.groupby(df_records.timestamp.dt.to_period('D'))['title'].count()
    st.sidebar.line_chart(
        pandas.Series(
            hour_stats.values,
            index=hour_stats.index.to_timestamp(),
            name='records per day',
        ),
    )

    ## Tables

    st.subheader('Actions')

    g = df_records.groupby('action')

    df_top_actions = pandas.DataFrame({
        'count': g['timestamp'].count(),
        'last': g['timestamp'].max(),
        'first': g['timestamp'].min(),
    }).sort_values('count', ascending=False)
    df_top_actions.index.name = 'action'
    st.dataframe(df_top_actions)

    st.subheader('Sample')

    pick_a_day = st.checkbox('Pick a day')
    if pick_a_day:
        st.text('The full history for that day')
        sel_date = st.date_input('Date')
        df_sample = df_records.loc[df_records.timestamp.dt.date == sel_date, :]

    else:
        max_sample_size = 1000
        st.text(f'A random subsample of {max_sample_size} (out of {df_records.shape[0]})')

        sample_size = min(max_sample_size, df_records.shape[0])
        df_sample = df_records.sample(sample_size)

    st.dataframe(
        df_sample.sort_values('timestamp').reset_index(drop=True),
        height=800,
    )

    hour_stats = df_sample.groupby(df_sample.timestamp.dt.to_period('H'))['title'].count()
    st.bar_chart(pandas.Series(hour_stats.values, name='activity', index=hour_stats.index.to_timestamp()))


if not df_records.empty:
    process_activity(df_records)