import collections

import pandas
import streamlit as st

from mydata import mailbox_analyzer


@st.experimental_memo
def load_accounts():
    accounts = mailbox_analyzer.discover_and_parse_mbox('exports', 'cache')
    return accounts


accounts = load_accounts()
my_addrs = collections.Counter([acc['my_addr'] for acc in accounts])

st.title('Your Accounts')

sel_addr, _ = st.selectbox('Email Address', [(None, None)] + my_addrs.most_common(), format_func=lambda acc: f'{acc[0]}')

df_accounts = pandas.DataFrame([
    acc
    for acc in accounts
    if sel_addr is None or acc['my_addr'] == sel_addr
])
df_accounts['domain_rank'] = df_accounts['domain_rank'].fillna(1000000).astype(int)
df_accounts = df_accounts.sort_values('domain_rank').reset_index(drop=True)

st.markdown(f'''
- Possible accounts: {df_accounts.shape[0]}
- Possible accounts with at least 1 generated message (detected): {df_accounts.query("generated > 0").shape[0]}
''')

st.dataframe(df_accounts, height=500)

