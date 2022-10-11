import streamlit as st

st.set_page_config(
    page_title='My Data',
    page_icon='ℹ️',
)

st.markdown("""
# My Data Explorer

This is a set of tools to explore your digital footprint and claim your data from web services.

Your exported data should be present in `exports`.

Use tabs to explore your data. The first run may take a while before the intermediate representation is prepared (stored in `cache`).   
""")