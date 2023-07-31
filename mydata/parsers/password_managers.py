"""
Password managers may help to identify accounts that you have forgotten about.
"""

import sqlite3
import pandas
import tldextract
import shutil
from pathlib import Path
import urllib.parse
from datetime import datetime, timedelta


def parse_chrome_logins(chrome_db_filename):
    con = sqlite3.connect(chrome_db_filename)

    df_chrome_logins = pandas.read_sql(
        """
        SELECT 
            id,
            CASE 
                WHEN date_last_used > 86400000000 THEN 
                    datetime(date_last_used / 1000000 + strftime('%s', '1601-01-01'), 'unixepoch', 'localtime')
                ELSE
                    NULL
            END AS time_last_used,
            CASE 
                WHEN date_password_modified > 86400000000 THEN 
                    datetime(date_password_modified / 1000000 + strftime('%s', '1601-01-01'), 'unixepoch', 'localtime')
                ELSE
                    NULL
            END AS time_last_modified,
            username_value AS username,
            origin_url AS url
        FROM logins
        """,
        con,
    ).set_index('id')

    def url_to_domain(url):
        parts = urllib.parse.urlparse(url)
        if parts.scheme in ('http', 'https'):
            domain = tldextract.extract(url).registered_domain
            if domain:
                return domain

    df_chrome_logins['domain'] = df_chrome_logins['url'].map(url_to_domain)
    return df_chrome_logins


if __name__ == '__main__':
    chrome_db_filename = Path.home() / 'Library/Application Support/Google/Chrome/Default/Login Data'

    # Backup a copy of the database
    # This is also necessary because Chrome will lock the database while it is running
    timestamp_str = datetime.now().strftime('%Y-%m-%dT%H-%M-%S')
    chrome_db_cache_filename = Path('cache') / f'chrome_login_data_{timestamp_str}.sqlite3'
    shutil.copy(chrome_db_filename, chrome_db_cache_filename)

    df_chrome_logins = parse_chrome_logins(chrome_db_cache_filename)
    df_chrome_logins.to_csv('cache/chrome_logins.csv')
