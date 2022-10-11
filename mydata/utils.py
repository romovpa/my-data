import csv
import re


class CSVWriter:
    def __init__(self, filename):
        self.csvfile = open(filename, 'w')
        self.writer = None

    def writerow(self, row):
        if self.writer is None:
            self.fields = list(row.keys())
            self.writer = csv.DictWriter(self.csvfile, fieldnames=self.fields)
            self.writer.writeheader()
        self.writer.writerow(row)

    def close(self):
        self.csvfile.close()


def get_email_address(string):
    if not isinstance(string, str):
        return
    email = re.findall(r'<(.+?)>', string)
    if not email:
        email = list(filter(lambda y: '@' in y, string.split()))
    return email[0] if email else None


def get_email_domain(string):
    if not isinstance(string, str):
        return
    email = re.findall(r'<.+@(.+)>', string)
    if not email:
        email = list(filter(lambda y: '@' in y, string.split()))
    return email[0].lower() if email else None
