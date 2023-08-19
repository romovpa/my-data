import email
import email.policy
import email.utils
import re
import time
from email.header import decode_header, make_header
from typing import NamedTuple


def header_to_str(text):
    if text is None:
        return

    header_parts = decode_header(text)
    fixed_header_parts = []
    for content, encoding in header_parts:
        if encoding is not None:
            fixed_content = content.decode(encoding, errors="ignore").encode(encoding)
        else:
            fixed_content = content
        fixed_header_parts.append((fixed_content, encoding))

    return str(make_header(fixed_header_parts))


def _get_first(arr):
    if arr is not None and len(arr) > 0:
        return arr[0]


def _addrs_to_str(addr_pairs):
    return ", ".join(map(str, addr_pairs))


class Address(NamedTuple):
    name: str
    email: str

    def __str__(self):
        if len(self.name.strip()) > 0:
            return f"{self.name} <{self.email}>"
        else:
            return f"{self.email}"

    @property
    def normalized(self):
        """Normalize email address."""
        addr = self.email or ""
        addr = re.sub(r"[^a-zA-Z0-9_.-@]", "", addr)
        try:
            addr_name, domain_part = addr.strip().rsplit("@", 1)
        except ValueError:
            pass
        else:
            addr_name = addr_name.replace(".", "")
            addr_name_parts = addr_name.split("+", 1)
            addr = addr_name_parts[0].lower() + "@" + domain_part.lower()
        return addr

    @staticmethod
    def from_pair(pair):
        name, addr = pair
        return Address(name, addr)


class Message:
    """Email message API for humans.

    Shortly about email headers https://en.wikipedia.org/wiki/Email#Message_header

    Good overview of message body types
    https://stackoverflow.com/questions/17874360/python-how-to-parse-the-body-from-a-raw-email-given-that-raw-email-does-not

    TODO: add short introduction to email and fields to the docstrings.
    """

    def __init__(self, message):
        self.message = email.message_from_bytes(bytes(message), policy=email.policy.default)

    def __getitem__(self, key):
        value = self.message[key]
        return header_to_str(value)

    def __repr__(self):
        summary_lines = [
            f"Message-ID: {self.message_id}",
            f'Date: {self.datetime.strftime("%Y-%m-%d %H:%M:%S") if self.datetime else None}',
            f"From: {self.addr_from}",
            f"To:   {_addrs_to_str(self.addrs_to)}",
        ]
        if self.addrs_cc:
            summary_lines.append(f"Cc:   {_addrs_to_str(self.addrs_cc)}")
        if self.addrs_cc:
            summary_lines.append(f"Bcc:  {_addrs_to_str(self.addrs_bcc)}")
        summary_lines += [
            f"Subject: {self.subject}",
        ]
        return "\n".join(summary_lines)

    def get_addresses(self, key):
        if key not in self.message:
            return []
        values = [header_to_str(value) for value in self.message.get_all(key)]
        return [Address.from_pair(addr_pair) for addr_pair in email.utils.getaddresses(values)]

    @property
    def message_id(self):
        """An automatic-generated field to prevent multiple deliveries and for reference in In-Reply-To: (see below)."""
        return self.message["Message-ID"]

    @property
    def in_reply_to(self):
        """Message-ID of the message this is a reply to. Used to link related messages together.
        This field only applies to reply messages.
        """
        return self.message["In-Reply-To"]

    @property
    def references(self):
        """Message-ID of the message this is a reply to, and the message-id of the message
        the previous reply was a reply to, etc.
        """
        return self.message["References"]

    @property
    def subject(self):
        """A brief summary of the topic of the message.
        Certain abbreviations are commonly used in the subject, including "RE:" and "FW:".
        """
        return header_to_str(self.message["Subject"])

    @property
    def datetime(self):
        """The local time and date the message was written.
        Like the From: field, many email clients fill this in automatically before sending.
        The recipient's client may display the time in the format and time zone local to them.
        """
        if self.message["Date"] is not None:
            return email.utils.parsedate_to_datetime(self.message["Date"])

    @property
    def unixtime(self):
        """Contents of Date header (same as self.datetime) in the form of unixtime"""
        if self.datetime is not None:
            return time.mktime(self.datetime.utctimetuple())

    @property
    def addrs_from(self):
        """The email address, and, optionally, the name of the author(s).
        Some email clients are changeable through account settings.
        """
        return self.get_addresses("From")

    @property
    def addrs_to(self):
        """The email address(es), and optionally name(s) of the message's recipient(s).
        Indicates primary recipients (multiple allowed), for secondary recipients see Cc: and Bcc: below.
        """
        return self.get_addresses("To")

    @property
    def addrs_cc(self):
        """Carbon copy; Many email clients mark email in one's inbox
        differently depending on whether they are in the To: or Cc: list.
        """
        return self.get_addresses("Cc")

    @property
    def addrs_bcc(self):
        """Blind carbon copy; addresses are usually only specified during
        SMTP delivery, and not usually listed in the message header.
        """
        return self.get_addresses("Bcc")

    @property
    def addr_from(self):
        """From address. There could be many From records, to get them all use msg.addrs_from"""
        return _get_first(self.get_addresses("From"))

    @property
    def addr_reply_to(self):
        """Address should be used to reply to the message."""
        return _get_first(self.get_addresses("Reply-To"))

    @property
    def labels(self):
        """Labels assigned to the message/thread

        TODO: Support not only Gmail format
        """
        labels = []
        if "X-Gmail-Labels" in self.message:
            labels.extend(self.message["X-Gmail-Labels"].split(","))
        return labels

    @property
    def thread_id(self):
        """Unique ID of the thread

        TODO: Support not only Gmail format
        """
        return self.message["X-GM-THRID"]

    def get_content(self, preferencelist=("related", "html", "plain")):
        submsg = self.message.get_body(preferencelist)
        if submsg is not None:
            return submsg.get_content()

    @property
    def content_plain(self):
        return self.get_content("plain")

    @property
    def content_html(self):
        return self.get_content("html")

    @property
    def attachments(self):
        """List attached files

        Example:
        >>> attachment = msg.attachments[0]
        >>> attachment.get_content_type()
        'application/pdf'
        >>> attachment.get_filename()
        'Invoice.pdf'
        >>> len(attachment.get_content())  # bytes size
        10000
        >>> with open('Invoice.pdf', 'wb') as f:
        ...     f.write(attachment.get_content())
        """
        return list(self.message.iter_attachments())


def get_email_address(string):
    if not isinstance(string, str):
        return
    email = re.findall(r"<(.+?)>", string)
    if not email:
        email = list(filter(lambda y: "@" in y, string.split()))
    return email[0] if email else None


def get_email_domain(string):
    if not isinstance(string, str):
        return
    email = re.findall(r"<.+@(.+)>", string)
    if not email:
        email = list(filter(lambda y: "@" in y, string.split()))
    return email[0].lower() if email else None
