import json
import textwrap

import praw
import os
import re
import logging
import doctest

import pytz
from google.cloud import firestore
from twilio.rest import Client
from datetime import datetime


def scan_text(text: str):
    match = re.search(r'\b\w{3}-\w{4}-\w{3}\b', text, re.MULTILINE)
    return match and match.group() or False


def scan_title(title: str):
    return re.search(r'\bcodes?\b', title, re.I)


def get_record(submission: praw.reddit.models.Submission):
    return {
        u'id': submission.id,
        u'name': submission.name,
        u'fullname': submission.fullname,
        u'title': submission.title,
        u'created_utc': submission.created_utc,
        u'permalink': submission.permalink,
        u'url': submission.url,
        u'shortlink': submission.shortlink,
        u'text': submission.selftext
    }


def notify(record):
    twilio_account_sid = os.getenv('TWILIO_ACCOUNT_SID')
    twilio_auth_token = os.getenv('TWILIO_AUTH_TOKEN')
    twilio_notify_sid = os.getenv('TWILIO_NOTIFY_SID')
    client = Client(twilio_account_sid, twilio_auth_token)
    binding = client.notify.services(twilio_notify_sid).bindings.create(
        identity='00000001',
        binding_type='sms',
        address='+18052845139'
    )
    notification = client.notify.services(twilio_notify_sid).notifications.create(
        identity='00000001',
        body=textwrap.dedent("""Title: {}
             Created: {}
             Link: {}
             Text: {}""")
            .format(
            record['title'],
            datetime.fromtimestamp(record['created_utc'], tz=pytz.timezone('US/Pacific')),
            record['shortlink'],
            record['text']
        )
    )

    print("Message sent at {} with notification sid {} and content: {}".format(
        datetime.now(tz=datetime.astimezone(pytz.timezone('US/Pacific'))), notification.sid, json.dumps(record)
    ))


def check_subreddit(event, context):
    """
    Checks a subreddit for the 'code' keyword in the title and for codes in the body.
    Notifies via SMS if found.

    :param event:
    :param context:
    :return:
    """
    log_level = logging._nameToLevel[os.getenv('LOG_LEVEL')] or logging.DEBUG
    handler = logging.StreamHandler()
    handler.setLevel(log_level)
    logger = logging.getLogger('prawcore')
    logger.setLevel(log_level)
    logger.addHandler(handler)

    db = firestore.Client()
    reddit = praw.Reddit(
        client_id=os.getenv('CLIENT_ID'),
        client_secret=os.getenv('CLIENT_SECRET'),
        password=os.getenv('PASSWORD'),
        username=os.getenv('USERNAME'),
        user_agent=os.getenv('USERAGENT')
    )
    sr = reddit.subreddit(os.getenv('SUBREDDIT'))
    col_ref = db.collection(u'reddit.{}.new'.format(sr.display_name))
    doc_ref = col_ref.document('last_scanned')
    prev_run_recent_sub_rcd = doc_ref.get().exists and doc_ref.get().to_dict() or None

    if prev_run_recent_sub_rcd:
        logger.debug("Last run's last submission title: {}".format(prev_run_recent_sub_rcd['title']))

    crnt_run_recent_sub_rcd = None
    crnt_run_older_sub_rcd = None

    for submission in sr.new(limit=100, params={'before': prev_run_recent_sub_rcd['fullname']}):
        submission_rcd = get_record(submission)
        crnt_run_older_sub_rcd = submission_rcd

        if crnt_run_recent_sub_rcd is None:
            crnt_run_recent_sub_rcd = submission_rcd

        if scan_title(submission_rcd['title']):
            notify(submission_rcd)
            continue

        if len(submission_rcd['text']) > 0:
            if scan_text(submission_rcd['text']):
                notify(submission_rcd)

    if crnt_run_recent_sub_rcd is not None:
        doc_ref.set(crnt_run_recent_sub_rcd)
        logging.info("Updated most recent subreddit submission scanned. {}".format(json.dumps(crnt_run_recent_sub_rcd)))
        logger.debug("Older submission scanned in this run: {}".format(json.dumps(crnt_run_older_sub_rcd)))
    else:
        logging.debug('No submissions returned.')
