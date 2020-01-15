import json
import os
import re
import logging
from datetime import datetime
from typing import Generator

import praw
import prawcore
import pytz
import google.cloud.logging
from google.cloud import firestore
from google.cloud import storage
from google.cloud.firestore_v1 import DocumentSnapshot
from google.cloud.firestore_v1 import DocumentReference, CollectionReference
from google.cloud import exceptions as google_cloud_exceptions
from google.cloud.logging.handlers import CloudLoggingHandler, setup_logging
from google.cloud import error_reporting
from google.cloud import vision
from praw.exceptions import PRAWException
from twilio.base.exceptions import TwilioException
from twilio.rest import Client as TwilioClient
from exceptions import RedditQueryPivotNotFound


def get_logger(log_level: str):
    client = google.cloud.logging.Client()
    handler = CloudLoggingHandler(client)
    setup_logging(handler, log_level=logging._nameToLevel[log_level])
    logger = logging.getLogger()
    logger.setLevel(logging._nameToLevel[log_level])
    return logger


def get_twilio_client(acct_sid: str, auth_token: str) -> TwilioClient:
    twilio_account_sid = acct_sid
    twilio_auth_token = auth_token
    return TwilioClient(twilio_account_sid, twilio_auth_token)


def notify_sms(
        record: dict,
        to_phone_numbers: list,
        from_phone_number: str,
        client: TwilioClient,
        logger: logging.Logger
):
    for to_num in to_phone_numbers:
        time = datetime.fromtimestamp(record['created_utc'], tz=pytz.timezone('US/Pacific'))
        msg_text = "Title: {}\nCreated: {}\nExcerpts: {}\nLink: {}".format(
            record['title'], time, record['excerpts'], record['shortlink']
        )
        message = client.messages.create(
            from_=from_phone_number,
            to=to_num,
            body=msg_text
        )

        logger.debug(
            "Message sent at {} with notification sid {} and content: {}".format(
                datetime.now(tz=pytz.timezone('US/Pacific')).timestamp(), message.sid, msg_text
            )
        )


def scan_text(pattern: str, text: str) -> set:
    matches = re.findall(pattern, text, re.MULTILINE | re.UNICODE)
    return set(matches)


def scan_title(pattern: str, title: str) -> bool:
    match = re.search(pattern, title, re.I)
    return match is not None


def scan_image(url: str, pattern: str, vis: vision, logger: logging.Logger) -> set:
    """
    >>> import main
    >>> from google.cloud import vision
    >>> import mock
    >>> logger = mock.Mock()
    >>> url = 'https://i.redd.it/775kfxtiyma41.jpg'
    >>> pattern = '\w{3}-\w{4}-\w{3}'
    >>> main.scan_image(url, pattern, vision, logger) ^ {
    ...                                          'XB8-Cytr-YWR',
    ...                                          'YQq-St4b-qzu',
    ...                                          'WOQ-bHUK-Uye',
    ...                                          'QIR-XWc7-Xwu',
    ...                                          '5KO-ASGZ-ypm',
    ...                                          'FuD-Xc7A-v5t',
    ...                                          'PZX-LUty-h4v'
    ...                                          }
    set()

    :param url: Url to the image to be parsed.
    :param pattern: Regex pattern to extract from parsed image's text.
    :param vis: google.cloud.vision module.
    :param logger: Logger.
    :return: set with text excerpts obtained from the image.
    """
    logging.debug("Beginning image scan for image with url '{}'.")
    image = vis.types.Image()
    image.source.image_uri = url
    client = vis.ImageAnnotatorClient()
    response_doc_text = client.document_text_detection(image=image)
    response_text = client.text_detection(image=image)
    text_doc_annotations = response_doc_text.text_annotations
    text_annotations = response_text.text_annotations
    text_doc_list = [text_annotation.description for text_annotation in text_doc_annotations]
    text_list = [text_annotation.description for text_annotation in text_annotations]
    text_doc_excerpts = set(re.findall(r'{}'.format(pattern), ' '.join(text_doc_list), re.MULTILINE | re.UNICODE))
    text_excerpts = set(re.findall(r'{}'.format(pattern), ' '.join(text_list), re.MULTILINE | re.UNICODE))

    logger.debug(
        "Number of excerpts in set obtained with the document text detection feature: {}.".format(text_doc_excerpts)
    )
    logger.debug(
        "Number of excerpts in set obtained with the text detection feature: {}.".format(text_excerpts)
    )

    return len(text_doc_excerpts) >= len(text_excerpts) and text_doc_excerpts or text_excerpts


def get_reddit_query_pivot(
        stream: Generator[DocumentSnapshot, None, None],
        reddit_client: praw.Reddit,
        logger: logging.Logger
) -> dict:
    """
    Find current run pivot, an existing previous run's most recent submission scanned on a subreddit's new feed.
    """
    for doc in stream:
        doc_dict = doc.to_dict()
        sb = reddit_client.submission(doc_dict['id'])

        # Check if submission still exists. Continue to the next if not.
        try:
            logger.debug("Testing if submission {} still exists.".format(doc_dict['id']))
            sb_selftext = sb.selftext
            sb_author = sb.author
            if sb_selftext == '[deleted]' or sb_selftext == '[removed]' or sb_author is None:
                continue
            return get_submission_record(sb)
        except prawcore.exceptions.NotFound:
            continue

    raise RedditQueryPivotNotFound('Reddit submissions query pivot not found.')


def get_reddit_client(client_id: str, client_secret: str, username: str, password: str, user_agent: str) -> praw.Reddit:
    return praw.Reddit(
        client_id=client_id,
        client_secret=client_secret,
        username=username,
        password=password,
        user_agent=user_agent
    )


def get_submission_record(submission: praw.reddit.models.Submission) -> dict:
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


def get_vars_dict(bucket_name: str, blob_name: str) -> dict:
    storage_client = storage.Client()
    blob = storage_client.get_bucket(bucket_name).get_blob(blob_name)

    return json.loads(blob.download_as_string())


def scan_subreddits_new(event, context):
    """
    Checks a subreddit for patterns in the title and text of a set of subreddits submission .
    Notifies via SMS if found.

    >>> import mock
    >>> import main
    >>> import os
    >>> mock_context = mock.Mock()
    >>> mock_context.event_id = '617187464135194'
    >>> mock_context.timestamp = '2019-07-15T22:09:03.761Z'
    >>> data = {}
    >>> def get_vars_dict(bucket_name, blob_name):
    ...     return {
    ...                 "REDDIT_CLIENT_ID": os.getenv('REDDIT_CLIENT_ID'),
    ...                 "REDDIT_CLIENT_SECRET": os.getenv('REDDIT_CLIENT_SECRET'),
    ...                 "REDDIT_PASSWORD": os.getenv('REDDIT_PASSWORD'),
    ...                 "REDDIT_USERAGENT": os.getenv('REDDIT_USERAGENT'),
    ...                 "REDDIT_USERNAME": os.getenv('REDDIT_USERNAME'),
    ...                 "TWILIO_ACCOUNT_SID": os.getenv('TWILIO_ACCOUNT_SID'),
    ...                 "TWILIO_AUTH_TOKEN": os.getenv('TWILIO_AUTH_TOKEN'),
    ...                 "TWILIO_NOTIFY_SID": os.getenv('TWILIO_NOTIFY_SID'),
    ...            }
    >>> main.get_vars_dict = get_vars_dict
    >>> main.scan_subreddits_new(data, mock_context)
    0

    :param event: dict The `data` field contains the PubsubMessage message. The `attributes` field will contain custom
     attributes if there are any.
    :param context: google.cloud.functions.Context The `event_id` field contains the Pub/Sub message ID. The `timestamp`
     field contains the publish time. The `event_type` field is the type of the event,
     ex: "google.pubsub.topic.publish". The `resource` field is the resource that emitted the event.
    """
    error_reporting_client = error_reporting.Client()
    logger = get_logger(os.getenv('LOG_LEVEL'))

    try:
        to_sms_numbers = os.getenv('TO_SMS_NUMBERS').split(',')
        from_sms_number = os.getenv('FROM_SMS_NUMBER')
        subreddits_names = os.getenv('SUBREDDITS').split(',')
        submission_text_regex = os.getenv('SUBMISSION_TEXT_RE')
        logger.debug("Testing text and images for pattern {}.".format(submission_text_regex))
    except Exception as e:
        logger.error("Failed to get environment variable(s) due to: {}".format(e))
        error_reporting_client.report_exception()
        return 1

    try:
        vars_dict = get_vars_dict(os.getenv('VARS_BUCKET'), os.getenv('VARS_BLOB'))
    except google_cloud_exceptions.NotFound as e:
        logger.error("Failed to obtain vars dictionary due to: {}".format(e))
        error_reporting_client.report_exception()
        return 1

    try:
        twilio_client = get_twilio_client(vars_dict['TWILIO_ACCOUNT_SID'], vars_dict['TWILIO_AUTH_TOKEN'])
    except TwilioException as e:
        logger.error("Failed to obtain Twilio client due to: {}".format(e))
        error_reporting_client.report_exception()
        return 1

    try:
        reddit = get_reddit_client(
            vars_dict['REDDIT_CLIENT_ID'],
            vars_dict['REDDIT_CLIENT_SECRET'],
            vars_dict['REDDIT_USERNAME'],
            vars_dict['REDDIT_PASSWORD'],
            vars_dict['REDDIT_USERAGENT']
        )
        subreddits = [reddit.subreddit(sr_name) for sr_name in subreddits_names]
    except PRAWException as e:
        logger.error("Failed to obtain Reddit PRAW client due to: {}".format(e))
        error_reporting_client.report_exception()
        return 1

    db = firestore.Client()

    # Scan stream 'new' of configured subreddits.
    for subreddit in subreddits:
        sr_name = subreddit.display_name
        logger.debug("Scanning subreddit\'s {} new feed.".format(sr_name))
        # Setup Firestore collections, documents references and streams.
        try:
            subreddit_col_ref: CollectionReference = db.collection(u'reddit.{}'.format(sr_name))
            subreddit_new_feed_doc_ref: DocumentReference = subreddit_col_ref.document(u'new')
            new_feed_first_scanned_col_ref: CollectionReference = subreddit_new_feed_doc_ref.collection(
                u'first_scanned'
            )
            new_feed_first_scanned_doc_stream: Generator[DocumentSnapshot, None, None] = \
                new_feed_first_scanned_col_ref \
                    .order_by(u'id', direction=firestore.Query.DESCENDING) \
                    .stream()
            new_feed_text_pattern_matched_col_ref: CollectionReference = subreddit_new_feed_doc_ref.collection(
                u'text_pattern_matched'
            )
            new_feed_image_pattern_matched_col_ref: CollectionReference = subreddit_new_feed_doc_ref.collection(
                u'image_pattern_matched'
            )
        except Exception as e:
            logger.error("Failed to setup Firestore collections, documents and stream due to: {}".format(e))
            error_reporting_client.report_exception()
            return 1

        try:
            # Most recent submission on subreddit's new feed on last run.
            prev_run_first_scanned_rcrd = get_reddit_query_pivot(new_feed_first_scanned_doc_stream, reddit, logger)
            logger.info(
                "Title of last run's most recent submission in subreddit {} is '{}'".format(
                    sr_name, prev_run_first_scanned_rcrd['title']
                )
            )
        except RedditQueryPivotNotFound:
            logger.warning('No submission found from previous runs to pivot the reddit query. Fetching the limit.')
            prev_run_first_scanned_rcrd = None

        prev_run_first_scanned_rcrd_fullname = prev_run_first_scanned_rcrd \
                                               and prev_run_first_scanned_rcrd['fullname'] \
                                               or None
        first_scanned_rcrd = None  # most recent submission on subreddit's new feed on current run

        # Scan new submissions in the subreddit before the most recent subreddit submission on the last run.
        try:
            for submission in subreddit.new(limit=100, params={'before': prev_run_first_scanned_rcrd_fullname}):
                submission_rcrd = get_submission_record(submission)
                logger.info("Scanning subreddit's {} submission with title: {}.".format(
                    sr_name, submission_rcrd['title']
                ))

                if first_scanned_rcrd is None:
                    first_scanned_rcrd = submission_rcrd

                if len(submission_rcrd['text']) > 0:
                    excerpts_set = scan_text(submission_text_regex, submission_rcrd['text'])
                    if len(excerpts_set) > 0:
                        submission_rcrd['excerpts'] = '\n'.join(excerpts_set)
                        logger.info(
                            'Found match in text of submission with id {}. Excerpts: {}.'.format(
                                submission_rcrd['id'], excerpts_set
                            )
                        )
                        new_feed_text_pattern_matched_col_ref.document(submission_rcrd['id']).set(submission_rcrd)
                        notify_sms(submission_rcrd, to_sms_numbers, from_sms_number, twilio_client, logger)
                        continue

                if re.match(r'^.+\.(jpg|png|jpeg|bmp|tiff)$', submission_rcrd['url']):
                    excerpts_set = scan_image(submission_rcrd['url'], submission_text_regex, vision, logger)
                    if len(excerpts_set) > 0:
                        submission_rcrd['excerpts'] = '\n'.join(excerpts_set)
                        logger.info(
                            'Found match in image of submission with id {}. Excerpts: {}.'.format(
                                submission_rcrd['id'], excerpts_set
                            )
                        )
                        new_feed_image_pattern_matched_col_ref.document(submission_rcrd['id']).set(submission_rcrd)
                        notify_sms(submission_rcrd, to_sms_numbers, from_sms_number, twilio_client, logger)
        except Exception as e:
            logger.warning("Failed to scan subreddit {}'s new feed due to: {}".format(sr_name, e))
            error_reporting_client.report_exception()
            continue  # if scan failed first scanned submission shouldn't be stored.

        # Save first scanned document (most recent submission) to be used as pivot on next run.
        if first_scanned_rcrd is not None:
            logger.info("Setting submission with title '{}' to collection {}.".format(
                first_scanned_rcrd['title'], new_feed_first_scanned_col_ref.id
            ))
            new_feed_first_scanned_col_ref.document(first_scanned_rcrd['id']).set(first_scanned_rcrd)
        else:
            logger.info('No new submissions found on subreddit\'s {} new feed.'.format(sr_name))

    return 0
