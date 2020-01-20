import json
import logging
import os
import re
from datetime import datetime
from typing import Generator

import praw
import prawcore
import pytz
from google.cloud import error_reporting
from google.cloud import exceptions as google_cloud_exceptions
from google.cloud import firestore, pubsub_v1 as pubsub
from google.cloud import storage
from google.cloud import vision
from google.cloud.firestore_v1 import DocumentReference, CollectionReference
from google.cloud.firestore_v1 import DocumentSnapshot
from google.cloud.logging import Client as LoggingClient
from praw.exceptions import PRAWException

from exceptions import RedditQueryPivotNotFound

_logger = None
_vars_dict = None


def get_logger(log_level: str):
    global _logger
    if _logger is not None:
        return _logger
    client = LoggingClient()
    handler = client.get_default_handler()
    formatter = logging.Formatter('[{levelname:<s}]-[{name:>s}]: {message}', None, '{')
    handler.setFormatter(formatter)
    handler.setLevel(logging._nameToLevel[log_level])
    logger = logging.getLogger('cloudLogger')
    logger.addHandler(handler)
    logger.setLevel(logging._nameToLevel[log_level])
    praw_logger = logging.getLogger('prawcore')
    praw_logger.setLevel(logging._nameToLevel[log_level])
    praw_logger.addHandler(handler)
    _logger = logger
    return _logger


def scan_text(pattern: str, text: str) -> set:
    matches = re.findall(pattern, text, re.MULTILINE | re.UNICODE)
    return set(matches)


def scan_title(pattern: str, title: str) -> bool:
    match = re.search(pattern, title, re.I)
    return match is not None


def scan_image(url: str, raw_pattern: str, vis: vision, logger: logging.Logger) -> set:
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
    :param raw_pattern: Regex pattern to extract from parsed image's text.
    :param vis: google.cloud.vision module.
    :param logger: Logger.
    :return: set with text excerpts obtained from the image.
    """
    logging.debug("Beginning image scan for image with url '{}'.")
    pattern = raw_pattern.rstrip('\b').lstrip('\b')
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

    if len(text_doc_excerpts) > 0:
        logger.info(
            "Number of excerpts in set obtained with the document text detection feature: {}."
                .format(len(text_doc_excerpts))
        )

    if len(text_excerpts) > 0:
        logger.info(
            "Number of excerpts in set obtained with the regular text detection feature: {}."
                .format(len(text_excerpts))
        )

    return len(text_doc_excerpts) >= len(text_excerpts) and text_doc_excerpts or text_excerpts


def get_reddit_query_pivot(
        stream: Generator[DocumentSnapshot, None, None],
        reddit_client: praw.Reddit,
        logger: logging.Logger
) -> dict:
    """
    Find current run's pivot, an existing previous run's most recent submission scanned on a subreddit's new feed.
    """
    for doc in stream:
        doc_dict = doc.to_dict()
        sb = reddit_client.submission(doc_dict['id'])

        # Check if submission is still in feed. Continue to the next if not.
        try:
            logger.debug("Testing if submission {} is still in feed.".format(doc_dict['id']))
            sb_selftext = sb.selftext
            if sb_selftext == '[deleted]' \
                    or sb_selftext == '[removed]' \
                    or sb.author is None \
                    or sb.removed_by_category is not None:
                logger.info("Submission {} has left feed, testing next submission for pivot.".format(doc_dict['id']))
                continue
            return get_submission_record(sb)
        except prawcore.exceptions.NotFound:
            logger.info("Submission {} has left feed, testing next submission for pivot.".format(doc_dict['id']))
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
        u'text': submission.selftext,
        u'html': submission.selftext_html,
        u'is_self': submission.is_self
    }


def get_vars_dict(bucket_name: str, blob_name: str) -> dict:
    """
    :param bucket_name: The vars dict document bucket name.
    :param blob_name: The vars dict document name.
    :return: Variables dictionary with environment parameters.
    """
    global _vars_dict
    if _vars_dict is not None:
        return _vars_dict
    storage_client = storage.Client()
    blob = storage_client.get_bucket(bucket_name).get_blob(blob_name)
    _vars_dict = json.loads(blob.download_as_string())
    return _vars_dict


def get_pst_from_utc(epoch: float) -> datetime:
    """
    Get datetime from epoch.
    :param epoch:
    :return: datetime in PST
    """
    return datetime.fromtimestamp(epoch, tz=pytz.timezone('US/Pacific'))


def notify_sms(
        record: dict,
        to_numbers: [],
        pub_sub_client: pubsub.PublisherClient,
        pub_sub_topic_name: str
):
    """
    Trigger sms notification function with payload.

    >>> import main
    >>> from google.cloud import pubsub_v1 as pubsub
    >>> rcrd = {
    ...     u'id': 'id',
    ...     u'name': 'Test',
    ...     u'fullname': 't3_Test',
    ...     u'title': 'Six booster packs for the new set, got it last night and don’t plan on using the code.',
    ...     u'created_utc': 1579338187,
    ...     u'permalink': 'reddit/r/something',
    ...     u'url': 'http://something.com',
    ...     u'shortlink': 'http://shortlink.com',
    ...     u'text': 'xxx-xxxx-xxx',
    ...     u'excerpts': ['K4Q-HPaz-png'],
    ...     u'is_self': True
    ... }
    >>> ps_client = pubsub.PublisherClient()
    >>> ps_publish_topic = 'projects/tribal-artifact-263821/topics/send_sms_notification'
    >>> main.notify_sms(rcrd, ['+18052845139'], ps_client, ps_publish_topic)
    0

    :param record:
    :param to_numbers:
    :param pub_sub_client:
    :param pub_sub_topic_name:
    :return:
    """
    time = get_pst_from_utc(record['created_utc'])
    msg_text = "Title: {}\nSubmitted: {}\nExcerpts:\n\t{}\nLink: {}\nIs media link: {}".format(
        record['title'], time, "\n\t".join(record['excerpts']), record['shortlink'], not bool(record['is_self']))
    data = json.dumps({
        'sms': {
            'message': msg_text,
            'to_numbers': to_numbers
        }
    })
    pub_sub_client.publish(pub_sub_topic_name, bytes(data, 'utf-8'))
    return 0


def notify_email(
        record: dict,
        to_emails: [],
        pub_sub_client: pubsub.PublisherClient,
        pub_sub_topic_name: str
):
    """
    Trigger email notification function with payload.

    >>> import main
    >>> from google.cloud import pubsub_v1 as pubsub
    >>> rcrd_self = {
    ...     u'id': 'id',
    ...     u'name': 'Test',
    ...     u'fullname': 't3_Test',
    ...     u'title': 'Six booster packs for the new set, got it last night and don’t plan on using the code.',
    ...     u'created_utc': 1579338187,
    ...     u'permalink': 'reddit/r/something',
    ...     u'url': 'http://something.com',
    ...     u'shortlink': 'http://shortlink.com',
    ...     u'text': 'xxx-xxxx-xxx',
    ...     u'excerpts': ['K4Q-HPaz-png'],
    ...     u'html': '<!-- SC_OFF --><div class="md"><p>Is that so much to ask?</p><BR></div><!-- SC_ON -->',
    ...     u'is_self': True
    ... }
    >>> ps_client = pubsub.PublisherClient()
    >>> ps_publish_topic = os.getenv('NOTIFY_PUBSUB_TOPIC')
    >>> main.notify_email(rcrd_self, ['jorgejch@gmail.com'], ps_client, ps_publish_topic)
    0
    >>> rcrd_link = {
    ...     u'id': 'eqtfe4',
    ...     u'name': 't3_eqtfe4',
    ...     u'fullname': 't3_eqtfe4',
    ...     u'title': 'A couple prerelease codes for people who aren’t able to make it to one. USA if that matters',
    ...     u'created_utc': 1579419582,
    ...     u'permalink': '/r/MagicArena/comments/eqtfe4/a_couple_prerelease_codes_for_people_who_arent/',
    ...     u'url': 'https://i.redd.it/91biqetsvob41.jpg',
    ...     u'shortlink': 'http://shortlink.com',
    ...     u'text': '',
    ...     u'excerpts': ['t4M-Xvt6-QCV', 'KD4-Ls8t-XMF'],
    ...     u'html': None,
    ...     u'is_self': False
    ... }
    >>> main.notify_email(rcrd_link, ['jorgejch@gmail.com'], ps_client, ps_publish_topic)
    0

    :param record:
    :param to_emails:
    :param pub_sub_client:
    :param pub_sub_topic_name:
    :return:
    """
    time = get_pst_from_utc(record['created_utc'])
    message_content = \
        """
    <div>
        <h3>{title}</h3>
        <h4>Info</h4>
            <ul>
                <li><p><b>Full name:</b> {fullname}</p></li>
                <li><p><b>Submitted on:</b> {time}</p></li>
            </ul>
        <h4>Links</h4>
            <ul>
                <li><a href={url}>{url_type}</a></li>
                <li><a href={shortlink}>Short link to submission.</a></li>
            </ul>
        <h4>Excerpts</h4>
            <ul>
                {excerpts_list}
            </ul>
        {content_html}
    </div>
    """.format(
            title=record['title'],
            fullname=record['fullname'],
            time=time,
            url=record['url'],
            url_type=record['is_self'] and "URL of submission." or "URL of linked media.",
            shortlink=record['shortlink'],
            excerpts_list="\n".join(list(map(lambda excerpt: '<li>' + excerpt + '</li>\n', record['excerpts']))),
            content_html=record['is_self']
                         and "<h3>Content</h3>\n{}".format(record['html'])
                         or "<h3>Image</h3>\n<img src=\"{}\" style=\"width:992px;\">".format(record['url'])
        )

    data = json.dumps({
        'email': {
            'message': message_content,
            'to_emails': to_emails,
            'subject': "[RedditParser][NEW][MTGA] Match found in submission: '{}'.".format(record['permalink'])
        }
    })
    pub_sub_client.publish(pub_sub_topic_name, bytes(data, 'utf-8'))
    return 0


def scan_subreddits_new(event, context):
    """
    Checks a subreddit for patterns in the title and text of a set of subreddits submission .
    Notifies via SMS if found.

    >>> import mock
    >>> import main
    >>> mock_context = mock.Mock()
    >>> mock_context.event_id = '617187464135194'
    >>> mock_context.timestamp = '2019-07-15T22:09:03.761Z'
    >>> data = {}
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
        subreddits_names = os.getenv('SUBREDDITS').split(',')
        submission_text_regex = os.getenv('SUBMISSION_TEXT_RE')
        to_emails = os.getenv('TO_EMAILS').split(',')
    except Exception as e:
        logger.error("Failed to get environment variable(s) due to: {}.".format(e))
        error_reporting_client.report_exception()
        return 1

    try:
        vars_dict = get_vars_dict(os.getenv('VARS_BUCKET'), os.getenv('VARS_BLOB'))
    except google_cloud_exceptions.NotFound as e:
        logger.error("Failed to obtain vars dictionary due to: {}.".format(e))
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
        logger.error("Failed to obtain Reddit PRAW client due to: {}.".format(e))
        error_reporting_client.report_exception()
        return 1

    try:
        sms_pub_sub_topic_name = vars_dict['SMS_NOTIFY_PUBSUB_TOPIC']
        email_pub_sub_topic_name = vars_dict['EMAIL_NOTIFY_PUBSUB_TOPIC']
    except KeyError as e:
        logging.error('Unable to get notify pub/sub topic, due to: {}.'.format(e))
        error_reporting_client.report_exception()
        return 1

    logger.debug("Testing text and images for pattern {}.".format(submission_text_regex))
    pub_sub_client = pubsub.PublisherClient()
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
                    .limit(3) \
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
            logger.warning(
                'No submission found from previous runs to pivot the reddit query for subreddit {}. Fetching the limit.'
                    .format(sr_name)
            )
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
                        submission_rcrd['excerpts'] = list(excerpts_set)
                        logger.info(
                            'Found match in text of submission with id {} for subreddit {}. Excerpts: {}.'.format(
                                submission_rcrd['id'], sr_name, excerpts_set
                            )
                        )
                        notify_email(submission_rcrd, to_emails, pub_sub_client, email_pub_sub_topic_name)
                        notify_sms(submission_rcrd, to_sms_numbers, pub_sub_client, sms_pub_sub_topic_name)
                        new_feed_text_pattern_matched_col_ref.document(submission_rcrd['id']).set(submission_rcrd)
                        continue

                if re.match(r'^.+\.(jpg|png|jpeg|bmp|tiff)$', submission_rcrd['url']):
                    excerpts_set = scan_image(submission_rcrd['url'], submission_text_regex, vision, logger)
                    if len(excerpts_set) > 0:
                        submission_rcrd['excerpts'] = list(excerpts_set)
                        logger.info(
                            'Found match in image of submission with id {} in subreddit {}. Excerpts: {}.'.format(
                                submission_rcrd['id'], sr_name, excerpts_set
                            )
                        )
                        notify_email(submission_rcrd, to_emails, pub_sub_client, email_pub_sub_topic_name)
                        notify_sms(submission_rcrd, to_sms_numbers, pub_sub_client, sms_pub_sub_topic_name)
                        new_feed_image_pattern_matched_col_ref.document(submission_rcrd['id']).set(submission_rcrd)
        except Exception as e:
            logger.error("Failed to scan subreddit {}'s new feed due to: {}".format(sr_name, e))
            error_reporting_client.report_exception()
            return 1  # if scan failed first scanned submission shouldn't be stored.

        # Save first scanned document (most recent submission) to be used as pivot on next run.
        if first_scanned_rcrd is not None:
            logger.info("Setting submission with title '{}' to collection {} for subreddit {}.".format(
                first_scanned_rcrd['title'], new_feed_first_scanned_col_ref.id, sr_name
            ))
            new_feed_first_scanned_col_ref.document(first_scanned_rcrd['id']).set(first_scanned_rcrd)
        else:
            logger.info('No new submissions found on subreddit\'s {} new feed.'.format(sr_name))
    return 0
