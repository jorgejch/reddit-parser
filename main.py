import json
import logging
import os
import re
from datetime import datetime
from io import StringIO
from typing import Generator

import mock
import praw
import prawcore
import pytz
from google.cloud import error_reporting
from google.cloud import firestore, pubsub_v1 as pubsub
from google.cloud import storage
from google.cloud import vision
from google.cloud.firestore_v1 import DocumentReference, CollectionReference
from google.cloud.firestore_v1 import DocumentSnapshot
from praw.exceptions import PRAWException

from reddit_parser_exceptions import RedditQueryPivotNotFound

_LOGGER = None
_ERROR_REPORTING_CLIENT = None
_REDDIT_CLIENT = None
_IMAGE_ANNOTATOR_CLIENT = None
_PUB_SUB_CLIENT = None
_DB_CLIENT = None


def _get_logger() -> logging.Logger:
    global _LOGGER
    if _LOGGER:
        return _LOGGER
    log_level: str = os.getenv("LOG_LEVEL") or "INFO"
    logging.basicConfig(level=logging._nameToLevel[log_level])
    _LOGGER = logging.getLogger()
    return _LOGGER


_get_logger().info('Loading module.')


def _get_error_reporting_client() -> error_reporting.Client:
    global _ERROR_REPORTING_CLIENT
    if _ERROR_REPORTING_CLIENT:
        return _ERROR_REPORTING_CLIENT
    _ERROR_REPORTING_CLIENT = error_reporting.Client()
    return _ERROR_REPORTING_CLIENT


def _get_reddit_client() -> praw.Reddit:
    global _REDDIT_CLIENT
    if _REDDIT_CLIENT:
        return _REDDIT_CLIENT
    vars_dict = _get_vars_dict()
    _REDDIT_CLIENT = praw.Reddit(
        client_id=vars_dict['REDDIT_CLIENT_ID'],
        client_secret=vars_dict['REDDIT_CLIENT_SECRET'],
        username=vars_dict['REDDIT_USERNAME'],
        password=vars_dict['REDDIT_PASSWORD'],
        user_agent=vars_dict['REDDIT_USERAGENT']
    )
    return _REDDIT_CLIENT


def _get_db_client() -> firestore.Client:
    global _DB_CLIENT
    if _DB_CLIENT:
        return _DB_CLIENT
    _DB_CLIENT = firestore.Client()
    return _DB_CLIENT


def _get_pub_sub_client() -> pubsub.PublisherClient:
    global _PUB_SUB_CLIENT
    if _PUB_SUB_CLIENT:
        return _PUB_SUB_CLIENT
    _PUB_SUB_CLIENT = pubsub.PublisherClient()
    return _PUB_SUB_CLIENT


def _get_image_annotator_client() -> vision.ImageAnnotatorClient:
    global _IMAGE_ANNOTATOR_CLIENT
    _get_logger().info(f'_IMAGE_ANNOTATOR_CLIENT value: {_IMAGE_ANNOTATOR_CLIENT}')
    if _IMAGE_ANNOTATOR_CLIENT:
        return _IMAGE_ANNOTATOR_CLIENT
    _get_logger().info("Instantiated ImageAnnotatorClient.")
    _IMAGE_ANNOTATOR_CLIENT = vision.ImageAnnotatorClient()
    return _IMAGE_ANNOTATOR_CLIENT


def _get_vars_dict() -> dict:
    """
    Get sensitive data dict.
    """
    storage_client = storage.Client()
    blob = storage_client.get_bucket(os.getenv('VARS_BUCKET')).get_blob(os.getenv('VARS_BLOB'))
    return json.loads(blob.download_as_string())


def _get_submission_record(submission: praw.reddit.models.Submission) -> dict:
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


def _get_pst_from_utc(epoch: float) -> datetime:
    """
    Get datetime from epoch.
    :param epoch:
    :return: datetime in PST
    """
    return datetime.fromtimestamp(epoch, tz=pytz.timezone('US/Pacific'))


def _get_reddit_query_pivot(
        stream: Generator[DocumentSnapshot, None, None],
        first_scanned_col_ref: CollectionReference
) -> dict:
    """
    Find current run's pivot, an existing previous run's most recent submission scanned on a subreddit's new feed.
    """
    for doc in stream:
        doc_dict = doc.to_dict()
        sb = _get_reddit_client().submission(doc_dict['id'])

        # Check if submission is still in feed. Continue to the next if not.
        try:
            _get_logger().debug(f"Testing if submission {doc_dict['id']} is still in feed.")
            sb_selftext = sb.selftext
            if sb_selftext == '[deleted]' \
                    or sb_selftext == '[removed]' \
                    or sb.author is None \
                    or sb.removed_by_category is not None:
                _get_logger().info(
                    f"Submission {doc_dict['id']} has left feed, testing next submission for pivot."
                )
                first_scanned_col_ref.document(doc.id).delete()  # delete from database so it's not tested again.
                continue
            return _get_submission_record(sb)
        except prawcore.exceptions.NotFound:
            _get_logger().info(f"Submission {doc_dict['id']} has left feed, testing next submission for pivot.")
            first_scanned_col_ref.document(doc.id).delete()
            continue
    raise RedditQueryPivotNotFound('Reddit submissions query pivot not found.')


def _scan_text(pattern: str, text: str) -> set:
    matches = re.findall(pattern, text, re.MULTILINE | re.UNICODE)
    return set(matches)


def _scan_image(url: str, raw_pattern: str) -> set:
    """
    >>> import main
    >>> import mock
    >>> url = 'https://i.redd.it/akkzmel1xpf41.jpg'
    >>> pattern = '\b\w{3}-\w{4}-\w{3}\b'
    >>> main._scan_image(url, pattern) ^ {
    ...                                          'FZY-5YPY-W1s',
    ...                                          'XUJ-nrfd-rbX',
    ...                                          'Zoq-nDsL-B38',
    ...                                          'p8q-bwDD-2gY',
    ...                                          'cUy-4nFi-9bD',
    ...                                          'rmZ-dZsJ-PzH',
    ...                                          }
    set()

    :param url: Url to the image to be parsed.
    :param raw_pattern: Regex pattern to extract from parsed image's text.
    :return: set with text excerpts obtained from the image.
    """
    _get_logger().info("Beginning image scan for image with url '{}'.".format(url))
    pattern = raw_pattern.rstrip('\b').lstrip('\b')
    image = vision.types.Image()
    image.source.image_uri = url
    response_text = _get_image_annotator_client().text_detection(image=image)
    text_annotations = response_text.text_annotations
    text_list = [text_annotation.description for text_annotation in text_annotations]
    text_excerpts = set(re.findall(r'{}'.format(pattern), ' '.join(text_list), re.MULTILINE | re.UNICODE))
    if len(text_excerpts) > 0:
        reg_log_strio = StringIO()
        reg_log_strio.write(
            "[OCR-REG] Number of excerpts in set with the regular text detection: {}.".format(len(text_excerpts))
        )
        reg_log_strio.write('\n\tExcerpts:\n\t\t{}'.format("\n\t\t".join(text_excerpts)))
        _get_logger().info(reg_log_strio.getvalue())
        reg_log_strio.close()
    return text_excerpts


def _save_image_to_mega(url):
    """
    Sends a file url to be saved on the Mega.nz service.

    >>> from main import _save_image_to_mega
    >>> url = 'https://i.redd.it/akkzmel1xpf41.jpg'
    >>> _save_image_to_mega(url)
    0
    """
    pub_sub_topic_name = os.getenv('MEGA_STORAGE_PUBSUB_TOPIC')
    data = bytes(json.dumps({
        "url": url,
        "folder": os.getenv('MEGA_IMAGE_FOLDER')
    }), 'utf-8')
    _get_logger().info(f"Data string to send: {data}")
    _get_pub_sub_client().publish(pub_sub_topic_name, data)

    return 0


def _notify_sms(
        record: dict,
        to_numbers: []
) -> int:
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
    >>> main._notify_sms(rcrd, ['+18052845139'])
    0

    :param record:
    :param to_numbers:
    :return:
    """
    pub_sub_topic_name = os.getenv('SMS_NOTIFY_PUBSUB_TOPIC')
    time = _get_pst_from_utc(record['created_utc'])
    msg_text = "Title: {}\nSubmitted: {}\nExcerpts:\n\t{}\nLink: {}\nIs media link: {}".format(
        record['title'], time, "\n\t".join(record['excerpts']), record['shortlink'], not bool(record['is_self']))
    data = json.dumps({
        'sms': {
            'message': msg_text,
            'to_numbers': to_numbers
        }
    })
    _get_pub_sub_client().publish(pub_sub_topic_name, bytes(data, 'utf-8'))
    return 0


def _notify_email(
        record: dict,
        to_emails: []
) -> int:
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
    >>> main._notify_email(rcrd_self, ['jorgejch@gmail.com'])
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
    >>> main._notify_email(rcrd_link, ['jorgejch@gmail.com'])
    0

    :param record:
    :param to_emails:
    :return:
    """
    pub_sub_topic_name = os.getenv('EMAIL_NOTIFY_PUBSUB_TOPIC')
    time = _get_pst_from_utc(record['created_utc'])
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
    _get_pub_sub_client().publish(pub_sub_topic_name, bytes(data, 'utf-8'))
    return 0


def scan_subreddits_new(event, context) -> int:
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
    _get_logger().info("Starting to scan subreddits.")

    try:
        to_sms_numbers = os.getenv('TO_SMS_NUMBERS').split(',')
        subreddits_names = os.getenv('SUBREDDITS').split(',')
        submission_text_regex = os.getenv('SUBMISSION_TEXT_RE')
        to_emails = os.getenv('TO_EMAILS').split(',')
    except Exception as e:
        _get_logger().error("Failed to get environment variable(s) due to: {}.".format(e), exc_info=True)
        _get_error_reporting_client().report_exception()
        return 1

    try:
        subreddits = [_get_reddit_client().subreddit(sr_name) for sr_name in subreddits_names]
    except PRAWException as e:
        _get_logger().error("Failed to obtain Reddit PRAW client due to: {}.".format(e), exc_info=True)
        _get_error_reporting_client().report_exception()
        return 1

    _get_logger().debug("Pattern in use to test images and text: {}".format(submission_text_regex))

    # Scan stream 'new' of configured subreddits.
    for subreddit in subreddits:
        sr_name = subreddit.display_name
        _get_logger().info("Scanning subreddit\'s {} new feed.".format(sr_name))
        # Setup Firestore collections, documents references and streams.
        try:
            subreddit_col_ref: CollectionReference = _get_db_client().collection(u'reddit.{}'.format(sr_name))
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
            _get_logger().error(
                "Failed to setup Firestore collections, documents and stream due to: {}".format(e), exc_info=True
            )
            _get_error_reporting_client().report_exception()
            return 1

        try:
            # Most recent submission on subreddit's new feed on last run.
            prev_run_first_scanned_rcrd = _get_reddit_query_pivot(
                new_feed_first_scanned_doc_stream,
                new_feed_first_scanned_col_ref
            )
            new_feed_first_scanned_doc_stream.close()

            _get_logger().info(
                "Title of last run's most recent submission in subreddit {} is '{}'".format(
                    sr_name, prev_run_first_scanned_rcrd['title']
                )
            )
        except RedditQueryPivotNotFound:
            _get_logger().warning(
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
                submission_rcrd = _get_submission_record(submission)
                _get_logger().info("Scanning subreddit's {} submission with title: {}.".format(
                    sr_name, submission_rcrd['title']
                ))

                if first_scanned_rcrd is None:
                    first_scanned_rcrd = submission_rcrd

                if len(submission_rcrd['text']) > 0:
                    excerpts_set = _scan_text(submission_text_regex, submission_rcrd['text'])
                    if len(excerpts_set) > 0:
                        submission_rcrd['excerpts'] = list(excerpts_set)
                        _get_logger().info(
                            'Found match in text of submission with id {} for subreddit {}. Excerpts: {}.'.format(
                                submission_rcrd['id'], sr_name, excerpts_set
                            )
                        )
                        _notify_email(submission_rcrd, to_emails)
                        # _notify_sms(submission_rcrd, to_sms_numbers)
                        new_feed_text_pattern_matched_col_ref.document(submission_rcrd['id']).set(submission_rcrd)
                        continue

                if re.match(r'^.+\.(jpg|png|jpeg|bmp|tiff)$', submission_rcrd['url']):
                    excerpts_set = _scan_image(submission_rcrd['url'], submission_text_regex)
                    if len(excerpts_set) > 0:
                        submission_rcrd['excerpts'] = list(excerpts_set)
                        _get_logger().info(
                            'Found match in image of submission with id {} in subreddit {}. Excerpts: {}.'.format(
                                submission_rcrd['id'], sr_name, excerpts_set
                            )
                        )
                        _notify_email(submission_rcrd, to_emails)
                        # _notify_sms(submission_rcrd, to_sms_numbers)
                        _save_image_to_mega(submission_rcrd['url'])  # save image to the Mega.nz  service.
                        new_feed_image_pattern_matched_col_ref.document(submission_rcrd['id']).set(submission_rcrd)
        except Exception as e:
            _get_logger().error(
                "Halting subreddits scan. Failed to scan subreddit {}'s new feed due to: {}".format(sr_name, e),
                exc_info=True
            )
            _get_error_reporting_client().report_exception()
            return 1  # if scan failed first scanned submission shouldn't be stored.

        try:
            # Save first scanned document (most recent submission) to be used as pivot on next run.
            if first_scanned_rcrd is not None:
                _get_logger().info("Setting submission with title '{}' to collection {} for subreddit {}.".format(
                    first_scanned_rcrd['title'], new_feed_first_scanned_col_ref.id, sr_name
                ))
                new_feed_first_scanned_col_ref.document(first_scanned_rcrd['id']).set(first_scanned_rcrd)
            else:
                _get_logger().info(
                    'No new submissions found on subreddit\'s {} new feed. Setting pivot as last submission.'
                        .format(sr_name)
                )
                new_feed_first_scanned_col_ref.document(prev_run_first_scanned_rcrd['id']) \
                    .set(prev_run_first_scanned_rcrd)
        except Exception as e:
            _get_logger().warning(f"Unable to persist final state due to: {e}", exc_info=True)
            _get_error_reporting_client().report_exception()

    _get_logger().info("Finished scanning subreddits.")
    return 0
