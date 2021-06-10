#!/opt/homebrew/bin/python3
from collections import Counter
from datetime import datetime, timezone
from textblob import TextBlob

import argparse
import copy
import json
import logging
import multiprocessing
import pathlib
import platform
import pprint
import ratelimit # https://pypi.org/project/ratelimit
import re
import requests
import spacy
import threading
import time
import traceback

# TODO:
# - Reject entities not in allow list
# - Paginate more comments

argparser = argparse.ArgumentParser()
argparser.add_argument('--dump-comments', action='store_true', default=False, help='dump comments to file')
argparser.add_argument('--dump-texts', action='store_true', default=False, help='dump text posts to file')
argparser.add_argument('--exclude-entity-label', metavar='LABEL', action='append', help='exclude specified entity types')
argparser.add_argument('--id', required=True, help='app client ID')
argparser.add_argument('--include-entity-label', metavar='LABEL', action='append', help='include only specified entity types')
argparser.add_argument('--max-age', metavar='SECONDS', type=int, default=3600*24, help='threshold for excluding posts by age')
argparser.add_argument('--out-dir', metavar='DIRECTORY', default='/tmp/reddit_collector', help='directory for writing output')
argparser.add_argument('--password', required=True, help='reddit development user password')
argparser.add_argument('--secret', required=True, help='app client secret')
argparser.add_argument('--skip-entities', action='store_true', default=False, help='skip entity analysis')
argparser.add_argument('-w', '--word-stats', metavar='WORD', action='append', help='worg to get stats for')
argparser.add_argument('--user', required=True, help='reddit development user')
argparser.add_argument('-v', '--verbose', action='store_true', default=False, help='verbose logging')
argparser.add_argument('--workers', metavar='NUM', type=int, default=8, help='number of workers')
argparser.add_argument('subreddit', nargs='+', help='subreddit to craw')
args = argparser.parse_args()

date_blob = datetime.now(timezone.utc).strftime('%Y%m%d')

subreddit_stats_template = {
    'negative_polarity_ratio': 0,
    'num_awards': 0,
    'num_comment_candidates': 0,
    'num_comments': 0,
    'num_interactions': 0,
    'num_polarity_eq_0': 0,
    'num_polarity_gt_0': 0,
    'num_polarity_lt_0': 0,
    'num_posts': 0,
    'num_selftexts': 0,
    'num_ups': 0,
    'num_urls': 0,
    'positive_polarity_ratio': 0,
}

oauth_endpoint = 'https://oauth.reddit.com'
www_endpoint = 'https://www.reddit.com'
user_agent = (platform.system() + ' ' + platform.release() + '; '
              + 'reddit_collector/v0.1 (by /u/' + args.user + ')')

out_dir_path = pathlib.Path(args.out_dir)
if not out_dir_path.exists():
    out_dir_path.mkdir()
logging.basicConfig(
    datefmt='%m/%d/%Y %I:%M:%S %p',
    encoding='utf-8',
    filename=(args.out_dir + '/reddit_collector.log'),
    format='%(levelname)s\t%(asctime)s\t%(message)s',
    level=logging.DEBUG)

# Reddit requires capping requests at 1 rps.
# https://github.com/reddit-archive/reddit/wiki/API#rules
@ratelimit.limits(calls=1, period=1)
def do_get(api_url, access_token, api_params={}):
    response = requests.get(
        url=api_url,
        params=api_params,
        headers={
            'Authorization': 'bearer ' + access_token,
            'User-Agent': user_agent,
        })
    return response

def dump_data(data, data_dir):
    dump_file_filename = args.out_dir + '/' + data_dir + '/' + date_blob + '/' + data['permalink'].replace('/', '_') + '.json'
    if args.verbose:
        logging.debug('Writing %s', dump_file_filename)
    with open(dump_file_filename, 'w') as fd:
        json.dump(data, fd)

def finalize_delegate_result(subreddit_name, delegator_queue, stats):
    word_stats, entities, sentiment_polarity = delegator_queue.get()
    # Entities
    stats['subreddit'][subreddit_name]['_tmp']['entities'] += entities
    if sentiment_polarity is None:
        pass # entity analysis is disabled
    elif sentiment_polarity == 0:
        stats['subreddit'][subreddit_name]['stats']['num_polarity_eq_0'] += 1
    elif sentiment_polarity > 0:
        stats['subreddit'][subreddit_name]['stats']['num_polarity_gt_0'] += 1
    else:
        stats['subreddit'][subreddit_name]['stats']['num_polarity_lt_0'] += 1
    # Word stats
    for match, count in word_stats.items():
        if match in stats['subreddit'][subreddit_name]['stats']['word']:
            stats['subreddit'][subreddit_name]['stats']['word'][match] += count
        else:
            stats['subreddit'][subreddit_name]['stats']['word'][match] = count
    return stats

def join_delegates(delegator, stats, subreddit_name, block=False):
    num_joined = 0
    num_finalized = 0
    while delegator['delegates']:
        delegate = delegator['delegates'].pop(0)
        if delegate.is_alive() and block is False:
            break
        else:
            delegate.join()
            num_joined += 1
    logging.debug('Joined %d delegates', num_joined)
    while not delegator['queue'].empty():
        stats = finalize_delegate_result(subreddit_name, delegator['queue'], stats)
        num_finalized += 1
    logging.debug('Finalized %d results', num_finalized)
    return delegator, stats

def paginate_and_process(url, access_token, hook, delegator, nlp, stats, subreddit_name,
                         api_params_template={'limit': 100, 'show': 'all'}):
    abort = False
    after = None
    failures = 0
    paginate_enabled = True
    while paginate_enabled is True:
        api_params = copy.deepcopy(api_params_template)
        if after is not None:
            api_params['after'] = after
        try:
            response = do_get(url, access_token, api_params=api_params)
            response, stats = process_response(response, stats)
            if response is not None:
                payload = response.json()
                delegator, stats, after = hook(payload, delegator, nlp, stats, subreddit_name)
                if after is None:
                    paginate_enabled = False
            elif failures <= 3:
                failures += 1
            else:
                abort = True
                break
        except ratelimit.exception.RateLimitException as exc:
            if delegator['delegates']:
                # In between API calls, join finished delegates and clear out the queue.
                delegator, stats = join_delegates(delegator, stats, subreddit_name)
            else:
                time.sleep(0.1)
        except Exception as exc:
            logging.error(traceback.format_exc())
            failures += 1
    return delegator, stats, abort

def process_article(payload, delegator, nlp, stats, subreddit_name):
    #post_list = payload[0]
    comment_list = payload[1]
    return process_comment_list(comment_list, delegator, nlp, stats, subreddit_name, skip_age_check=True)

def process_comment_list(payload, delegator, nlp, stats, subreddit_name, skip_age_check=False):
    logging.debug('Processing %s comments', len(payload['data']['children']))
    age_seconds = None
    for comment in payload['data']['children']:
        if comment['kind'] == 'more':
            stats['subreddit'][subreddit_name]['_tmp']['more_comments'].append(comment['data'])
            continue
        age_seconds = datetime.now(timezone.utc).timestamp() - float(comment['data']['created_utc'])
        if skip_age_check is False and age_seconds >= args.max_age:
            continue
        logging.debug('Processing comment, age: %d, length: %d', age_seconds, len(comment['data']['body']))
        delegate = threading.Thread(target=process_text_blob,
                                    args=(nlp, comment['data']['body'], delegator['queue']))
        delegate.start()
        delegator['delegates'].append(delegate)
        if comment['data']['replies']:
            process_comment_list(comment['data']['replies'], delegator, nlp, stats, subreddit_name, skip_age_check=True)
        stats['subreddit'][subreddit_name]['stats']['num_awards'] += int(comment['data']['total_awards_received'])
        stats['subreddit'][subreddit_name]['stats']['num_interactions'] += 1
        stats['subreddit'][subreddit_name]['stats']['num_ups'] += int(comment['data']['ups'])
        if args.dump_comments:
            delegate = threading.Thread(target=dump_data,
                                        args=(comment['data'], 'comments'))
            delegate.start()
            delegator['delegates'].append(delegate)
    if age_seconds == None:
        logging.debug('Did not find any viable comments in: %s', payload)
        return delegator, stats, None
    if age_seconds > args.max_age:
        logging.debug('Found comment created %s seconds ago. Disabling pagination.', age_seconds)
        return delegator, stats, None
    else:
        return delegator, stats, payload['data']['after']

def process_post_list(payload, delegator, nlp, stats, subreddit_name):
    logging.debug('Processing %s posts', len(payload['data']['children']))
    age_seconds = None
    for post in payload['data']['children']:
        age_seconds = datetime.now(timezone.utc).timestamp() - float(post['data']['created_utc'])
        if age_seconds >= args.max_age:
            break
        logging.debug('Processing post, age: %d, length: %d', age_seconds, len(post['data']['title']))
        # Glob title and text body together for entity extraction.
        delegate = threading.Thread(target=process_text_blob,
                                    args=(nlp, post['data']['title'] + '. ' + post['data']['selftext'], delegator['queue']))
        delegate.start()
        delegator['delegates'].append(delegate)
        stats['subreddit'][subreddit_name]['stats']['num_awards'] += int(post['data']['total_awards_received'])
        stats['subreddit'][subreddit_name]['stats']['num_comments'] += int(post['data']['num_comments'])
        stats['subreddit'][subreddit_name]['stats']['num_interactions'] += 1
        stats['subreddit'][subreddit_name]['stats']['num_posts'] += 1
        stats['subreddit'][subreddit_name]['stats']['num_ups'] += int(post['data']['ups'])
        if not post['data']['selftext']:
            stats['subreddit'][subreddit_name]['stats']['num_urls'] += 1
        else:
            stats['subreddit'][subreddit_name]['stats']['num_selftexts'] += 1
            if args.dump_texts:
                delegate = threading.Thread(target=dump_data,
                                            args=(post['data'], 'selftexts'))
                delegate.start()
                delegator['delegates'].append(delegate)
        if int(post['data']['ups']) > 100 and int(post['data']['num_comments']) > 0:
            stats['subreddit'][subreddit_name]['_tmp']['comment_candidates'].append(post['data']['permalink'])
    if age_seconds == None:
        logging.debug('Did not find any viable posts in: %s', payload)
        return delegator, stats, None
    if age_seconds > args.max_age:
        logging.debug('Found post created %s seconds ago. Disabling pagination.', age_seconds)
        return delegator, stats, None
    else:
        return delegator, stats, payload['data']['after']

def process_response(response, stats):
    if response.status_code in stats['script']['status_codes']:
        stats['script']['status_codes'][response.status_code] += 1
    else:
        stats['script']['status_codes'][response.status_code] = 1
    if response.status_code == 200:
        return response, stats
    else:
        logging.error('%s %s %s', response, response.headers, response.content)
        return None, stats

def process_text_blob(nlp, blob, queue):
    filtered_entities = []
    sentiment = None
    word_stats = {}
    if args.skip_entities is False:
        text = nlp(blob)
        # See NER: https://spacy.io/models/en#en_core_web_sm-labels
        if args.include_entity_label:
            for ent in text.ents:
                if ent.label_ in args.include_entity_label:
                    filtered_entities.append((ent.text, ent.label_))
        elif args.exclude_entity_label:
            for ent in text.ents:
                if ent.label_ not in args.exclude_entity_label:
                    filtered_entities.append((ent.text, ent.label_))
        else:
            filtered_entities = [(ent.text, ent.label_) for ent in text.ents]
        sentiment = TextBlob(blob).sentiment
        if args.verbose:
            logging.debug('%s, entities: %s, text: %s', sentiment, text.ents, blob)
    for target_word in args.word_stats:
        matches = re.findall('[^0-9a-zA-Z]*(' + target_word + ')[^0-9a-zA-Z]*', blob)
        num_matches = len(matches)
        if num_matches:
            for match in matches:
                if match in word_stats:
                    word_stats[match] += 1
                else:
                    word_stats[match] = 1
            logging.debug('Found word: %s, %d times', target_word, num_matches)
    queue.put((word_stats, filtered_entities, sentiment))

if __name__ == '__main__':

    access_token_filename = '/tmp/reddit.token.json'
    access_token_path = pathlib.Path(access_token_filename)
    access_token = None
    # Fetch new token if token file is not found or token file is older
    # than 45 minutes. Default token expiration is 60 minutes.
    if (not access_token_path.exists()
            or datetime.now(timezone.utc).timestamp() - access_token_path.stat().st_mtime > 2700):
        access_token_failures = 0
        while access_token is None:
            response = requests.post(
                url=(www_endpoint + '/api/v1/access_token'),
                auth=requests.auth.HTTPBasicAuth(args.id, args.secret),
                data={
                    'grant_type': 'password',
                    'username': args.user,
                    'password': args.password
                },
                headers={
                    'User-Agent': user_agent,
                })
            if response.status_code == 200:
                access_token = response.json()['access_token']
                with open(access_token_filename, 'w') as fd:
                    fd.write(response.content.decode('utf-8'))
                    logging.info('New process, using access token %s', access_token)
            elif access_token_failures < 3:
                    logging.warn('Failed to get access token: %s %s %s', response, response.headers, response.text)
                    access_token_failures += 1
            else:
                logging.error('Unable to get access token. Exiting.')
                exit(1)
    else:
        with open(access_token_filename) as fd:
            access_token = json.load(fd)['access_token']
            logging.info('New process, using existing access token %s, last modified %d seconds ago',
                access_token, datetime.now(timezone.utc).timestamp() - access_token_path.stat().st_mtime)

    # Check output directory
    if args.dump_comments:
        selftext_path = pathlib.Path(args.out_dir + '/comments/' + date_blob)
        if not selftext_path.exists():
            selftext_path.mkdir(parents=True)
    if args.dump_texts:
        selftext_path = pathlib.Path(args.out_dir + '/selftexts/' + date_blob)
        if not selftext_path.exists():
            selftext_path.mkdir(parents=True)

    nlp = spacy.load('en_core_web_sm')
    logging.debug("Pipeline: %s", nlp.pipe_names)

    # Process subreddits
    abort = False
    delegator = {
        'delegates': [],
        'queue': multiprocessing.Queue(),
    }
    stats = {
        'script': {
            'status_codes': {}},
        'subreddit': {},
    }
    for subreddit_name in args.subreddit:
        # Initialize stats map
        stats['subreddit'][subreddit_name] = {
            '_tmp': {
                'comment_candidates': [],
                'entities': [],
                'more_comments': [],
            },
            'stats': copy.deepcopy(subreddit_stats_template)
        }
        stats['subreddit'][subreddit_name]['stats']['word'] = {}

        delegator, stats, abort = paginate_and_process(
            oauth_endpoint + '/r/' + subreddit_name + '/new', access_token,
            process_post_list, delegator, nlp, stats, subreddit_name)
        if abort is True:
            break
        # Currently, /r/[subreddit]/comments doesn't return enough comments.
        #delegator, stats, abort = paginate_and_process(
        #    oauth_endpoint + '/r/' + subreddit_name + '/comments', access_token,
        #    process_comment_list, delegator, nlp, stats, subreddit_name)
        for permalink in stats['subreddit'][subreddit_name]['_tmp']['comment_candidates']:
            delegator, stats, abort = paginate_and_process(
                oauth_endpoint + permalink, access_token,
                process_article, delegator, nlp, stats, subreddit_name,
                api_params_template={'depth': 100, 'limit': 100, 'showmore': 1, 'sort': 'confidence', 'threaded': 1})
            if abort is True:
                break
        if abort is True:
            break
    if abort is True:
        logging.error('Run aborted')
    else:
        for subreddit_name in args.subreddit:
            delegator, stats = join_delegates(delegator, stats, subreddit_name, block=True)

            stats['subreddit'][subreddit_name]['stats']['num_comment_candidates'] = len(
                stats['subreddit'][subreddit_name]['_tmp']['comment_candidates'])

            if stats['subreddit'][subreddit_name]['stats']['num_interactions'] > 0:
                stats['subreddit'][subreddit_name]['stats']['negative_polarity_ratio'] = (
                    stats['subreddit'][subreddit_name]['stats']['num_polarity_lt_0'] / stats['subreddit'][subreddit_name]['stats']['num_interactions'])
                stats['subreddit'][subreddit_name]['stats']['positive_polarity_ratio'] = (
                    stats['subreddit'][subreddit_name]['stats']['num_polarity_gt_0'] / stats['subreddit'][subreddit_name]['stats']['num_interactions'])

            stats['subreddit'][subreddit_name]['stats']['top_entities'] = Counter(
                stats['subreddit'][subreddit_name]['_tmp']['entities']).most_common()[:50]
            del stats['subreddit'][subreddit_name]['_tmp']

        logging.debug('Script stats:\n%s', pprint.pformat(stats['script']))
        logging.debug('Subreddit stats:\n%s', pprint.pformat(stats['subreddit']))
        sorted_subreddits = sorted(stats['subreddit'].keys())
        for stat_name in subreddit_stats_template.keys():
            stat_file_filename = args.out_dir + '/subreddit.' + stat_name + '.tsv'
            logging.info('Generating %s stats in %s', stat_name, stat_file_filename)

            data = []
            for subreddit_name in sorted_subreddits:
                data.append(stats['subreddit'][subreddit_name]['stats'][stat_name])
            serialized_data = '\t'.join([datetime.now(timezone.utc).strftime('%m/%d/%Y %H:%M:%S')] + [str(value) for value in data]) + '\n'

            stat_file_path = pathlib.Path(stat_file_filename)
            if not stat_file_path.exists() or stat_file_path.stat().st_size == 0:
                with open(stat_file_filename, 'w') as fd:
                    fd.write('\t'.join(['timestamp'] + sorted_subreddits) + '\n')
                    fd.write(serialized_data)
            else:
                with open(stat_file_filename, 'a') as fd:
                    fd.write(serialized_data)

