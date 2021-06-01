#!/opt/homebrew/bin/python3
from ratelimit import limits  # https://pypi.org/project/ratelimit
from datetime import datetime, timezone

import argparse
import concurrent.futures
import copy
import json
import logging
import pathlib
import platform
import pprint
import requests
import traceback

access_token_filename = '/tmp/reddit.token.json'
access_token_path = pathlib.Path(access_token_filename)
access_token = None

argparser = argparse.ArgumentParser()
argparser.add_argument('--dump-selftext', action='store_true', default=False, help='dump text posts to file')
argparser.add_argument('--id', required=True, help='app client ID')
argparser.add_argument('--workers', metavar='NUM', type=int, default=8, help='number of workers (not used for making requests)')
argparser.add_argument('--max-age', metavar='SECONDS', type=int, default=3600*24, help='threshold for excluding posts by age')
argparser.add_argument('--out-dir', metavar='DIRECTORY', default='/tmp/reddit_collector', help='directory for writing output')
argparser.add_argument('--password', required=True, help='reddit development user password')
argparser.add_argument('--secret', required=True, help='app client secret')
argparser.add_argument('--user', required=True, help='reddit development user')
argparser.add_argument('subreddit', nargs='+', help='subreddit to craw')
args = argparser.parse_args()

stats = {
    'script': {
        'status_codes': {}},
    'subreddit': {}
}
subreddit_stats_template = {
    'avg_age_seconds': 0,
    'num_posts': 0,
    'num_comments': 0,
    'num_crossposts': 0,
    'num_selftexts': 0,
    'num_ups': 0,
    'num_urls': 0,
}
user_agent = (platform.system() + ' ' + platform.release() + '; '
              + 'reddit_collector/v0.1 (by /u/' + args.user + ')')

logging.basicConfig(
    datefmt='%m/%d/%Y %I:%M:%S %p',
    encoding='utf-8',
    format='%(levelname)s\t%(asctime)s\t%(message)s',
    level=logging.DEBUG)

# Reddit requires capping requests at 1 rps.
# https://github.com/reddit-archive/reddit/wiki/API#rules
@limits(calls=1, period=1)
def do_get(api_url, api_params={}):
    response = requests.get(
        url=api_url,
        params=api_params,
        headers={
            'Authorization': 'bearer ' + access_token,
            'User-Agent': user_agent,
        })
    logging.debug('%s %s', response, response.headers)
    if response.status_code in stats['script']['status_codes']:
        stats['script']['status_codes'][response.status_code] += 1
    else:
        stats['script']['status_codes'][response.status_code] = 1
    return response

def dump_post_data(data):
    dump_file_filename = args.out_dir + '/selftext/' + data['permalink'].replace('/', '_') + '.json'
    logging.debug('Writing %s', dump_file_filename)
    with open(dump_file_filename, 'w') as fd:
        json.dump(fd, data)

def process_post_list(response, process_pool, subreddit_name=None):
    response_data = response.json()
    try:
        assert response_data['data']
        assert response_data['data']['children']
        logging.debug('Processing %s posts', len(response_data['data']['children']))
        age_seconds = 0
        for post in response_data['data']['children']:
            age_seconds = datetime.now(timezone.utc).timestamp() - float(post['data']['created_utc'])
            if age_seconds >= args.max_age:
                break
            stats['subreddit'][subreddit_name]['_tmp']['authors'].add(post['data']['author'])
            stats['subreddit'][subreddit_name]['stats']['num_posts'] += 1
            stats['subreddit'][subreddit_name]['stats']['num_comments'] += int(post['data']['num_comments'])
            stats['subreddit'][subreddit_name]['stats']['num_crossposts'] += int(post['data']['num_crossposts'])
            stats['subreddit'][subreddit_name]['stats']['num_ups'] += int(post['data']['ups'])
            stats['subreddit'][subreddit_name]['_tmp']['sum_age_seconds'] += age_seconds
            if not post['data']['selftext']:
                logging.debug('created_utc: %s, age_seconds: %f, ups: %s, num_comments: %s, num_crossposts: %s, type: url, title: %s',
                    post['data']['created_utc'],
                    age_seconds,
                    post['data']['ups'],
                    post['data']['num_comments'],
                    post['data']['num_crossposts'],
                    post['data']['title'])
                stats['subreddit'][subreddit_name]['stats']['num_urls'] += 1
            else:
                logging.debug('created_utc: %s, age_seconds: %f, ups: %s, num_comments: %s, num_crossposts: %s, type: selftext, title: %s',
                    post['data']['created_utc'],
                    age_seconds,
                    post['data']['ups'],
                    post['data']['num_comments'],
                    post['data']['num_crossposts'],
                    post['data']['title'])
                stats['subreddit'][subreddit_name]['stats']['num_selftexts'] += 1
                if args.dump_selftext:
                    process_pool.submit(dump_post_data, post['data'])
        return None if age_seconds == 0 or age_seconds > args.max_age else response_data['data']['after']
    except Exception as exc:
        logging.error(traceback.format_exc())

if __name__ == '__main__':

    # Fetch new token if token file is not found or token file is older
    # than 59 minutes. Default token expiration is 60 minutes.
    if (not access_token_path.exists()
            or datetime.now(timezone.utc).timestamp() - access_token_path.stat().st_mtime > 3540):
        logging.info('Fetching new access token')
        response = requests.post(
            url='https://www.reddit.com/api/v1/access_token',
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
    else:
        logging.info('Using existing access token')
        with open(access_token_filename) as fd:
            access_token = json.load(fd)['access_token']

    logging.info('Using token %s, last modified %d seconds ago',
        access_token, datetime.now(timezone.utc).timestamp() - access_token_path.stat().st_mtime)

    out_dir_path = pathlib.Path(args.out_dir)
    if not out_dir_path.exists():
        out_dir_path.mkdir()
    if args.dump_selftext:
        selftext_path = pathlib.Path(args.out_dir + '/selftext')
        if not selftext_path.exists():
            selftext_path.mkdir()

    process_pool = concurrent.futures.ProcessPoolExecutor(max_workers=args.workers)

    for subreddit_name in args.subreddit:
        stats['subreddit'][subreddit_name] = {
            '_tmp': {
                'authors': set(),
                'sum_age_seconds': 0
            },
            'stats': copy.deepcopy(subreddit_stats_template)
        }
        after = None
        api_url = 'https://oauth.reddit.com/r/' + subreddit_name + '/new'
        paginate_enabled = True
        while paginate_enabled is True:
            api_params={'limit': 100, 'show': 'all'}
            if after is not None:
                api_params['after'] = after
            response = do_get(api_url, api_params=api_params)
            if response.status_code == 200:
                after = process_post_list(response, process_pool, subreddit_name=subreddit_name)
                if after is None:
                    paginate_enabled = False
        if stats['subreddit'][subreddit_name]['stats']['num_posts'] > 0:
            stats['subreddit'][subreddit_name]['stats']['avg_age_seconds'] = (
                stats['subreddit'][subreddit_name]['_tmp']['sum_age_seconds'] / stats['subreddit'][subreddit_name]['stats']['num_posts'])
        else:
            stats['subreddit'][subreddit_name]['stats']['avg_age_seconds'] = 0
        stats['subreddit'][subreddit_name]['stats']['num_authors'] = len(
            stats['subreddit'][subreddit_name]['_tmp']['authors'])
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

    process_pool.shutdown()
