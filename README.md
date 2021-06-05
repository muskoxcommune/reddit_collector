# reddit_collector

Given a list of subreddits, craws them and records post stats.

## Basic stats
- avg_age_seconds
- num_posts
- num_comments
- num_crossposts
- num_selftexts
- num_ups
- num_urls

## Examples
For commandline execution:
```
./scratch.py \
        --id <client_id> \
        --secret <client_secret> \
        --user <user> \
        --password <password> \
    amcstock \
    gme \
    superstonk \
    wallstreetbets
```
For an hourly cron job:
```
0 * * * * <path to>/scratch.py --id <client_id> --secret <client_secret> --user <user> --password <password> amcstock gme superstonk wallstreetbets
```
WARNING: Avoid exposing secrets in crontabs and shell histories.

## Help text
```
usage: reddit_collector.py [-h] --id ID [--max-age SECONDS] [--out-dir DIRECTORY] --password PASSWORD --secret SECRET --user USER subreddit [subreddit ...]

positional arguments:
  subreddit            subreddit to craw

optional arguments:
  -h, --help           show this help message and exit
  --id ID              app client ID
  --max-age SECONDS    threshold for excluding posts by age
  --out-dir DIRECTORY  directory for writing output
  --password PASSWORD  reddit development user password
  --secret SECRET      app client secret
  --user USER          reddit development user
```

## Install spaCy
```
pip3 install spacy
# See: https://spacy.io/models/en
python3 -m spacy download en_core_web_lg
```
