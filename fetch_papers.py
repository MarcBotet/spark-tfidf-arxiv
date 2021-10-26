# code inspired in https://github.com/karpathy/arxiv-sanity-preserver

import time
import urllib.request
from pathlib import Path

import feedparser
import yaml

from data_to_cassandra import Cassandra


def encode_feedparser_dict(d):

	if isinstance(d, feedparser.FeedParserDict) or isinstance(d, dict):
		j = {}
		for k in d.keys():
			j[k] = encode_feedparser_dict(d[k])
		return j
	elif isinstance(d, list):
		l = []
		for k in d:
			l.append(encode_feedparser_dict(k))
		return l
	else:
		return d


def parse_arxiv_url(url):
	ix = url.rfind('/')
	idversion = url[ix + 1:]  # extract just the id (and the version)
	parts = idversion.split('v')
	assert len(parts) == 2, 'error parsing url ' + url
	return parts[0], int(parts[1])


if __name__ == "__main__":

	config = yaml.load(Path('config.yml').read_text(), Loader=yaml.SafeLoader)
	start_index = config['start_index']
	max_index = config['max_index']
	results_per_iteration = config['results_per_iteration']
	search_query = config['search_query']
	wait_time = config['wait_time']

	base_url = 'http://export.arxiv.org/api/query?'  # base api query url
	print('Searching arXiv for CS papers')

	cassandra = Cassandra()

	num_added_total = 0
	for i in range(start_index, max_index, results_per_iteration):
		db = {}

		print(f"Results {i} - {i + results_per_iteration}")
		query = 'search_query=%s&sortBy=lastUpdatedDate&start=%i&max_results=%i' % (search_query, i, results_per_iteration)

		with urllib.request.urlopen(base_url + query) as url:
			response = url.read()

		parse = feedparser.parse(response)
		num_added = 0

		for e in parse.entries:

			j = encode_feedparser_dict(e)

			# to get the paper pid
			rawid, version = parse_arxiv_url(j['id'])
			j['_rawid'] = rawid
			j['_version'] = version

			db[rawid] = j

		# print some information
		print('Added num_added papers')

		if len(parse.entries) == 0:
			print('Received no results from arxiv. Probably rate limiting')
			break

		## adding to cassandra
		cassandra.db_to_cassandra(db)
		num_added_total += len(db)

		print(f'Sleeping for {wait_time} seconds' )
		time.sleep(wait_time)

	print(f'It has been added {num_added_total} new papers to the database')
