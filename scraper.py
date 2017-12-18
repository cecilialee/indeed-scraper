# This is a template for a Python scraper on morph.io (https://morph.io)
# including some code snippets below that you should find helpful

# import scraperwiki
# import lxml.html
#
# # Read in a page
# html = scraperwiki.scrape("http://foo.com")
#
# # Find something on the page using css selectors
# root = lxml.html.fromstring(html)
# root.cssselect("div[align='left']")
#
# # Write out to the sqlite database using scraperwiki library
# scraperwiki.sqlite.save(unique_keys=['name'], data={"name": "susan", "occupation": "software developer"})
#
# # An arbitrary query against the database
# scraperwiki.sql.select("* from data where 'name'='peter'")

# You don't have to do things with the ScraperWiki and lxml libraries.
# You can use whatever libraries you want: https://morph.io/documentation/python
# All that matters is that your final data is written to an SQLite database
# called "data.sqlite" in the current working directory which has at least a table
# called "data".

import scraperwiki
from bs4 import BeautifulSoup
import requests as rqs
from datetime import datetime
import re
import numpy as np
from collections import defaultdict
from collections import OrderedDict
import pandas as pd
import unicodecsv as csv


# User parameters ========================================
START_SCRAPING = True
WHAT = "Data Scientist"
WHERE = "Hong Kong"
RECORD_CSV = True
RECORD_EXCEL = True
RECORD_DB = True
FIELDNAMES = ['scrape_datetime', 'jk', 'job_title', 'company', 'job_location', 'post_datetime', 'job_description']
# ========================================================


def get_job_url(what = WHAT, where = WHERE, start = 0):
	"""Return the URL of the Indeed *job* query."""
	return 'https://www.indeed.hk/jobs?q=' + what.replace(' ', '+') + '&l=' + where.replace(' ', '+') + '&start=' + str(start)


def get_viewjob_url(jk):
	"""Return the URL of the Indeed *view job* query."""
	return 'https://www.indeed.hk/viewjob?jk=' + jk


def get_job_soup(what = WHAT, where = WHERE, start = 0):
	"""Return BeautifulSoup for Indeed *job* page."""
	job_page = rqs.get(get_job_url(what, where, start))
	return BeautifulSoup(job_page.text, 'html.parser')


def scrape_single_page(what = WHAT, where = WHERE, start = 0):
	"""Scrape a single Indeed page."""

	# Initiate new dict to store *all* job data in one query.
	page_job_data = defaultdict(str)

	# Make BeautifulSoup with Indeed *job* page.
	job_soup = get_job_soup(what, where, start)

	# Scrape every job data (expecting 15 results) on Indeed job page.
	for job in job_soup.find_all('div', {'class': 'row'}):

		# Initiate new dict to store a *single* job data.
		# job_data = dict()
		job_data = OrderedDict()

		# `scrape_datetime`
		scrape_datetime = datetime.now()
		job_data['scrape_datetime'] = scrape_datetime

		# `jk` : unique Indeed job id
		jk = job.attrs['data-jk']
		job_data['jk'] = jk

		# `job_title`
		for content in job.find_all('a', {'data-tn-element': 'jobTitle'}):
			job_data['job_title'] = content.text.lstrip()

		# `company`
		# `job_location`
		# `post_datetime`
		for data in [['company', 'company'], ['location', 'job_location'], ['date', 'post_datetime']]:
			for content in job.find_all('span', {'class': data[0]}):
				job_data[data[1]] = content.text.lstrip()

		# TODO: wrangle `date` (str to datetime)
		# TODO: `external_url`

		# Make BeautifulSoup with Indeed *view job* page by passing the `jk` value.
		viewjob_page = rqs.get(get_viewjob_url(jk))
		viewjob_soup = BeautifulSoup(viewjob_page.text, 'html.parser')

		# `job_description`
		for content in viewjob_soup.find_all('span', {'id': 'job_summary'}):
			job_data['job_description'] = content.text

		# Append single job data to `all_job_data` using `jk` as key.
		page_job_data[jk] = job_data

	return page_job_data


def get_start_range(what = WHAT, where = WHERE):
	# TODO: docstring

	print("Getting start range.")

	# Make BeautifulSoup with Indeed *job* page.
	job_soup = get_job_soup(what, where)

	# Scrape the search count string from Indeed job page.
	for content in job_soup.find_all('div', {'id': 'searchCount'}):
		search_count = content.text.lstrip()

	# Extract total number of jobs found from the search_count string.
	pattern = r'\s[0-9]+$'
	total_jobs = int(re.findall(pattern, search_count)[0].lstrip())

	# Set start range.
	start_range = list(np.arange(0, total_jobs, 10))
	del start_range[-1]

	return start_range


def scrape_all_pages(what = WHAT, where = WHERE):
	# TODO: docstring

	start_range = get_start_range(what, where)

	all_job_data = defaultdict(str)

	for i in start_range:
		page_job_data = scrape_single_page(what, where, start = i)
		all_job_data.update(page_job_data)
		print("Scraping start = {0}.".format(i))

	return all_job_data


def write_to_csv(filename, data):
	with open(filename, 'w') as csvfile:
		writer = csv.DictWriter(csvfile, fieldnames = FIELDNAMES)
		writer.writeheader()
		for value in data.values():
			writer.writerow(value)


# TODO: def `write_to_excel`


def write_to_db(data):
	for value in data.values():
		scraperwiki.sqlite.save(unique_keys = ['jk'], data = value)



def scrape_indeed(what=WHAT, where=WHERE, record_csv=RECORD_CSV, record_excel=RECORD_EXCEL, record_db=RECORD_DB):

	print("Starting to scrape Indeed: {0} in {1}.".format(what, where))

	data = scrape_all_pages(what, where)

	date = datetime.now().strftime("%Y%m%d")

	if record_csv:
		csv_filename = '{0}_{1}_{2}_{3}.csv'.format(what, where, 'indeed', date).lower().replace(' ', '')
		print("Writing to '{0}'.".format(csv_filename))
		write_to_csv(csv_filename, data)
		print("Writing completed. View your data at '{0}'.".format(csv_filename))

	if record_excel:
		pass

	if record_db:
		print("Writing to scraperwiki database.")
		write_to_db(data)
		print("Writing completed. View your data at 'data.sqlite'.")

	print("Scraping completed.")


if START_SCRAPING:
	scrape_indeed()