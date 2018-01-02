import scraperwiki
from bs4 import BeautifulSoup
import requests as rqs
from datetime import datetime
import time
import re
import numpy as np
from collections import defaultdict
from collections import OrderedDict
import unicodecsv as csv


# User parameters ========================================
START_SCRAPING = True
WHAT = "Data Scientist"
WHERE = "Hong Kong"
RECORD_CSV = True
RECORD_DB = True
# FIELDNAMES = ['scrape_datetime', 'jk', 'job_title', 'company', 'job_location', 'post_datetime', 'job_description']
FIELDNAMES = ['epoch', 'scrping_dt', 'ad_id_indeed', 'ad_jobtitle_indeed', 'ad_cie_indeed', 'ad_jobloc_indeed', 'ad_post_dt_indeed', 'ad_jobdes_indeed', 'search_ad_url', 'ad_url', 'ad_jobdate', 'ad_jobtitle', 'ad_jobcie', 'ad_jobdes', 'ad_email']
EXECTIME = datetime.now()
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

	# Make Soup with Indeed *job* page.
	job_soup = get_job_soup(what, where, start)

	# Scrape every job data (expecting 15 results) on Indeed job page.
	for job in job_soup.find_all('div', {'class': 'row'}):

		# Initiate new OrderedDict to store a *single* job data.
		job_data = OrderedDict()

		# `epoch`
		job_data['epoch'] = time.mktime(EXECTIME.timetuple())

		# `scrping_dt`
		job_data['scrping_dt'] = EXECTIME.strftime("%Y-%m-%d %H:%M:%S")

		# `ad_id_indeed` : unique Indeed job id ('jk')
		jk = job.attrs['data-jk']
		job_data['ad_id_indeed'] = jk

		# `ad_jobtitle_indeed`
		for content in job.find_all('a', {'data-tn-element': 'jobTitle'}):
			job_data['ad_jobtitle_indeed'] = content.text.lstrip()

		# `ad_cie_indeed`
		# `ad_jobloc_indeed`
		# `ad_post_dt_indeed`
		for field in [['company', 'ad_cie_indeed'], ['location', 'ad_jobloc_indeed'], ['date', 'ad_post_dt_indeed']]:
			for content in job.find_all('span', {'class': field[0]}):
				job_data[field[1]] = content.text.lstrip()

		# Make Soup with Indeed *view job* page by passing the jk value (`ad_id_indeed`).
		viewjob_page = rqs.get(get_viewjob_url(jk))
		viewjob_soup = BeautifulSoup(viewjob_page.text, 'lxml')

		# `ad_jobdes_indeed`
		for content in viewjob_soup.find_all('span', {'id': 'job_summary'}):
			job_data['ad_jobdes_indeed'] = content.text

		# `search_ad_url`
		# `ad_url`
		for content in job.find_all('a', {'class': 'turnstileLink'}):
			search_ad_url = 'https://www.indeed.hk' + content['href']
			job_data['search_ad_url'] = search_ad_url

			try:
				ad_url = rqs.get(search_ad_url).url
				job_data['ad_url'] = ad_url
			except:
				ad_url = search_ad_url
				job_data['ad_url'] = search_ad_url


		# Ad scraper data. Temporarily set to nan.
		# `ad_jobdate`
		# `ad_jobtitle`
		# `ad_jobcie`
		# `ad_jobdes`
		# `ad_email`
		for item in ['ad_jobdate', 'ad_jobtitle', 'ad_jobcie', 'ad_jobdes', 'ad_email']:
			job_data[item] = np.nan

		# Append single job data to `all_job_data` using `ad_url` as key.
		page_job_data[ad_url] = job_data

		print('Get {}'.format(ad_url))

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
		print("Scraping start = {0}.".format(i))
		page_job_data = scrape_single_page(what, where, start = i)
		all_job_data.update(page_job_data)

	return all_job_data


def write_to_csv(filename, data):
	with open(filename, 'w') as csvfile:
		writer = csv.DictWriter(csvfile, fieldnames = FIELDNAMES)
		writer.writeheader()
		for value in data.values():
			writer.writerow(value)

def write_to_db(data):
	for value in data.values():
		scraperwiki.sqlite.save(unique_keys = ['ad_url'], data = value)



def scrape_indeed(what=WHAT, where=WHERE, record_csv=RECORD_CSV, record_db=RECORD_DB):

	print("Starting to scrape Indeed: {0} in {1}.".format(what, where))

	data = scrape_all_pages(what, where)
	scrape_date = EXECTIME.strftime("%Y%m%d")

	if record_csv:
		csv_filename = '{0}_{1}_{2}_{3}.csv'.format(what, where, 'indeed', scrape_date).lower().replace(' ', '')
		print("Writing to '{0}'.".format(csv_filename))
		write_to_csv(csv_filename, data)
		print("Writing completed. View your data at '{0}'.".format(csv_filename))

	if record_db:
		print("Writing to scraperwiki database.")
		write_to_db(data)
		print("Writing completed. View your data at 'data.sqlite'.")

	print("Scraping completed.")


if START_SCRAPING:
	scrape_indeed()