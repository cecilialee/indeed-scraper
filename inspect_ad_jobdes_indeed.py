import pandas
import requests as rqs
from bs4 import BeautifulSoup
import webbrowser



WHAT = "Data Scientist"
WHERE = "Hong Kong"

# Inspect problematic ads
# ============================================================================

data = pandas.read_csv('datascientist_hongkong_indeed_20180102.csv')
len_jobdes = data['ad_jobdes_indeed'].apply(len)

bug_len = 100
filter_bug_jobdes = len_jobdes[len_jobdes < bug_len]
id_bug_jobdes = data.iloc[filter_bug_jobdes.keys()]['ad_id_indeed']

bug_jk = list(id_bug_jobdes)

filter_ok_jobdes = len_jobdes[len_jobdes > bug_len]
id_ok_jobdes = data.iloc[filter_ok_jobdes.keys()]['ad_id_indeed'][:10]

ok_jk = list(id_ok_jobdes)

print("\nThere are {} ads with an `ad_jobdes_indeed` of length smaller than {}:\n\n{}".format(len(id_bug_jobdes), bug_len, id_bug_jobdes))
# print("\nExamples of normal ads:\n\n{}".format(id_ok_jobdes))

# Helper functions
# ============================================================================

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


# Parse `ad_jobdes_indeed`
# ============================================================================

def get_ad_jobdes_indeed(jk, parser):

	url = get_viewjob_url(jk)
	viewjob_page = rqs.get(url)
	viewjob_soup = BeautifulSoup(viewjob_page.text, parser)

	for content in viewjob_soup.find_all('span', {'id': 'job_summary'}):
		# job_data['ad_jobdes_indeed'] = content.text
		print(content.text)

	webbrowser.open_new(url)