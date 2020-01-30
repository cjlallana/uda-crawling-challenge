'''
Created on 2020/01/28

:author: Carlos Lallana
'''

# Python standard library
import logging, time, random, re, ast
from pprint import pprint

logging.basicConfig(level=logging.INFO)

# Third-party libraries
import requests
from bs4 import BeautifulSoup

# Global variables (actually used as constants)
BASE_URL = 'https://www.idealista.com/'

MAX_PROVINCES = 1
MAX_CITIES_PER_PROVINCE = 2
MAX_STREETS_PER_CITY = 2
MAX_STREET_NUMBERS_PER_STREET = 2


def get():

	# Create a Session object, wich will persist certain headers and parameters
	# across requests. It will have all the methods of the main Requests API.
	session = requests.Session()

	# Prepare a minimal header to avoid bot detection
	headers = {
	    'authority': 'www.idealista.com',
	    'cache-control': 'no-cache',
	    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36'
	}

	# Call the main URL
	content = get_url_content(	session=session,
								url=BASE_URL + 'maps/',
								headers=headers)

	if not content:
		logging.error('Error retrieving the main URL')
		return -1

	# Get a list of each province URL
	provinces_urls = get_provinces_urls(content, limit=MAX_PROVINCES)
	logging.info('Retrieved %d provinces URLs' % len(provinces_urls))

	# Get a list of each province city URL
	cities_urls = get_entities_urls(provinces_urls,
									session,
									headers,
									limit=MAX_CITIES_PER_PROVINCE)

	logging.info('Total cities retrieved: %s' % len(cities_urls))

	streets_urls = get_entities_urls(cities_urls,
									session, 
									headers, 
									limit=MAX_STREETS_PER_CITY)

	logging.info('Total streets retrieved: %s' % len(streets_urls))

	street_numbers_urls = get_entities_urls(streets_urls,
											session,
											headers,
											limit=MAX_STREET_NUMBERS_PER_STREET)

	logging.info('Total street numbers retrieved: %s' % len(street_numbers_urls))

	## We reached the last level of crawling, so we start the scraping now
	logging.info("Now getting each home's full info...")
	get_final_data(street_numbers_urls, session, headers)
	
	return


def get_url_content(session=None, url=None, headers=None, n_retries=3):
	'''
	Method that performs the requests to the URL, handling 403 or similiar
	errors by performing an exponential backoff.
	
	:param session: Python Requests Session object
	:param url: target URL to make the request to
	:param headers: headers to include in the request
	:param n_retries: number of retries for the exponential backoff
	:return: content of the URL when status code is 200
	'''
	
	for n in range(0, n_retries + 1):

		# Call the specific URL
		r = session.get(url, headers=headers)

		# Check that the response is okay. 
		if r.ok:
			return r.text

		# If not, the bot detection got us
		else:
			if n < n_retries:
				logging.warning('Error retrieving %s: %s %s' % 
							(r.url, r.status_code, r.reason))

				logging.warning('Retry %d in %d seconds...' % (n + 1, 4 ** (n+1)))

				time.sleep((4 ** (n+1)) + random.random())

	logging.error('Error getting the URL content. The bot detection got us.')
	return None


def get_provinces_urls(content, limit=None):
	

	# List that will contain the provinces URLs found
	provinces_urls = []

	# Simple counter to avoid calling len() on each loop
	province_counter = 0

	# Soupify the retrieved response
	soup = BeautifulSoup(content, "html.parser")

	# Find the '.links-block' DIV (there should be just one)
	div = soup.find('div', attrs={'class': 'links-block'})

	# Get all the links within that DIV and add it to the list
	links = div.find_all('a')

	for a in links:
	    provinces_urls.append(a['href'])
	    province_counter +=1

	    if limit and province_counter == limit:
	    	break

	return provinces_urls


def get_entities_urls(list_of_urls, session, headers, limit=None):
	'''
	As the cities, streets and street numbers pages have a very similiar
	HTML structure, this method covers all of them in order to get their 
	corresponding "children entities" (e.g.: province -> all its cities).

	:param list_of_urls: list containing the URLs of the provinces, cities,
	or streets that we want to get the sublinks from.
	:param session: Requests API Session object
	:param headers: headers to use on the request
	:param limit: limit of entities retrieved per street
	'''

	# List that will contain all the children entities found
	entities_urls = []

	for url in list_of_urls:

		# Simple counter to control the limit of retrieved entities
		# (to avoid calling len() later on each loop)
		counter = 0

		content = get_url_content(	session=session,
									url=url,
									headers=headers)
		
		if not content:
			# If there was an error getting the content of a URL,
			# at least we return the already retrieved entities
			return entities_urls

		# Soupify the retrieved response
		soup = BeautifulSoup(content, "html.parser")

		# Find the '.links-block' DIV (there should be just one)
		entities_div = soup.find('div', attrs={'class': 'links-block'})

		# Get all the links within that DIV and add it to the list
		entities_div = entities_div.find_all('a')

		for a in entities_div:
			entities_urls.append(a['href'])
			counter += 1

			if limit and limit == counter:
				break

		logging.info('Found %d entities under %s' % (counter, url))

	return entities_urls


def get_final_data(street_numbers_urls, session, headers, limit=None):
	'''
	Method that extracts all the data from a street number page, which
	contains all the info needed.

	:param session: Requests API Session object
	:param headers: headers to use on the request
	:param limit: limit of streets retrieved per street
	'''

	final_result = []

	for sn_url in street_numbers_urls:

		# This list will contain the full data of a home/apartment
		full_home_data = []

		content = get_url_content(	session=session,
									url=sn_url,
									headers=headers)

		# Soupify the retrieved response
		soup = BeautifulSoup(content, "html.parser")

		## BREADCRUMB data
		info_lis = soup.find_all('li', {'class': 'change-display'})

		# We should get 5 <li>s, and we want to discard the first one
		for il in info_lis[1:]:

			full_home_data.append(il.text.strip())

		## GEO data
		try:
			# Get what's inside the BUILDING_AREA var in the html <script>
			building_area_str = re.compile('var\s+BUILDING_AREA\s+=\s+(.*?);') \
								.search(content).group(1)
			
			# The retrieved info could be either 'null' or a dict

			if building_area_str == 'null':
				geo = 'No geolocation'

			else:
				# Evaluate the string (it should a dict with lists of lists)
				building_area = ast.literal_eval(building_area_str)

				coordinates_list = building_area.get('coordinates')[0][0][0]
				# Convert the list to a string
				geo = ','.join(str(c) for c in coordinates_list)

			full_home_data.append(geo)

		except Exception as e:
			logging.error('Error getting the geolocation of %s: %s' %
							(sn_url, e))
			
			full_home_data.append('-')

		
		## If a street number has more than one home/apartment, there is
		## a table containing all of their info.
		## But if there is just a single home, then we get a list (<ul> <li>)

		## <TABLE> data
		info_table = soup.find('table', {'id': 'Vivienda-table'})

		if info_table:

			try:
				t_body = info_table.find('tbody')

				rows = t_body.find_all('tr')

				for row in rows:
					cols = row.find_all('td')
					cols = [entity.text.strip() for entity in cols]
					final_result.append(
						full_home_data + [entity for entity in cols if entity])

			except Exception as e:
				logging.error('Error getting the table info of %s: %s ' %
								(sn_url, e))

		else:
			try:
				info_uls = soup.find_all('ul', {'class': 'table-list'})

				for ul in info_uls:
					lis = ul.find_all('li')
					final_result.append(
						full_home_data + [entity.text.strip() for entity in lis])

			except Exception as e:
				logging.error('Error getting the info of %s: %s ' %
								(sn_url, e))

	pprint(final_result)


if __name__ == '__main__':
	get()