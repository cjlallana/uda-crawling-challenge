'''
Created on 2020/01/28

:author: Carlos Lallana
'''

# Python standard library
import logging, time, random
from pprint import pprint

# Third-party libraries
import requests
from bs4 import BeautifulSoup

# Global variables (actually used as constants)
MAX_PROVINCES = 2
MAX_CITIES_PER_PROVINCE = 2
MAX_STREETS_PER_CITY = 2
MAX_STREET_NUMBERS_PER_STREET = 10


def get():

	BASE_URL = 'https://www.idealista.com/'

	# Create a Session object, wich will persist certain headers and parameters
	# across requests. It will have all the methods of the main Requests API.
	s = requests.Session()

	# Prepare a minimal header to avoid bot detection
	headers = {
	    'authority': 'www.idealista.com',
	    'cache-control': 'no-cache',
	    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36'
	}

	# Call the main URL
	content = get_url_content(	session=s,
								url=BASE_URL + 'maps/',
								headers=headers)

	if not content:
		logging.error('Error retrieving the main URL')
		return -1

	# Get a list of each province URL
	provinces_urls = get_provinces_urls(content, limit=MAX_PROVINCES)
	print('Retrieved %d provinces URLs' % len(provinces_urls))

	# Get a list of each province city URL
	cities_urls = get_cities_urls(provinces_urls, s, headers, limit=MAX_CITIES_PER_PROVINCE)
	print('Total cities retrieved: %s' % len(cities_urls))

	streets_urls = get_streets_urls(cities_urls, s, headers, limit=MAX_STREETS_PER_CITY)
	print('Total streets retrieved: %s' % len(streets_urls))

	street_numbers_urls = get_entities_urls(streets_urls, s, headers, limit=MAX_STREET_NUMBERS_PER_STREET)
	print('Total street numbers retrieved: %s' % len(street_numbers_urls))
	pprint(street_numbers_urls)
	return


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


def get_cities_urls(provinces_urls, session, headers, limit=None):
	'''
	:param provinces_url: list containing the URLs of each province
	:param limit: limit of cities retrieved per province
	'''

	# List that will contain all the cities found
	cities_urls = []

	# For each province URL, get their cities
	for province_url in provinces_urls:

		# Simple counter to avoid calling len() later on each loop
		city_counter = 0

		content = get_url_content(	session=session,
									url=province_url,
									headers=headers)
		
		if not content:
			return cities_urls

		# Soupify the retrieved response
		province_soup = BeautifulSoup(content, "html.parser")

		# Find the '.links-block' DIV (there should be just one)
		cities_div = province_soup.find('div', attrs={'class': 'links-block'})

		# Get all the links within that DIV and add it to the list
		cities_links = cities_div.find_all('a')

		for a in cities_links:
			cities_urls.append(a['href'])
			city_counter +=1

			if limit and limit == city_counter:
				break

		print('Retrieved %d cities under %s' % (city_counter, province_url))

	return cities_urls


def get_streets_urls(cities_urls, session, headers, limit=None):
	'''
	:param cities_urls: list containing the URLs of each city
	:param limit: limit of streets retrieved per street
	'''

	# List that will contain all the streets found
	streets_urls = []

	for city_url in cities_urls:

		# Simple counter to avoid calling len() later on each loop
		streets_counter = 0

		content = get_url_content(	session=session,
									url=city_url,
									headers=headers)
		
		if not content:
			return streets_urls

		# Soupify the retrieved response
		city_soup = BeautifulSoup(content, "html.parser")

		# Find the '.links-block' DIV (there should be just one)
		streets_div = city_soup.find('div', attrs={'class': 'links-block'})

		# Get all the links within that DIV and add it to the list
		streets_links = streets_div.find_all('a')

		for a in streets_links:
			streets_urls.append(a['href'])
			streets_counter += 1

			if limit and limit == streets_counter:
				break

		print('Found %d streets under %s' % (streets_counter, city_url))

	return streets_urls


def get_entities_urls(list_of_urls, session, headers, limit=None):
	'''
	As the cities, streets and street numbers pages have a very similiar
	HTML structure, this method covers all of them in order to get their 
	corresponding "children".

	:param list_of_urls: list containing the URLs of the provinces, cities,
	or streets that we want to get the sublinks from.
	:param limit: limit of streets retrieved per street
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

		print('Found %d entities under %s' % (counter, url))

	return entities_urls


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



if __name__ == '__main__':
	get()