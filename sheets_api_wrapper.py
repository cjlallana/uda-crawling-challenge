'''
:author: Carlos Lallana

:since: 2019/05/07
'''
import sys, logging, random, time, json

from google.oauth2 import service_account

from googleapiclient.discovery import build
#from oauth2client.service_account import ServiceAccountCredentials


# If we change the scopes, change them in the admin console and tutorial!
SCOPES = ['https://www.googleapis.com/auth/drive']


def open_local_keyfile(keyfile_path):
	'''Opens a keyfile stored locally, and returns the content as a dictionary
	
	:param keyfile_path: local path were the file is stored
	:rtype: dictionary
	'''
	try:
		return json.loads(open(keyfile_path).read())

	except Exception as e:
		logging.error('Error opening the keyfile: %s' % e)
		return None


def evaluate_keyfile(keyfile_content):
	'''If the keyfile is stored in Datastore as an ndb.JsonProperty, the 
	content is a dict stored as a unicode string, This method evaluates
	that content and returns a proper Python dict.
	
	:param keyfile_content: unicode string containing a dict
	:return: evaluated dict or None if error
	'''
	try:
		# As the keyfile is a string dict, we safely evaluate it to a real dict 
		import ast
		return ast.literal_eval(keyfile_content)
		
	except Exception as e:
		logging.error('Error evaluating Keyfile content: %e' % e)
		return None
	

def get_credentials_object(keyfile, user_email=None):
	'''Creates and returns the Credentials object that holds refresh and access 
	tokens that authorize access to a single user's data.
	
	:param keyfile: generated JSON containing the service account details
	:param user_email: the email of the user.
	:return: the constructed credentials.
	:rtype: google.auth.service_account.Credentials
	'''
	#credentials = service_account.Credentials.from_service_account_file(keyfile)
	credentials = service_account.Credentials.from_service_account_info(keyfile)
	
	# Scopes and subject can also be modified separately 
	credentials = credentials.with_scopes(SCOPES)
	if user_email:
		credentials = credentials.with_subject(user_email)

	return credentials
	
	
def authorize_credentials(credentials, n_retries=3):
	'''Builds and returns an authorized Sheets service object. 
	Performs an exponential backoff retry strategy.
	
	:param credentials: ServiceAccountCredentials (Credentials object)
	:return: Sheets API service instance
	'''
	for n in range(1, n_retries + 1):
		try:
			# Explicitly set the timeout, as it defaults to 5s
			#http = credentials.authorize(httplib2.Http(timeout=30))
			
			return build('sheets', 'v4', 
						credentials=credentials,
						cache_discovery=False)	# file_cache is unavailable 
												# when using google-auth, so
												# set it false to avoid warnings
			
		except Exception as e:
			logging.warning('Line %d: %s' % (sys.exc_info()[-1].tb_lineno, e))
	
		logging.warning('Retry %d (authorize_credentials) ...' % n)
		time.sleep((2 ** n) + random.random())

	logging.error('Error trying to authorize credentials')
	return None


def get_spreadsheet(service, spreadsheet_id, ranges=[],
					include_grid_data=False, n_retries=3):
	'''Returns the spreadsheet related to the given ID.
	
	:param service: Sheets API service instance
	:param spreadsheet_id: 
	:param ranges: the ranges to retrieve from the spreadsheet. All by default.
	:param includeGridData: optional grid data to retrieve only subsets of the 
	spreadsheet
	:return: retrieved Spreadsheet
	'''
	
	request = service.spreadsheets().get(spreadsheetId=spreadsheet_id, 
										ranges=ranges, 
										includeGridData=include_grid_data)
	
	for n in range(1, n_retries + 1):
		try:
			response = request.execute()
											
			return response
			
		except Exception as e:
			logging.warning('Line %d: %s' % (sys.exc_info()[-1].tb_lineno, e))
	
		logging.warning('Retry %d (get_spreadsheet) ...' % n)
		time.sleep((2 ** n) + random.random())

	logging.error('Error getting Spreadsheet %s' % spreadsheet_id)


def append_to_spreadsheet(	service, spreadsheet_id, values, range_='A1:B2', 
							n_retries=3):
	'''Appends data to the end of a given Spreadsheet, assuming that there are
	no blank rows.
	
	:param service: Sheets API service instance
	:param spreadsheet_id:
	:param values: list of lists, containing each row's data
	:param range_: range to look up for an empty table and start writing there
	'''
	body = {
		'values': values
	}
	
	try:
		request = service.spreadsheets().values().append(spreadsheetId=spreadsheet_id, 
												range=range_,
												valueInputOption="USER_ENTERED", 
												body=body)
	
	except Exception as e:
		logging.error('Line %d: %s' % (sys.exc_info()[-1].tb_lineno, e))
		logging.error('Error appending data to Spreadsheet: %s' % e)
		return None
	
		
	for n in range(1, n_retries + 1):
		try:

			response = request.execute()
											
			return response
			
		except Exception as e:
			logging.warning('Line %d: %s' % (sys.exc_info()[-1].tb_lineno, e))
	
		logging.warning('Retry %d (append_to_spreadsheet) ...' % n)
		time.sleep((2 ** n) + random.random())

	logging.error('Error appending data to Spreadsheet')