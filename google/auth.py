import argparse, os
from apiclient.discovery import build
import httplib2
from oauth2client import client, tools
from oauth2client.file import Storage
credential = os.path.relpath(__file__).replace('auth.py', 'credential_file')

def get_service(api_name, api_version):
  # Parse command-line arguments.
  parser = argparse.ArgumentParser(
      formatter_class=argparse.RawDescriptionHelpFormatter,
      parents=[tools.argparser])
  flags = parser.parse_args([])

  credentials = get_credential()

  http_auth = credentials.authorize(http=httplib2.Http())
  # Build the service object.
  service = build(api_name, api_version, http=http_auth)

  return service

def get_credential():
  client_secrets_path = os.path.relpath(__file__).replace('auth.py', 'client_secrets.json')
  storage = Storage(credential)
  credentials = storage.get()
  if not credentials:
    # Set up a Flow object to be used if we need to authenticate.
    flow = client.flow_from_clientsecrets(
      client_secrets_path, scope=['https://www.googleapis.com/auth/analytics.readonly'],
      redirect_uri="urn:ietf:wg:oauth:2.0:oob",
      message=tools.message_if_missing(client_secrets_path))
    auth_uri = flow.step1_get_authorize_url()
    print("Get the code in:", auth_uri)
    auth_code = input('Code: ').strip()
    credentials = flow.step2_exchange(auth_code)
  storage.put(credentials)
  return credentials
