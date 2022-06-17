# pip3 install PyGithub requests
# pip install google-auth

import requests
from pprint import pprint
from github import Github
import google.auth
import google.oauth2.credentials
from google.auth.transport.requests import AuthorizedSession

credentials, project = google.auth.default()
# credentials = google.oauth2.credentials.Credentials(
#     'access_token')
authed_session = AuthorizedSession(credentials)

# url to request
url = f"https://api.github.com/repos/soundcommerce/mono"
# make the request and return the json
user_data = authed_session.get(url).json()
# pretty print JSON data
pprint(user_data)