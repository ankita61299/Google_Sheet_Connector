CREATE OR REPLACE NETWORK RULE google_apis_network_rule
MODE = EGRESS
TYPE = HOST_PORT
VALUE_LIST = ('sheets.googleapis.com');

CREATE OR REPLACE SECURITY INTEGRATION google_translate_oauth
TYPE = API_AUTHENTICATION
AUTH_TYPE = OAUTH2
OAUTH_CLIENT_ID = 'XXXXXXXXXXXXX'
OAUTH_CLIENT_SECRET = 'XXXXXXXXXXXXXX'
OAUTH_TOKEN_ENDPOINT = 'https://oauth2.googleapis.com/token'
OAUTH_AUTHORIZATION_ENDPOINT =
'https://accounts.google.com/o/oauth2/auth'
OAUTH_ALLOWED_SCOPES =
('https://www.googleapis.com/auth/spreadsheets.readonly')
ENABLED = TRUE;

CREATE OR REPLACE SECRET oauth_token
TYPE = oauth2
API_AUTHENTICATION = google_translate_oauth
OAUTH_REFRESH_TOKEN = 'XXXXXXXXXX';

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION
google_apis_access_integration
ALLOWED_NETWORK_RULES = (google_apis_network_rule)
ALLOWED_AUTHENTICATION_SECRETS = (oauth_token)
ENABLED = true;
