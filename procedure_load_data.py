CREATE OR REPLACE FUNCTION
GOOGLE_PYTHON_NAMED_RANGE("SPREEDSHEET_ID"
VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('requests','snowflake-snowpark-python','pandas')
HANDLER = 'get_data'
EXTERNAL_ACCESS_INTEGRATIONS = (xxxxxxx)
SECRETS = ('cred'=xxxxxxxxxx)
AS '
import snowflake.snowpark as
snowpark
import _snowflake
import requests
import json
import pandas
import numpy

def get_data(spreedsheet_id):
token = _snowflake.get_oauth_access_token(''cred'')
url = "https://sheets.googleapis.com/v4/spreadsheets/"+
spreedsheet_id
response = requests.get(url, headers = {"Authorization": "Bearer
" + token})

data = response.json()
named_ranges = data.get(''namedRanges'', [])
s=""

if named_ranges:
print("Named Ranges:")
for named_range in named_ranges:
#range.append(named_range[''name''])
s=s+'',''+
named_range[''name'']

else:
print("No named ranges
found.")
return s[1:];

';
