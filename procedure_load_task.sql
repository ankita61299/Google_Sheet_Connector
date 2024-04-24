CREATE OR REPLACE PROCEDURE
LOAD_DATA_TASK("TABLE_NAME_"
VARCHAR(16777216), "SPREADSHEET_KEY" VARCHAR(16777216), "RANGE_"
VARCHAR(16777216), "FLAG" VARCHAR(16777216))
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
import snowflake.snowpark as snowpark
import pandas as pd
from datetime import datetime
def main(session:
snowpark.Session,table_name_,spreadsheet_key,range_,flag):
query = f"select
google_python(''{spreadsheet_key}'',''{range_}'')"
m=session.sql(query).collect()[0]
xx = eval(m[0])
y=xx[''values'']
df=pd.DataFrame(y[1:],columns=y[0])

try:
if flag==''1'':
snowpark_df = session.write_pandas(df, table_name_,

auto_create_table=True,overwrite=True);

else:
snowpark_df = session.write_pandas(df, table_name_,

auto_create_table=True,overwrite=False);

session.sql("CALL

SYSTEM$SEND_EMAIL(''my_email_int'',''xxxxxx'',''Connector is
active'',''Connector got sync '')")
except:

session.sql("CALL

SYSTEM$SEND_EMAIL(''my_email_int'',''xxxxxxxx'',''Connector is
failed'',''Please check the connector current sync of connector
got failed '')")
return ''TRUE''
