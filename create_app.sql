CREATE or replace STREAMLIT db_name.schema_name.Gsheet_app
FROM '@internal_stage'
query_warehouse=warehouse
MAIN_FILE =
'db_name.schema_name.internal_stage/gsheet_app.py';
