#  Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.

# Import python packages
import datetime
import json
import streamlit as st
from PIL import Image
import pandas as pd
import base64
from snowflake.snowpark.context import get_active_session


st.set_page_config(layout='wide')
button_color = "#1f77b4"
user = st.experimental_user["user_name"]

session = get_active_session()


def get_image_str(image_name):
    mime_type = image_name.split('.')[-1:][0].lower()
    with open(image_name, "rb") as f:
        content_bytes = f.read()
    content_b64encoded = base64.b64encode(content_bytes).decode()
    image_string = f'data:image/{mime_type};base64,{content_b64encoded}'
    return image_string
    

@st.cache_resource
def setup_form(para):
    st.subheader("Setup form")
    #expander = st.expander("See explanation")
    
  
    st.text("To connect Google Sheets to Snowhouse, you must have Read / Write access to a Google Sheet")
    st.info(" :blue[Step 1  : Select a range]")
    st.text("Open your Google Sheet and select the range that you want added to your destination. \nYou can change your selected range later if needed  To select a range, \nyou can do either of the following:"
            "manually select the range as shown below, or \nSelect just the columns (for example, Sheet1!A:D) .If you select just the columns,\n App only creates rows for up to the final row that has values in your sheet\n(for example Sheet1!A1:D6)") 
   
    
    # Write image data in Snowflake table
    # Create a Snowflake stage to store the function
   
    image1 = Image.open('namerange.png')
    st.image(image1,width=500)
  

    st.info(":blue[Step 2  : Create named range]")

    st.text(" 1. In your Google Sheet, go to Data > Named ranges.")
    image = get_image_str("selectrange.png")
    st.image(image ,width=500)

    st.text("In the Named ranges menu, enter a name for your new range and click Done.")
    image4 = Image.open('range.png')
    st.image(image4,width=500)

    

    st.info(":blue[Step 3  : Share Google Sheet with streamlit app]")
    
    st.text("In the top right corner of your Google Sheet, click Share and Enter the email address  xxxxxxxxx and give it View permissions.")
    image3 = Image.open('service_aacount.png')
    st.image(image3,width=500)


    st.info(":blue[Step 4 : Find your spreadsheet URL]")
    st.text("Copy the spreadsheet's URL from your browser's address bar")
    image2 = Image.open('sheet_key.png')
    st.image(image2,width=500)







def gsheet_write(db_name_,schema_name_,table_name_,spreadsheet_key_,named_range_):

    
    

    table_name_= '\''+table_name_ +'\''
    db_name_= '\''+ db_name_ + '\''
    schema_name_  = '\''+schema_name_ +'\''
    named_range_='\''+named_range_+'\''
    spreadsheet_key_='\''+ spreadsheet_key_+'\''

    session = get_active_session()
    
    put_data= f'''
    call Load_data({table_name_},{spreadsheet_key_},{named_range_});
    '''
    data1 = session.sql(put_data).collect()
    
    insert_data= f'''
    insert into <db>.<schmema>.<logging_table>
    select
    current_date(),
    {db_name_},
    {schema_name_},
    {table_name_},
    {spreadsheet_key_},
    {named_range_}
    '''

    #st.write(put_data)
    

    st.success('Data Loaded Successfully', icon="✅")
    data2 = session.sql(insert_data).collect()
    

    

   
  



def ingest():
    
    col1, col2, col3 = st.columns(3)
    with col2:
      st.header(" ❄️ :blue[Google Sheet Connector]")
    #image = Image.open('credentials/gs_snf.png',width=1,)

    #st.image(image, caption='Sunrise by the mountains')
    
    
    session = get_active_session()
    tab1, tab2 ,tab3 = st.tabs(["Create connector", "Edit connector","Connectors Status"])

   
    with tab1:
        col1, col2, =st.columns(2)

        with col1: 
            
            expander1 = st.expander("Source")
            
            if "Sheet" not in st.session_state:
                    st.session_state.Sheet = None


            sheet_url = expander1.text_input(
                " Spreadsheet URL  :red[*]",key='disabled3'
               
            )

            start_pos = sheet_url.find("/d/") + len("/d/")
            end_pos = sheet_url.find("/edit")
            spreadsheet_key = sheet_url[start_pos:end_pos]
            named_range=""
            spreadsheet_key__='\''+ spreadsheet_key+'\''

            enable_second_input = len(sheet_url) > 0
            st.session_state.Sheet=enable_second_input

            if 'database_' not in st.session_state:
                    st.session_state.database_ = ""

            if enable_second_input:
            #if st.button("Find Sheet"):

                get_named_range =f'''select google_python_named_range({spreadsheet_key__})
                '''
               
                data3 = session.sql(get_named_range).collect()
                
                #rs_range_name=query_snowflake2(get_named_range)
                list_r=[]
                list_r=data3
                
                for data in data3:
                    deviceId = str(data[0])
                
                variant_list = deviceId.split(',')
                
                
                #st.write(type(list_range))
                
               

                
                named_range = expander1.selectbox(
                'Range Name',
                (variant_list))

           


            

            expander2 = st.expander("❄️ Destination")
            col3,col4 = expander2.columns(2)
            
            Get_db_list = f'''select IT 
                '''
            
            #data4 = session.sql(Get_db_list).collect()
            

            
            #result2=query_snowflake( Get_db_list)


            with col3:

                database_name = expander2.selectbox(
                    'Database Name :red[*]',
                    ('db_name',' '),key='disabled4')
                
                if database_name != st.session_state.database_:
                    st.session_state.database_ = database_name

            




            #db_name = st.text_input(
                #label=' Enter Database Name :red[*]' , key="db1"

            #)
            

            
            Get_schema_list = f'''select schema_name from {database_name}.INFORMATION_SCHEMA.SCHEMATA                              
                '''
            
            if 'schema' not in st.session_state:
                    st.session_state.schema = ""

            #schema_list = query_snowflake(Get_schema_list)
            with col4:
                schema_name = expander2.selectbox(
                    'Schema Name:red[*]',
                    ('schema_name',' '),key='disabled1')
                
                if schema_name != st.session_state.schema:
                    st.session_state.schema = database_name

            #schema_name = st.text_input(
                #" Enter  Schema Name  :red[*] ", key="schema"
                
            #)

            if 'table' not in st.session_state:
                    st.session_state.table = ""

            table_name = expander2.text_input(
                "Table Name  :red[*] ",key='disabled2'
                
            )

            if table_name != st.session_state.table:
                    st.session_state.table = database_name
            
            st.session_state.query_result="true"
            
            #print(spreadsheet_key.split("d/",1)[1])
            

            

            
            if "Save" not in st.session_state:
                    st.session_state.Save = None
            if "set" not in st.session_state:
                    st.session_state.set = None 
           

           
                
                
                #st.write(type(list_range))
            
                

            st.session_state.Save=expander2.button("Load Data",type = 'primary')
            if st.session_state.Save:

                gsheet_write(database_name,schema_name,table_name,spreadsheet_key,named_range)
                #expander = st.expander("See sample loaded data")
                select_sample=f'''select * from {table_name}
            '''
                #cur.executes(select_sample)
                #df_sample = cur.fetch_pandas_all()
                #expander.dataframe(df)
                
            expander3 = st.expander("⏰ Schedule")
        
            table_name_t='\''+ table_name+'\''
            
            
            #options=""

            if "option" not in st.session_state:
                    st.session_state.option = None  

            options = expander3.selectbox(
            'Schedule At ',
            ('15 minutes', '30 minutes', '6 hours','12 hours', '24 hours'))
                
            
            
            if options=='15 minutes':
                time='15 *'

            if options=='30 minutes':
                time='30 *'

            if options=='6 hours':
                time='* 6' 

            if options=='12 hours':
                time='* 12' 


            if options=='24 hours':
                time='1 *'   

            if options != st.session_state.option:
                    st.session_state.option = options 
            
            
            
            Flag=0;
            col3, col4, =st.columns(2)

            if "radio" not in st.session_state:
                    st.session_state.radio = None   

            load  = expander3.radio(
                "Data Load Type",
                ('Full Refresh', 'Recurring Load'))
            
            if load == 'Full Refresh':
                Flag='1'

            if load == 'Recurring Load':
                Flag='2'

            if load != st.session_state.radio:
                    st.session_state.radio = load

            


            spreadsheet_key='\''+ spreadsheet_key+'\''
            named_range='\''+named_range+'\''

            create_task =f'''
            create or replace task {table_name}_TASK
            warehouse=gsheet_connector_wh
            schedule='USING CRON  {time} * * * UTC'
            as call Load_data_task({table_name_t},{spreadsheet_key},{named_range},{Flag});
            '''  
            
            alter_task=f'''
            alter task {table_name}_TASK resume
            '''

            db = f'''
            use database {database_name}
            '''
            
            st.session_state.set=expander3.button('Set',type = 'primary')
            if st.session_state.set:
                session.sql("use database IT")
                session.sql("use database GSHEET_CONNECTOR")
                
            
                #cur.execute(db)
                #st.write(create_task)
                session.sql(create_task).collect()
                session.sql(alter_task).collect()
                
                st.success("Connector scheduled successfully")
 
            

        with col2:
            setup_form('1')
            #st.write("setup guide coming")
          
            
    

def write_to_gsheet(snowflake_query_,Spreadsheet_key,Sheet_name):
    
    snowflake_query__=f'''select array_agg(array_construct(*)) as p from ({snowflake_query_})
        '''
    data2 = session.sql(snowflake_query__).collect()
    
    df = pd.DataFrame(data2)
    for row in data2:
        column1_value = row[0]

    
    snowflake_query_='\''+snowflake_query_+'\''
    #df = pd.DataFrame(column1_value)
    df.replace(to_replace='undefined', value=None, inplace=True)
    #st.write(df)
    df.replace(to_replace='"', value="'", inplace=True)
    column1_value=column1_value.replace('"', "'")
    c_values = df.values.tolist()
    
    #data2=data2.replace('"', "'")
    #df = data2.fetch_pandas_all()
    #st.write(column1_value)
    

    put_data=f'''call RETL_LOAD_DATA({snowflake_query_},{Spreadsheet_key},{Sheet_name}) 
    '''
    #st.write(put_data)
    data2 = session.sql(put_data).collect()
  

    return c_values;

    # Convert result to a list of lists
   



with st.sidebar:
    tabs= st.radio(label = 'Select Options', options = ('Googlesheet Ingestion', 'rETL To googlesheet'))

if tabs == 'Googlesheet Ingestion':
     ingest()

