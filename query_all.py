#!/usr/bin/env python
# coding: utf-8

# In[3]:


import gspread
import gspread_dataframe as gsdf
from google.oauth2.service_account import Credentials
import mysql.connector
from datetime import datetime, date
import pandas as pd
import numpy as np

# #Load credentials from the JSON key file
# credentials = Credentials.from_service_account_file('datafromdatabase-556db248ebb2.json',
#                                                     scopes=['https://www.googleapis.com/auth/spreadsheets'])


# Load credentials from the JSON key file
credentials = Credentials.from_service_account_file('/home/ubuntu/simran/datafromdatabase-556db248ebb2.json',
                                                    scopes=['https://www.googleapis.com/auth/spreadsheets'])


# Authorize the client
gc = gspread.authorize(credentials)

# Open a Google Sheets spreadsheet by key
spreadsheet_key = '1GM2F7E1TgMMUTDM18-MRF3He0HjojTPs9DKlTdxI7bk'
worksheet1 = gc.open_by_key(spreadsheet_key).worksheet('Master')
worksheet2 = gc.open_by_key(spreadsheet_key).worksheet('Quotation Details')
worksheet3 = gc.open_by_key(spreadsheet_key).worksheet('query_all')
worksheet4 = gc.open_by_key(spreadsheet_key).worksheet('Category Action')
worksheet5 = gc.open_by_key(spreadsheet_key).worksheet('Quotation Request DB')



# Connect to MySQL database
my_conn = mysql.connector.connect(
                                    host='3.109.191.111',
                                    user='analytics_user',
                                    password='Analytics@sql@32',
                                    db='lsq_db',
                                  )

print("Connection Query Excecuted", datetime.now())


# In[4]:


# Fetch data from the Google Sheets directly into DataFrames
oldquery = pd.DataFrame(worksheet1.get_all_records())
oldquery = oldquery.head(19500)


# Fetch data from the Google Sheets directly into DataFrames
detail = pd.DataFrame(worksheet2.get_all_records())
detail = detail.head(5000)

#Creation of main sheet
main = pd.merge(oldquery, detail, on='queryId', how='right')



columns_to_keep=['timestamp',
                'queryId',
                'email',
                'productName',
                'specification',
                'brand',
                'quantity',
                'establishmentType',
                'bedNo',
                'establishmentname',
                'pocName',
                'phonePoc',
                'urgentQuery',
                'reasonUrgent',
                'clientName',
                'city',
                'remarks',
                'leadSource',
                'warmth',
                'opportunityId',
                'actionTaken']

old_Query = main[columns_to_keep].copy()

print("Old data created", datetime.now())


# In[5]:


my_cursor = my_conn.cursor()
my_cursor.execute(''' SELECT timestamp,
                             queryId,
                             email,
                             productName,
                             specification,
                             brand,
                             quantity,
                             establishmentType,
                             bedNo,
                             establishmentname,
                             pocName,
                             phonePoc,
                             urgentQuery,
                             reasonUrgent,
                             clientName,
                             city,
                             remarks,
                             leadSource,
                             warmth,
                             opportunityId,
                             actionTaken
                             
                    FROM quotation_db.quotation_action ''')

my_result = my_cursor.fetchall()

print("Order SQL Query Excecuted", datetime.now())

# Convert MySQL result to a DataFrame
header = [i[0] for i in my_cursor.description]
New_Query = pd.DataFrame(my_result, columns=header)

# Append Old & New Query               
Query = pd.concat([old_Query, New_Query], ignore_index=True)


conditions = [
    Query['actionTaken'].isin(['Quotation Requested', 'Assigned to Pricing', 'Sent to Pricing','Re-opened by Sales']),
    Query['actionTaken'].isin(['Assigned to Category', 'Re-opened to Category'])
]

choices = ['Pricing', 'Category']

# Use np.select to create the new column based on conditions
Query['pending_on'] = np.select(conditions, choices, default=None)

Query = Query.drop_duplicates(subset=['timestamp', 'queryId', 'actionTaken'])
Query['timestamp'] = pd.to_datetime(Query['timestamp'])

Query= Query.sort_values(by=['queryId', 'timestamp'], ascending=[True, False])


print("New data created", datetime.now())

# print(Query)


# In[6]:


TATQuery = Query.copy()

# Creating next action time
TATQuery['next_action'] = TATQuery.groupby('queryId')['timestamp'].shift(1)

# Convert next_action column to datetime
TATQuery['next_action'] = pd.to_datetime(TATQuery['next_action'])

# Current timestamp with timezone conversion
current_timestamp = pd.Timestamp.now(tz='Asia/Kolkata').tz_localize(None)

# Calculate TAT in minutes
TATQuery['TAT'] = (TATQuery['next_action'].fillna(current_timestamp) - TATQuery['timestamp']).dt.total_seconds() / 3600



# Calculate pricing_TAT, category_TAT, and overall_TAT
TATQuery['pricing_TAT'] = TATQuery.apply(lambda row: row['TAT'] / 60 if row['pending_on'] == 'Pricing' else 0, axis=1)
TATQuery['category_TAT'] = TATQuery.apply(lambda row: row['TAT'] / 60 if row['pending_on'] == 'Category' else 0, axis=1)
TATQuery['overall_TAT'] = TATQuery['TAT'] / 60

# Group by queryId and calculate sums
TATQuery = TATQuery.groupby('queryId').agg({
    'pricing_TAT': 'sum',
    'category_TAT': 'sum',
    'overall_TAT': 'sum'
}).reset_index()




# # Display the DataFrame
# print(TATQuery)

# filtered_df = TATQuery[TATQuery['queryId'] == 'Q04924']

# # Print the filtered DataFrame
# print(filtered_df)


# In[7]:


#Creating unique query with latest time

Query_sorted =Query.copy()

Query_sorted['row_number'] = Query_sorted.groupby('queryId').cumcount() + 1

UniqueQuery = Query_sorted[Query_sorted['row_number'] == 1]

UniqueQuery = UniqueQuery.drop(columns=['row_number'])


# creation of current status
UniqueQuery['current_status'] = UniqueQuery['actionTaken'].fillna('Quotation Requested')

#Creating Open status
UniqueQuery['open_status'] = UniqueQuery['current_status'].apply(
    lambda x: 'Closed' if x in ['Cannot Procure', 'Quotation Generated'] else 'Open'
)


#Creation of Last action creation aging

UniqueQuery['timestamp'] = pd.to_datetime(UniqueQuery['timestamp'])

# Get the current timestamp in the specified timezone and remove timezone info
current_time_ist = pd.Timestamp.now(tz='Asia/Kolkata').tz_localize(None)

# Calculate the difference in hours
UniqueQuery['last_action_aging'] = (current_time_ist - UniqueQuery['timestamp']).dt.total_seconds()




# In[8]:


MyQuery = pd.merge(UniqueQuery, TATQuery, on='queryId', how='left')
MyQuery = MyQuery.sort_values(by='timestamp')

MyQuery = MyQuery[MyQuery['queryId'].str[:2] != 'QG']

# print(MyQuery)


# In[9]:


#Clear Worksheet
range_to_clear_sheet = 'A:AB'
worksheet3.batch_clear([range_to_clear_sheet])
#updating worksheet with data
gsdf.set_with_dataframe(worksheet3, MyQuery )


# In[10]:


my_cursor = my_conn.cursor()
my_cursor.execute(''' SELECT  date(updatedAt),
                              queryId ,
                              email,
                              productName,
                              specification,
                              brand,
                              quantity,
                              establishmentType,
                              bedNo,
                              establishmentname,
                              pocName,
                             phonePoc,
                             urgentQuery,
                             reasonUrgent,
                             clientName,
                             city,
                             remarks,
                             leadSource,
                             warmth,
                             opportunityId,
                             actionTaken                         
                      FROM quotation_db.quotation_action
                      where actionTaken='Sent to Pricing'
                      
                     ''')

my_result1 = my_cursor.fetchall()

# Convert MySQL result to a DataFrame
header = [i[0] for i in my_cursor.description]
Cat_Query = pd.DataFrame(my_result1, columns=header)

columns_to_select = ['pending_on', 'current_status', 'open_status', 'last_action_aging', 
                     'pricing_TAT', 'category_TAT', 'overall_TAT']

# Merge Cat_Query with selected columns from MyQuery based on 'queryId'
Cat_Query = Cat_Query.merge(MyQuery[['queryId'] + columns_to_select], on='queryId', how='left')



#Clear Worksheet
range_to_clear_sheet = 'A:AB'
worksheet4.batch_clear([range_to_clear_sheet])
#updating worksheet with data
gsdf.set_with_dataframe(worksheet4, Cat_Query )


# In[11]:


#Clear Worksheet
range_to_clear_sheet = 'A:B'
worksheet4.batch_clear([range_to_clear_sheet])
#updating worksheet with data
gsdf.set_with_dataframe(worksheet4, Cat_Query )

my_cursor = my_conn.cursor()
my_cursor.execute(''' SELECT date(timestamp),
                       queryId,
                       email
                             
                    FROM quotation_db.quotation_action a
                    where actionTaken='Quotation Requested'                      
                     ''')

my_resultX = my_cursor.fetchall()

# Convert MySQL result to a DataFrame
header = [i[0] for i in my_cursor.description]
Raise_Query = pd.DataFrame(my_resultX, columns=header)


#Clear Worksheet
range_to_clear_sheet = 'A:B'
worksheet5.batch_clear([range_to_clear_sheet])
#updating worksheet with data
gsdf.set_with_dataframe(worksheet5, Raise_Query)


# In[ ]:




