#!/usr/bin/env python
# coding: utf-8

# In[5]:


import gspread
import gspread_dataframe as gsdf
from google.oauth2.service_account import Credentials
import mysql.connector
from datetime import datetime, date
import pandas as pd
import numpy as np
import pytz

#Load credentials from the JSON key file
# credentials = Credentials.from_service_account_file('datafromdatabase-556db248ebb2.json',
#                                                      scopes=['https://www.googleapis.com/auth/spreadsheets'])


# # Load credentials from the JSON key file
credentials = Credentials.from_service_account_file('/home/ubuntu/simran/datafromdatabase-556db248ebb2.json',
                                                    scopes=['https://www.googleapis.com/auth/spreadsheets'])




# Authorize the client
gc = gspread.authorize(credentials)


# Open a Google Sheets spreadsheet by key
spreadsheet_key = '1_RxzGDIG2ZHN-vI6OIoH3qhHyi68pAzFCVdyWPa5FZE'
worksheetEX = gc.open_by_key(spreadsheet_key).worksheet('Form responses 1')


spreadsheet_key = '1_RxzGDIG2ZHN-vI6OIoH3qhHyi68pAzFCVdyWPa5FZE'
worksheet1 = gc.open_by_key(spreadsheet_key).worksheet('Inspections Requests')

df_requests = pd.DataFrame(worksheetEX.get_all_records())
limited_rows_requests = df_requests.head(1000)

Product_Inspection_Raw = limited_rows_requests[limited_rows_requests["Select your requirement:"]=="Product Inspection"]
Product_Inspection_Raw


Product_Inspection_Raw["helper"]= Product_Inspection_Raw["Service Request ID"] + " <> "+ Product_Inspection_Raw["Client / Establishment / Channel Partner Name"]

Product_Inspection_Raw["Date"]=pd.to_datetime(Product_Inspection_Raw["Timestamp"]).dt.date

Product_Inspection_Raw=Product_Inspection_Raw.rename(columns={
                                                            "Select Order and Product Details":"Complaint Helper"
                                                           })



# Replace NaNs and infinite values with a default value (e.g., None)
Product_Inspection_Raw.replace([np.inf, -np.inf], np.nan, inplace=True)
Product_Inspection_Raw.fillna('', inplace=True)


# In[6]:


# Open a Google Sheets spreadsheet by key
spreadsheet_key = '14u2n7D3-ZJ2zKoaOJLTJDphY_0qgwmg7d2VQXFctllk'
worksheetFeed = gc.open_by_key(spreadsheet_key).worksheet('Inspection Feedbacks')

df_requestsFeed = pd.DataFrame(worksheetFeed.get_all_records())
limited_rows_Feedback = df_requestsFeed.head(1000)



limited_rows_Feedback=limited_rows_Feedback.rename(columns={
                                                            "Date":"DateDone",
                                                            "Email ID":"InspectionDoneBy",
                                                           "Date Formatted":"InspectionDoneDate",
                                                            "Stage":"InspectionStage"
                                                           })


# Convert InspectionDoneDate into date format, specifying dayfirst=True
limited_rows_Feedback['InspectionDoneDate'] = pd.to_datetime(limited_rows_Feedback['InspectionDoneDate'], dayfirst=True, errors='coerce')

# Drop rows with NaT (Not a Time) in InspectionDoneDate
limited_rows_Feedback = limited_rows_Feedback.dropna(subset=['InspectionDoneDate'])

idx = limited_rows_Feedback.groupby("Inspection ID")["InspectionDoneDate"].idxmax()
limited_rows_Feedback = limited_rows_Feedback.loc[idx]





# In[7]:


Inspection_Master=pd.merge(Product_Inspection_Raw, 
                           limited_rows_Feedback,
                          left_on= "Service Request ID",
                          right_on="Inspection ID",
                          how="left")

Inspection_Master["InspectionStatus"]=np.where(Inspection_Master["InspectionStage"].isin(["Completed","Cancelled"]),
                                               "Closed","Open"
                                              )


today = pd.to_datetime(datetime.now(pytz.timezone('Asia/Kolkata')).date())


Inspection_Master['InspectionDoneDate'] = pd.to_datetime(Inspection_Master['InspectionDoneDate'])
Inspection_Master['Date'] = pd.to_datetime(Inspection_Master['Date'])
Inspection_Master['Preferred Date 1'] = pd.to_datetime(Inspection_Master['Preferred Date 1'])


Inspection_Master["TAT / Aging"]=np.where(Inspection_Master["InspectionStatus"]=="Closed",
                                          (Inspection_Master["InspectionDoneDate"]-Inspection_Master["Date"]).dt.days,
                                          (today-Inspection_Master["Date"]).dt.days
                                         )


Inspection_Master["TAT / Aging From Preferred Slot 1"]= (today-Inspection_Master['InspectionDoneDate']).dt.days



# Convert float columns to integers where appropriate
Inspection_Master['TAT / Aging'] = Inspection_Master['TAT / Aging'].fillna(0).astype(int)
Inspection_Master["TAT / Aging From Preferred Slot 1"] = Inspection_Master["TAT / Aging From Preferred Slot 1"].fillna(0).astype(int)


# In[ ]:





# In[8]:


columns_to_keep=["helper",
                "Date",
                "Service Request ID",
                "Timestamp",
                "Email address",
                'Select your requirement:',
                'Installation Helper',
                'Complaint Helper',
                'Client / Establishment / Channel Partner Name',
                'Client / Establishment / Channel POC Name',
                'Client / Establishment / Channel POC Phone Number',
                'Address of Requirement',
                'Product Name',
                'Brand',
                'Specifications',
                'Product Condition',
                'Preferred Date 1',
                'Preferred Time Slot 1',
                'Preferred Date 2',
                'Preferred Time Slot 2',
                'Preferred Date 3',
                'Requested Time Slot 3',
                'Additional Remarks',
                'InspectionStage',
                 'InspectionStatus',
                 'InspectionDoneDate',
                 'TAT / Aging',
                 'InspectionDoneBy',
                 "TAT / Aging From Preferred Slot 1"
                 ]


new_df = Inspection_Master[columns_to_keep].copy()

# Convert datetime columns to strings for serialization
for col in new_df.select_dtypes(include=['datetime64[ns]']).columns:
    new_df[col] = new_df[col].dt.strftime('%Y-%m-%d')

new_df




#Clear Worksheet sales dashboard
range_to_clear_sheet1 = 'A:AB'
worksheet1.batch_clear([range_to_clear_sheet1])

#updating worksheet with data
gsdf.set_with_dataframe(worksheet1, new_df)


# In[ ]:






