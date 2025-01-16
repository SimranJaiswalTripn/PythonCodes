#!/usr/bin/env python
# coding: utf-8

# In[208]:


import gspread
import gspread_dataframe as gsdf
from google.oauth2.service_account import Credentials
import mysql.connector
from datetime import datetime, date
import pandas as pd
import numpy as np

# # #Load credentials from the JSON key file
# credentials = Credentials.from_service_account_file('datafromdatabase-556db248ebb2.json',
#                                                    scopes=['https://www.googleapis.com/auth/spreadsheets'])


# Load credentials from the JSON key file
credentials = Credentials.from_service_account_file('/home/ubuntu/simran/datafromdatabase-556db248ebb2.json',
                                                    scopes=['https://www.googleapis.com/auth/spreadsheets'])




# Connect to MySQL database
my_conn = mysql.connector.connect(
                                    host='3.109.191.111',
                                    user='analytics_user',
                                    password='Analytics@sql@32',
                                    db='lsq_db',
                                  )

print("Connection Query Excecuted", datetime.now())




my_cursor = my_conn.cursor()
my_cursor.execute('''SELECT installation_id,
                            sales_order_id,
                            reference_number,
                            sku,
                             CASE WHEN installation_status IN ("Cancelled","Installation Cancelled") 
                                  THEN "Cancelled"
                             ELSE installation_status END AS installation_status
                     FROM service_db.installations  ''')
                            


my_result = my_cursor.fetchall()

print("Order SQL Query Excecuted", datetime.now())


# Convert MySQL result to a DataFrame
header = [i[0] for i in my_cursor.description]
installation_df = pd.DataFrame(my_result, columns=header)





my_cursor = my_conn.cursor()
my_cursor.execute('''  Select x.reference_number,
                              x.sku,
                              x.Total_item_order,
                              x.Total_quantity_cancelled,
                              y.status,
                              y.QuantityDelivered,
                              x.salesorder_id
                      from
                         (SELECT a.salesorder_id,
                                 sku,
                                 reference_number,
                                 sum(quantity) AS Total_item_order,
                                 sum(quantity_cancelled) AS Total_quantity_cancelled,
                                 concat(a.salesorder_id,sku) AS helper
                           FROM zoho_db_new.sales_orders_line_items a
                           left join zoho_db_new.sales_orders b
                           on a.salesorder_id=b.salesorder_id
                           group by salesorder_id,sku,reference_number) x

                  left join 
                             (SELECT d.salesorder_id,
                                     sku,
                                     status,
                                     sum(quantity) AS QuantityDelivered,
                                     concat(d.salesorder_id,sku) AS helper
                              FROM zoho_db_new.packages_line_item c
                              left join zoho_db_new.packages d
                              on c.package_id=d.package_id
                              where status='delivered'
                              group by salesorder_id,sku) y
                  on x.helper=y.helper ''')
                            


my_result = my_cursor.fetchall()

print("Order SQL Query Excecuted", datetime.now())


# Convert MySQL result to a DataFrame
header = [i[0] for i in my_cursor.description]
delivery_df = pd.DataFrame(my_result, columns=header)

# Close MySQL connection
my_conn.close()


# Step 1: Merge installation_df and delivery_df
merged_df = installation_df.merge(
    delivery_df[['reference_number', 'sku', 'QuantityDelivered', 'Total_quantity_cancelled', 'Total_item_order']],
    on=['reference_number', 'sku'],
    how='left'
)

# Step 2: Sort rows to prioritize 'installation_status' as 'Cancelled'
merged_df = merged_df.sort_values(
    by=['reference_number', 'sku', 'installation_status'],
    ascending=[True, True, False]  # Sort 'installation_status' so 'Cancelled' comes first
)

# Step 3: Apply 'Cancelled' status for the 'Total_quantity_cancelled'
cancelled_mask = merged_df.groupby(['reference_number', 'sku']).cumcount() < merged_df['Total_quantity_cancelled']
merged_df.loc[cancelled_mask, 'delivery_status'] = 'Cancelled'


# Step 4: Calculate cumulative cancellations and remaining deliveries
merged_df['cumulative_cancelled'] = merged_df.groupby(['reference_number', 'sku'])['delivery_status'].transform(lambda x: (x == 'Cancelled').cumsum())
merged_df['remaining_deliveries'] = merged_df['Total_item_order'] - merged_df['cumulative_cancelled']


## Step 5: Assign 'Delivered' to QuantityDelivered number of records where delivery_status is NaN and not Cancelled
delivered_mask = (merged_df['delivery_status'].isna())  # Only consider rows with NaN
eligible_for_delivery = merged_df[delivered_mask].copy()  # Work only with NaN rows

# Use cumcount to assign Delivered to the first QuantityDelivered rows with NaN
eligible_for_delivery['cumcount'] = eligible_for_delivery.groupby(['reference_number', 'sku']).cumcount()
eligible_for_delivery.loc[
    eligible_for_delivery['cumcount'] < eligible_for_delivery['QuantityDelivered'],
    'delivery_status'
] = 'Delivered'

# Merge the updated `delivery_status` back into the main dataframe
merged_df.update(eligible_for_delivery[['delivery_status']])


# Step 6: Assign 'Not Delivered' to any remaining NaN values in delivery_status
merged_df['delivery_status'].fillna('Not Delivered', inplace=True)

result_df = merged_df[['installation_id', 'reference_number', 'sku', 'installation_status', 'delivery_status']]






# Authorize the client
gc = gspread.authorize(credentials)
# Open a Google Sheets spreadsheet by key (Sample Raised)
spreadsheet_key = '1uMhEtRANAnz2I_n1_pSSle_P3bD3QdvLfRC1fmU321o'
worksheet = gc.open_by_key(spreadsheet_key).worksheet('Delivery Status')

#Clear Worksheet
range_to_clear_sheet = 'A:G'
worksheet.batch_clear([range_to_clear_sheet])
#updating worksheet with data
gsdf.set_with_dataframe(worksheet, result_df)

