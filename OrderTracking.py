#!/usr/bin/env python
# coding: utf-8

# In[7]:


import gspread
import gspread_dataframe as gsdf
from google.oauth2.service_account import Credentials
import mysql.connector
from datetime import datetime, date

import pandas as pd
import numpy as np

# # Load credentials from the JSON key file
# credentials = Credentials.from_service_account_file('datafromdatabase-556db248ebb2.json',
#                                                     scopes=['https://www.googleapis.com/auth/spreadsheets'])


# # Load credentials from the JSON key file
credentials = Credentials.from_service_account_file('/home/ubuntu/zoho/datafromdatabase-556db248ebb2.json',
                                                    scopes=['https://www.googleapis.com/auth/spreadsheets'])

# Authorize the client
gc = gspread.authorize(credentials)

# Open a Google Sheets spreadsheet by key(Zoho working)
spreadsheet_key = '14s0XALM_v0ywJJ7RhOVtqLGFO4wjULMk2Kmlw-_Rpzg'
worksheet1 = gc.open_by_key(spreadsheet_key).worksheet('order_tracking')


# Open a Google Sheets spreadsheet by key(Collection)
spreadsheet_key2 = '17hfReleXYzBcCuwXFnkDx3Ps3P_gwumxSuqGxu111og'
worksheet2= gc.open_by_key(spreadsheet_key2).worksheet('python')

# Open a Google Sheets spreadsheet by key(Invoice)
spreadsheet_key3 = '14s0XALM_v0ywJJ7RhOVtqLGFO4wjULMk2Kmlw-_Rpzg'
worksheet3= gc.open_by_key(spreadsheet_key3).worksheet('invoice_receiving')


# Open a Google Sheets spreadsheet by key(Order Tracking)
# spreadsheet_key = '1oKEvWPP2CWSh_t0efHSVx-P6mVSt8mPZnOgTEC5EaEo'
# worksheet4 = gc.open_by_key(spreadsheet_key).worksheet('order_tracking')





# Connect to MySQL database
my_conn = mysql.connector.connect(
                                    host='3.109.191.111',
                                    user='analytics_user',
                                    password='Analytics@sql@32',
                                    db='lsq_db',
                                  )
my_cursor = my_conn.cursor()

print("Connection Query Excecuted", datetime.now())


# In[8]:


my_cursor = my_conn.cursor()

my_cursor.execute(''' with old AS (Select * From
                      (SELECT a.salesorder_id,
                              a.date,
                              a.salesorder_number, 
                              trim(cast(a.reference_number AS char)) AS Order_ID,
                              a.customer_id,
                              b.cf_client_id AS client_id,
                              b.contact_name,
                              a.cf_supply_poc AS Category_POC,
                              DATE(STR_TO_DATE(a.cf_customer_required_delivery_, '%d/%m/%Y %h:%i %p')) AS  required_delivery_date,
                              DATE(STR_TO_DATE(a.cf_committed_delivery_date, '%d/%m/%Y')) AS committed_delivery_date,
                              a.shipment_date AS expected_shipment_date,
                              a.status AS Order_status,
                              a.shipment_date,
                              a.shipping_city,
                              a.payment_terms AS payment_terms_in_days,
                              a.payment_terms_label,
                              a.last_modified_time,
                              DATE_FORMAT(STR_TO_DATE(a.last_modified_time, '%Y-%m-%dT%H:%i:%s'), '%Y-%m-%d') AS Last_modified,
                              a.cf_expected_delivery_time_unformatted AS expected_delivery_time,
                              a.salesperson_name
                                                 FROM zoho_db.sales_orders a
                                                 left join zoho_db.client b
                                                 on a.customer_id=b.client_id
                                                 where cf_order_sample='Order Raised'
                                                 group by a.salesorder_id)p
                                                 
                                                 
                              Left join (SELECT salesorder_id AS salesorder_id2 ,
                                                GROUP_CONCAT(concat(name,",",quantity) SEPARATOR ', ') AS Product_details,
                                                round(Sum(quantity),3) as qty_Ordered,
                                                sum(quantity_cancelled) AS quantity_cancelled,
                                                sum(quantity_invoiced) AS quantity_invoiced 
                                                
                                                                          FROM zoho_db.sales_orders_line_items
                                                                          group by salesorder_id) q
                                                                          on p.salesorder_id=q.salesorder_id2
                                               
                             left join (SELECT  z.salesorder_id AS salesorder_id3,
                                                round(SUM(CASE WHEN z.status = 'Delivered' THEN y.quantity ELSE 0 END),3) AS Qty_Delivered,
                                                SUM(CASE WHEN z.status IN ('Shipped', 'Delivered') THEN y.quantity ELSE 0 END) AS                                                                                 Qty_Shipped,
                                                max(shipping_address) AS shipping_address,
                                                max(billing_address) AS billing_address
                                                
                                                                      FROM zoho_db.packages_line_item AS y
                                                                      LEFT JOIN zoho_db.packages AS z ON y.package_id = z.package_id
                                                                      GROUP BY z.salesorder_id) r
                                                                      on p.salesorder_id=r.salesorder_id3
                                                
                            left join (Select salesorder_id AS salesorder_id9 ,
                                              sum(shipping_charge) AS Shipping_Charge, 
                                              max(date(shipment_delivered_date)) AS Delivery_Date,
                                              max(carrier) AS Delivery_Mode
                                                                FROM zoho_db.shipments 
                                                                group by salesorder_id) s
                                                                on p.salesorder_id=s.salesorder_id9
                            left join (  SELECT salesorder_id4,
                                                SUM(item_total) AS Revenue_Excluding_GST,
                                                SUM(item_total + (item_total * tax_percentage / 100)) AS Revenue_Including_GST
                                         FROM ( SELECT f.status, 
                                                       g.item_total, 
                                                       g.salesorder_id AS salesorder_id4 , 
                                                       g.tax_percentage
                                                FROM zoho_db.sales_orders f
                                                RIGHT JOIN zoho_db.sales_orders_line_items g 
                                                ON f.salesorder_id = g.salesorder_id
                                              WHERE status IN ('Fulfilled', 'Confirmed', 'onhold', 'shipped','partially_shipped')
                                               ) AS temp1 GROUP BY salesorder_id4) t
                                               
                                               on p.salesorder_id=t.salesorder_id4
                                     left join(select sum(rate) AS ReturnRate,
                                                 Sum(quantity) AS ReturnQty,
                                                 Sum(item_total) AS ReturnTotal,
                                                 salesorder_id AS salesorder_idReturn
                                        from zoho_db.salesreturns_line_item bt
                                        right join zoho_db.salesreturns at
                                        on at.salesreturn_id =bt.salesreturn_id
                                        where salesreturn_status in ('approved','closed')
                                        group by salesorder_id)u
                                        on p.salesorder_id=u.salesorder_idReturn
                                        
                                        
                                        
                                left join(   SELECT max(a.date) AS PackageDate,
                                                    d.salesorder_id AS salesorder_idPackage,
                                                    sum(d.quantity) AS quantity_packed
                                             FROM  zoho_db.packages_line_item d
                                             LEFT join zoho_db.packages a
                                             on a.salesorder_id=d.salesorder_id
                                             group by d.salesorder_id)v
                                             on p.salesorder_id=v.salesorder_idPackage),
	
    
    
    
    new AS (Select * From
                      (SELECT a.salesorder_id,
                              a.date,
                              a.salesorder_number, 
                              trim(cast(a.reference_number AS char)) AS Order_ID,
                              a.customer_id,
                              b.cf_client_id AS client_id,
                              b.contact_name,
                              a.cf_supply_poc AS Category_POC,
                              DATE(STR_TO_DATE(a.cf_customer_required_delivery_, '%d/%m/%Y %h:%i %p')) AS  required_delivery_date,
                              DATE(STR_TO_DATE(a.cf_committed_delivery_date, '%d/%m/%Y')) AS committed_delivery_date,
                              a.shipment_date AS expected_shipment_date,
                              a.status AS Order_status,
                              a.shipment_date,
                              a.shipping_city,
                              a.payment_terms AS payment_terms_in_days,
                              a.payment_terms_label,
                              a.last_modified_time,
                              DATE_FORMAT(STR_TO_DATE(a.last_modified_time, '%Y-%m-%dT%H:%i:%s'), '%Y-%m-%d') AS Last_modified,
                              a.cf_expected_delivery_time_unformatted AS expected_delivery_time,
                              a.salesperson_name
                                                 FROM zoho_db_new.sales_orders a
                                                 left join zoho_db_new.client b
                                                 on a.customer_id=b.client_id
                                                 where cf_order_sample='Order Raised'
                                                 group by a.salesorder_id)p
                                                 
                                                 
                              Left join (SELECT salesorder_id AS salesorder_id2 ,
                                                GROUP_CONCAT(concat(name,",",quantity) SEPARATOR ', ') AS Product_details,
                                                Sum(quantity) as qty_Ordered,
                                                sum(quantity_cancelled) AS quantity_cancelled,
                                                sum(quantity_invoiced) AS quantity_invoiced 
                                                
                                                                          FROM zoho_db_new.sales_orders_line_items
                                                                          group by salesorder_id) q
                                                                          on p.salesorder_id=q.salesorder_id2
                                               
                             left join (SELECT  z.salesorder_id AS salesorder_id3,
                                                SUM(CASE WHEN z.status = 'Delivered' THEN y.quantity ELSE 0 END) AS Qty_Delivered,
                                                SUM(CASE WHEN z.status IN ('Shipped', 'Delivered') THEN y.quantity ELSE 0 END) AS                                                                                 Qty_Shipped,
                                                max(shipping_address) AS shipping_address,
                                                max(billing_address) AS billing_address
                                                
                                                                      FROM zoho_db_new.packages_line_item AS y
                                                                      LEFT JOIN zoho_db_new.packages AS z ON y.package_id = z.package_id
                                                                      GROUP BY z.salesorder_id) r
                                                                      on p.salesorder_id=r.salesorder_id3
                                                
                            left join (Select salesorder_id AS salesorder_id9 ,
                                              sum(shipping_charge) AS Shipping_Charge, 
                                              max(date(shipment_delivered_date)) AS Delivery_Date,
                                              max(carrier) AS Delivery_Mode
                                                                FROM zoho_db_new.shipments 
                                                                group by salesorder_id) s
                                                                on p.salesorder_id=s.salesorder_id9
                            left join (  SELECT salesorder_id4,
                                                SUM(item_total) AS Revenue_Excluding_GST,
                                                SUM(item_total + (item_total * tax_percentage / 100)) AS Revenue_Including_GST
                                         FROM ( SELECT f.status, 
                                                       g.item_total, 
                                                       g.salesorder_id AS salesorder_id4 , 
                                                       g.tax_percentage
                                                FROM zoho_db_new.sales_orders f
                                                RIGHT JOIN zoho_db_new.sales_orders_line_items g 
                                                ON f.salesorder_id = g.salesorder_id
                                              WHERE status IN ('Fulfilled', 'Confirmed', 'onhold', 'shipped','partially_shipped','overdue','open','invoiced','partially_invoiced')
                                               ) AS temp1 GROUP BY salesorder_id4) t
                                               
                                               on p.salesorder_id=t.salesorder_id4
                                     left join(select sum(rate) AS ReturnRate,
                                                 Sum(quantity) AS ReturnQty,
                                                 Sum(item_total) AS ReturnTotal,
                                                 salesorder_id AS salesorder_idReturn
                                        from zoho_db_new.salesreturns_line_item bt
                                        right join zoho_db_new.salesreturns at
                                        on at.salesreturn_id =bt.salesreturn_id
                                        where salesreturn_status in ('approved','closed')
                                        group by salesorder_id)u
                                        on p.salesorder_id=u.salesorder_idReturn
                                        
                                        
                                        
                                left join(   SELECT max(a.date) AS PackageDate,
                                                    d.salesorder_id AS salesorder_idPackage,
                                                    sum(d.quantity) AS quantity_packed
                                             FROM  zoho_db_new.packages_line_item d
                                             LEFT join zoho_db_new.packages a
                                             on a.salesorder_id=d.salesorder_id
                                             group by d.salesorder_id)v
                                             on p.salesorder_id=v.salesorder_idPackage)
                                             
select * from new a
union all
select * from old b
WHERE 
    NOT EXISTS (
        SELECT 1 
        FROM new c
        WHERE b.Order_ID = c.Order_ID)
                                    
                                    
                                                                                                         ''')

my_result = my_cursor.fetchall()

# Convert MySQL result to a DataFrame
header = [i[0] for i in my_cursor.description]
tracker = pd.DataFrame(my_result, columns=header)


# # Close MySQL connection
# my_conn.close()

print("SQL Query Excecuted", datetime.now())



#Creating Qty Not Shipper
tracker['Qty_Not_Shipped']= tracker['qty_Ordered']-tracker['Qty_Shipped']




# creating Delivery Status
conditions = [
    tracker['Order_status'] == "draft",                                        
    # If Order_status is "draft"
    tracker['Order_status'] == "void",                                         
    # If Order_status is "void"
    tracker['quantity_cancelled'] == tracker['qty_Ordered'],                    
    # If quantity_cancelled equals qty_Ordered
    tracker['qty_Ordered'] - tracker['quantity_cancelled'] == tracker['Qty_Delivered'],
     # If quantity_cancelled equals qty_Ordered & Return
    tracker['qty_Ordered'] - tracker['quantity_cancelled'] == tracker['Qty_Delivered']+tracker['ReturnQty'],                                              
    # If Qty_Shipped is 0
    tracker['Qty_Delivered'] > 0,                                              
    # If Qty_Delivered is greater than 0
    tracker['qty_Ordered'] - tracker['quantity_cancelled'] == tracker['Qty_Shipped'],    
    # If qty_Ordered - quantity_cancelled equals Qty_Shipped
    tracker['Qty_Shipped'] > 0                                                
    # If Qty_Shipped is greater than 0
]

choices = [
    "To be confirmed",
    "Cancelled",
    "Cancelled",
    "Delivered",
    "Delivered", 
    "Partially Delivered",
    "Shipped",
    "Partially Shipped"
]


tracker['Delivery_Status'] = np.select(conditions, choices, default="Not Shipped")






#Creating percentage % of Total Items Delivered / Shipped

delivered_ratio = tracker['Qty_Delivered'] / (tracker['qty_Ordered'] - tracker['quantity_cancelled'])
shipped_ratio = tracker['Qty_Shipped'] / (tracker['qty_Ordered'] - tracker['quantity_cancelled'])


tracker['% of Total Items Delivered / Shipped'] = np.where(tracker['Qty_Delivered'] > 0, delivered_ratio,
                                  np.where(tracker['Qty_Shipped'] > 0, shipped_ratio, "-"))





# creating Delivery Delay Flag
tracker['committed_delivery_date'] = pd.to_datetime(tracker['committed_delivery_date'])

today = pd.Timestamp(datetime.now().date())

tracker['Delivery Delay Flag'] = tracker.apply(lambda row: 1 if (today - row['committed_delivery_date']).days > 0 and row['Delivery_Status'] != "Delivered" else 0, axis=1)


#Create Is the client aligned for logistics charges? (Yes/No) with blank values
tracker['Is the client aligned for logistics charges? (Yes/No)'] = ""



#Create Invoice Status
def calculate_invoiced_status(row):
    
    if row['qty_Ordered'] - row['quantity_cancelled'] == row['quantity_invoiced']:
        return "Invoiced"
    elif row['Order_status'] == 'draft':
        return "To be confirmed"
    elif row['Order_status'] == 'void':
        return "Cancelled"
    elif row['quantity_invoiced'] > 0:
        return "Partially Invoiced"
    elif row['quantity_invoiced'] == 0:
        return "Not Invoiced"
    else:
        return ""


tracker['Invoiced_Status'] = tracker.apply(calculate_invoiced_status, axis=1)


# Fetch data from the Google Sheets directly into DataFrames for invoice date
df_sheet2 = pd.DataFrame(worksheet2.get_all_records())
invoice_df = df_sheet2.head(4500)

tracker['Order_ID'] = tracker['Order_ID'].astype(str)
invoice_df['Order IDX'] = invoice_df['Order IDX'].astype(str)


tracker = tracker.merge(invoice_df[['Order IDX','Order Date', 'Invoice Date','Payment Status']],
                                   left_on='Order_ID', right_on='Order IDX', how='left')


tracker= tracker.drop(columns=['Order IDX'])
tracker['Order Date'] = pd.to_datetime(tracker['Order Date'], errors='coerce')



# Creating On-Time Delivery
tracker['Delivery_Date'] = pd.to_datetime(tracker['Delivery_Date'])
tracker['committed_delivery_date'] = pd.to_datetime(tracker['committed_delivery_date'])

tracker['On-Time Delivery'] = tracker.apply(lambda row: 'Yes' if row['committed_delivery_date'] >= row['Delivery_Date'] else 'No', axis=1)


# creating In-Full Delivery
tracker['In-Full Delivery'] = tracker.apply(lambda row: 'Yes' if row['qty_Ordered'] == row['Qty_Delivered'] else 'No', axis=1)


#Creating OTIF 
tracker['OTIF'] = np.where((tracker['On-Time Delivery'] == 'Yes') & (tracker['In-Full Delivery'] == 'Yes'), 'Yes', 'No')


# Creating Total Order Cycle (till delivery)
tracker['date'] = pd.to_datetime(tracker['date'])


tracker['Total Order Cycle (till delivery)'] = np.where(tracker['Delivery_Status'] == 'Delivered',
                                                        (tracker['Delivery_Date'] - tracker['date']).dt.days,
                                                        '')

# Creating Closing Date
tracker['Invoice Date'] = pd.to_datetime(tracker['Invoice Date'], format='%d-%b-%y', errors='coerce')


# tracker['Closing_Date'] = np.maximum(tracker['Delivery_Date'], tracker['Invoice Date'])

def calculate_closing_date(row):
    if pd.notnull(row['Delivery_Date']) or pd.notnull(row['Invoice Date']):
        return max(row['Delivery_Date'], row['Invoice Date'])
    else:
        return 'Not Closed'

# Apply the custom function to each row
tracker['Closing_Date'] = tracker.apply(calculate_closing_date, axis=1)



# Creating Receiving Invoice form filled (Yes/No)
# Fetch data from the Google Sheets directly into DataFrames for invoice date
df_sheet3 = pd.DataFrame(worksheet3.get_all_records())
df3 = df_sheet3.head(4500)


df3['Order ID1'] = df3['Order ID1'].astype(str)
df3.drop_duplicates(subset=['Order ID1'], inplace=True)


# Define the conditions
tracker = tracker.merge(df3[['Order ID1','Upload Signed Invoice Receiving']],
                                   left_on='Order_ID', right_on='Order ID1', how='left')


# conditions = [
#     tracker['Order ID1'] == "" |tracker['Order ID1'].isnull(),
#     tracker['Upload Signed Invoice Receiving'] != ""
# ]

# # Define the corresponding values
# values = ["No", "Yes"]

# # Apply the conditions and assign the result to a new column
# tracker['Receiving Invoice form filled (Yes/No)'] = np.where(conditions[0], values[0], np.where(conditions[1], values[1], "No"))

# Define the conditions
condition_1 = (tracker['Order ID1'].isnull()) | (tracker['Order ID1'].str.strip() == '')
condition_2 = tracker['Upload Signed Invoice Receiving'] != ""

# Apply the conditions and assign the result to a new column
tracker['Receiving Invoice form filled (Yes/No)'] = np.where(
    condition_1, 'No', np.where(condition_2, 'Yes', 'No')
)


# Creating Payment Terms Date
tracker['payment_terms_in_days'] = tracker['payment_terms_in_days'].astype(int)

# Convert dates to timedelta objects
tracker['Delivery_Date'] = pd.to_datetime(tracker['Delivery_Date'])
tracker['Invoice Date'] = pd.to_datetime(tracker['Invoice Date'])
tracker['Order Date'] = pd.to_datetime(tracker['Order Date'])

# Convert payment terms to timedelta objects
payment_terms_timedelta = pd.to_timedelta(tracker['payment_terms_in_days'], unit='D')

# Define the conditions and corresponding values
conditions = [
    (tracker['Invoiced_Status'] == 'Invoiced') & (tracker['Delivery_Status'] == 'Delivered'),
    (tracker['Invoiced_Status'] == 'Partially Invoiced') & (tracker['Delivery_Status'] == 'Partially Delivered'),
    (tracker['Invoiced_Status'] == 'Invoiced') & (tracker['Delivery_Status'] != 'Delivered'),
    (tracker['Invoiced_Status'] == 'Partially Invoiced') & (tracker['Delivery_Status'] != 'Delivered'),
    (tracker['Invoiced_Status'] != 'Invoiced') & (tracker['Delivery_Status'] == 'Delivered'),
    (tracker['Invoiced_Status'] != 'Invoiced') & (tracker['Delivery_Status'] == 'Partially Delivered'),

    True
]

values = [
    (np.maximum(tracker['Delivery_Date'], tracker['Invoice Date']) + payment_terms_timedelta).astype(object),
    (np.maximum(tracker['Delivery_Date'], tracker['Invoice Date']) + payment_terms_timedelta).astype(object),
    (tracker['Invoice Date'] + payment_terms_timedelta).astype(object),
    (tracker['Invoice Date'] + payment_terms_timedelta).astype(object),
    (tracker['Delivery_Date'] + payment_terms_timedelta).astype(object),
    (tracker['Delivery_Date'] + payment_terms_timedelta).astype(object),
    (tracker['Order Date'] + payment_terms_timedelta).astype(object)
]

# Apply the conditions and assign the result to a new column 'Payment Terms Date'
tracker['Payment Terms Date'] = np.select(conditions, values)


#creating Order Count Flag
tracker['Order Count Flag'] = tracker['Order_status'].apply(lambda x: 0 if x in ['draft', 'void'] else 1)


#creating Delivery Flag
tracker['Delivery Flag'] = (tracker['Delivery_Status'].isin(["Delivered", "Partially Delivered"])).astype(int)


# Creating EMI / Non EMI
tracker['EMI / Non EMI'] = tracker['payment_terms_label'].map({
    "Due on Receipt": "Non EMI",
    "Non EMI": "Non EMI",
    "Due end of the month": "Non EMI",
    "Due end of next month": "Non EMI",
    "Advance": "Non EMI",
    "Within 7 Days": "Non EMI",
    "Within 15 Days": "Non EMI",
    "Within 30 Days": "Non EMI",
    "Within 45 Days": "Non EMI",
    "Within 60 Days": "Non EMI",
    "Within 90 Days": "Non EMI",
    "Within 120 Days": "Non EMI",
    "EMI - 2 Months": "EMI",
    "EMI - 3 Months": "EMI",
    "EMI - 4 Months": "EMI",
    "EMI - 5 Months": "EMI",
    "EMI - 6 Months": "EMI",
    "EMI - 7 Months": "EMI",
    "EMI - 8 Months": "EMI",
    "EMI - 9 Months": "EMI",
    "EMI - 10 Months": "EMI",
    "EMI - 11 Months": "EMI",
    "EMI - 12 Months": "EMI",
    "EMI - 24 Months": "EMI",
    "EMI - 36 Months": "EMI"
})


tracker['Remarks for Delivery Delay'] = ""


#Create Invoice Status
def calculate_package_status(row):
    
    if row['qty_Ordered'] - row['quantity_cancelled'] == row['quantity_packed']:
        return "Packed"
    elif row['Order_status'] == 'draft':
        return "To be confirmed"
    elif row['Order_status'] == 'void':
        return "Cancelled"
    elif row['quantity_packed'] > 0:
        return "Partially Packed"
    elif row['quantity_packed'] == 0:
        return "Not Invoiced"
    else:
        return "No Package"


tracker['Package_Status'] = tracker.apply(calculate_package_status, axis=1)





#Create Invoice Status
def calculate_shipment_status(row):
    
    if row['qty_Ordered'] - row['quantity_cancelled'] == row['Qty_Shipped']:
        return "Shipped"
    elif row['Order_status'] == 'draft':
        return "To be confirmed"
    elif row['Order_status'] == 'void':
        return "Cancelled"
    elif row['Qty_Shipped'] > 0:
        return "Partially Shipped"
    elif row['Qty_Shipped'] == 0:
        return "Not Shipped"
    else:
        return "No Shipment"


tracker['Shipment_Status'] = tracker.apply(calculate_shipment_status, axis=1)









column_to_keep=['salesorder_id',
                'date',
                'salesorder_number',
                'Order_ID',
                'customer_id',
                'client_id',
                'contact_name',
                'Product_details',
                'Category_POC',
                'required_delivery_date',
                'committed_delivery_date',
                'shipment_date',
                'Order_status',
                'qty_Ordered',
                'Qty_Not_Shipped',
                'Qty_Shipped',
                'Qty_Delivered',
                'quantity_cancelled',
                'quantity_invoiced',
                'Delivery_Status',
                '% of Total Items Delivered / Shipped',
                'Delivery Delay Flag',
                'shipment_date',
                'Is the client aligned for logistics charges? (Yes/No)',
                'Shipping_Charge',
                'Delivery_Date',
                'shipping_city',
                'Delivery_Mode',
                'Invoiced_Status',
                'Invoice Date',
                'On-Time Delivery',
                'In-Full Delivery',
                'OTIF',
                'Total Order Cycle (till delivery)',
                'Closing_Date',
                'Payment Status',
                'Revenue_Excluding_GST',
                'Revenue_Including_GST',
                'Receiving Invoice form filled (Yes/No)',
                'Order Count Flag',
                'payment_terms_in_days',
                'Payment Terms Date',
                'Delivery Flag',
                'payment_terms_label',
                'EMI / Non EMI',
                'Last_modified',
                'billing_address',
                'shipping_address',
                'Order Date',
                'expected_delivery_time',
                'Remarks for Delivery Delay',
                'Upload Signed Invoice Receiving',
                'quantity_packed', 
                'PackageDate',
                'Package_Status',
                'Shipment_Status',
                'salesperson_name'
                
               ]



mydf = tracker[column_to_keep].copy()


print("Order Tracking Sheet Created", datetime.now())


# In[13]:


# #Clear Worksheet NEW
range_to_clear_sheet2 = 'A:BE'
worksheet1.batch_clear([range_to_clear_sheet2])



# #updating worksheet with data
gsdf.set_with_dataframe(worksheet1,mydf)
print("Sent data to Sheet!", datetime.now())



# #Clear Worksheet NEW
range_to_clear_sheet2 = 'A:BE'
worksheet4.batch_clear([range_to_clear_sheet2])



# #updating worksheet with data
# gsdf.set_with_dataframe(worksheet4,mydf)
# print("Sent data to Sheet!", datetime.now())




# In[ ]:





# In[ ]:




