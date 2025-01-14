#!/usr/bin/env python
# coding: utf-8

# In[1]:


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
spreadsheet_key = '1AntBZqWnL2cQc1EzVn_u_HhvK_J8YNtG1gfHDQt5y0I'
worksheet1 = gc.open_by_key(spreadsheet_key).worksheet('Query')
worksheet2 = gc.open_by_key(spreadsheet_key).worksheet('ZohoData(Auto)')
worksheet3 = gc.open_by_key(spreadsheet_key).worksheet('Zoplar POC')
worksheet4 = gc.open_by_key(spreadsheet_key).worksheet('Serial Number')






# Connect to MySQL database
my_conn = mysql.connector.connect(
                                    host='3.109.191.111',
                                    user='analytics_user',
                                    password='Analytics@sql@32',
                                    db='quotation_db',
                                  )

print("Connection Query Excecuted", datetime.now())


# In[2]:


my_cursor = my_conn.cursor()
my_cursor.execute('''  SELECT date(timestamp) AS date,
                              p.queryID,
                              sku,name,
                              brand,
                              partner_name,
                              Zoplar_poc,
                              category_TAT 
                      from(SELECT b.queryID, 
                                  a.sku,
                                  a.name,
                                  a.brand,
                                  a.partner_name,
                                  c.Zoplar_poc
                            FROM quotation_db.quotation_action_items a
                            left join quotation_db.quotation_action b
                            on a.actionId=b.actionId
                            left join quotation_db.vendor_data c
                            on trim(a.partner_name)=trim(c.name)
                            where actionTaken='Sent to Pricing') p
                     left join (SELECT queryId , 
                                       SUM(CASE WHEN pending_on = 'Category' THEN TAT ELSE 0 END)/60 AS category_TAT
                                FROM (SELECT queryId,
                                             pending_on,
                                             timestampdiff(MINUTE,timestamp,COALESCE(next_action,CONVERT_TZ(CURRENT_TIMESTAMP, '+00:00', '+05:30'))) AS TAT
                                       FROM (SELECT a.timestamp,
                                                    a.queryId,
                                                    CASE   WHEN actionTaken IN ('Quotation Requested','Assigned to Pricing',
                                                                'Sent to Pricing','Re-opened by Sales')
                                                           THEN 'Pricing'
                                                           WHEN actionTaken IN ('Assigned to Category', 'Re-opened to Category')
                                                           THEN 'Category'
                                                           ELSE NULL END AS pending_on,
                                                     LEAD(timestamp) OVER (PARTITION BY a.queryId ORDER BY a.timestamp) AS next_action
                                            FROM quotation_db.quotation_action AS a) as q
                                            ) AS h GROUP BY queryId) q
                                           on   p.queryID=q.queryID
                                left join (SELECT timestamp,queryId FROM quotation_db.quotation_action
                                where actionTaken='Quotation Requested')r
                                on	p.queryID=r.queryID
                                where timestamp is not null
                                order by timestamp desc


                           ''')


my_result = my_cursor.fetchall()

print("Quotation SQL Query Excecuted", datetime.now())

# Convert MySQL result to a DataFrame
header = [i[0] for i in my_cursor.description]
df_mysql = pd.DataFrame(my_result, columns=header)


#Clear Worksheet
range_to_clear_sheet = 'A:H'
worksheet1.batch_clear([range_to_clear_sheet])
#updating worksheet with data
gsdf.set_with_dataframe(worksheet1, df_mysql )

print("Data Sent to Sheet!", datetime.now())


# In[3]:


my_cursor = my_conn.cursor()
my_cursor.execute('''  SELECT q.date AS BillDate,
                              q.payment_terms,
                              q.payment_terms_label,
                              t.cf_zoplar_poc,
                              t.contact_name,
                              (u.quantity*bcy_rate) AS CostPrice,
                              r.sku,
                              s.reference_number,
                              u.SellingPrice,
                              u.required_delivery_date,
                              u.committed_delivery_date,
                              u.ShipmentDeliveryDate,
                              r.name,
                              v.type
                              
                       FROM zoho_db_new.bills_line_items p
                       left join zoho_db_new.bills q
                       on p.bill_id=q.bill_id
                       left join zoho_db_new.purchase_order_line_item r
                       on r.line_item_id=p.purchaseorder_item_id
                       left join zoho_db_new.purchase_order s
                       on s.purchaseorder_id=r.purchaseorder_id
                       left  join zoho_db_new.partners t
                       on t.partner_id=q.vendor_id
                       left join (select a.sku,
                                         b.reference_number,
                                         (a.quantity*a.rate) AS SellingPrice,
                                         DATE(STR_TO_DATE(b.cf_customer_required_delivery_, '%d/%m/%Y %h:%i %p')) AS  required_delivery_date,
                                         DATE(STR_TO_DATE(b.cf_committed_delivery_date, '%d/%m/%Y')) AS committed_delivery_date,
                                         DATE(e.shipment_delivered_date) AS ShipmentDeliveryDate,
                                         concat(a.sku,b.reference_number) AS helper,
                                         a.quantity
                                   from zoho_db_new.sales_orders_line_items a
                                   left join zoho_db_new.sales_orders b
                                   on a.salesorder_id=b.salesorder_id
                                   left join zoho_db_new.packages_line_item c
                                   on a.line_item_id=c.so_line_item_id
                                   left join zoho_db_new.shipments_line_item d
                                   on d.line_item_id=c.line_item_id
                                   left join zoho_db_new.shipments e
                                   on d.shipment_id=e.shipment_id) u
                       on u.helper=concat(r.sku,s.reference_number)
                       
                       left join (SELECT sku,
                                         CASE 
                                             WHEN Product_Name LIKE '%pre-owned%' 
                                                  OR Product_Category = 'Medical Devices (Pre-Owned)' 
                                                  OR Product_Name LIKE '%pre owned%' 
                                             THEN 'Pre-owned'
                                         ELSE 'New'
                                         END AS Type
                                  FROM product_db.sku) v
                                  on r.sku=v.sku
                                  
                       where q.status in ( 'paid','overdue','open')
                       
                           ''')


my_result = my_cursor.fetchall()

print("ZohoData SQL Query Excecuted", datetime.now())

# Convert MySQL result to a DataFrame
header = [i[0] for i in my_cursor.description]
df_mysql = pd.DataFrame(my_result, columns=header)


#Creating Margin 
df_mysql['SellingPrice'] = pd.to_numeric(df_mysql['SellingPrice'], errors='coerce')
df_mysql['CostPrice'] = pd.to_numeric(df_mysql['CostPrice'], errors='coerce')



# df_mysql.dropna(subset=['SellingPrice', 'CostPrice'], inplace=True)

df_mysql['margin'] = (df_mysql['SellingPrice'] - df_mysql['CostPrice']) / df_mysql['SellingPrice']

df_mysql.replace([np.inf, -np.inf], np.nan, inplace=True)





#Clear Worksheet
range_to_clear_sheet = 'A:O'
worksheet2.batch_clear([range_to_clear_sheet])
#updating worksheet with data
gsdf.set_with_dataframe(worksheet2, df_mysql )

print("Data Sent to Sheet!", datetime.now())


# In[6]:


my_cursor = my_conn.cursor()
my_cursor.execute('''   Select distinct * from ((SELECT serial_numbers,
                                                        cf_zoplar_poc
                                                 FROM zoho_db.bills a
                                                 
                                                 left join zoho_db.bills_line_items b
                                                 on a.bill_id=b.bill_id
                                                 
                                                 left join zoho_db.partners c
                                                 on a.vendor_id=c.partner_id
                                                 
                                                 left join zoho_db_new.partners d
                                                 on c.cf_vendor_id= d.cf_vendor_id
                                                 
                                                 where serial_numbers IS NOT NULL and serial_numbers not in ("")  
                                                 
                                                 union all
                                                 
                                                 SELECT serial_numbers,
                                                        cf_zoplar_poc 
                                                 FROM zoho_db_new.bills a
                                                 
                                                 left join zoho_db_new.bills_line_items b
                                                 on a.bill_id=b.bill_id
                                                 
                                                 left join zoho_db_new.partners c
                                                 on a.vendor_id=c.partner_id
                                                 
                                                 where serial_numbers IS NOT NULL and serial_numbers not in (""))
                                                 
                                                 
                                                 union all
                                                 
                                                 (SELECT serial_numbers,
                                                         cf_zoplar_poc
                                                 FROM zoho_db_new.purchase_received_line_item a
                                                 
                                                 left join  zoho_db_new.purchase_received b
                                                 on a.purchasereceive_id=b.receive_id
                                                 
                                                 left join zoho_db_new.partners c
                                                 on b.vendor_id =c.partner_id
                                                 
                                                 where serial_numbers IS NOT NULL and serial_numbers not in ("")
                                                 
                                                 union all
                                                 
                                                 SELECT serial_numbers,
                                                        cf_zoplar_poc
                                                 FROM zoho_db.purchase_received_line_item a
                                                 
                                                 left join  zoho_db.purchase_received b
                                                 on a.purchasereceive_id=b.receive_id
                                                 
                                                 left join zoho_db.partners c
                                                 on b.vendor_id =c.partner_id
                                                 
                                                 left join zoho_db_new.partners d
                                                 on c.cf_vendor_id= d.cf_vendor_id
                                                 
                                                 where serial_numbers IS NOT NULL and serial_numbers not in (""))) X                      
                                                                            ''')


my_result = my_cursor.fetchall()

print("Zoplar POC SQL Query Excecuted", datetime.now())

# Convert MySQL result to a DataFrame
header = [i[0] for i in my_cursor.description]
df_mysql = pd.DataFrame(my_result, columns=header)






#Clear Worksheet
range_to_clear_sheet = 'A:B'
worksheet3.batch_clear([range_to_clear_sheet])
#updating worksheet with data
gsdf.set_with_dataframe(worksheet3, df_mysql )

print("Data Sent to Sheet!", datetime.now())


# In[8]:


my_cursor = my_conn.cursor()
my_cursor.execute('''   with new AS (select * from (Select b.reference_number,
                                                           a.sku,
                                                           COALESCE(NULLIF(c.serial_numbers, ''), d.serial_numbers) AS serial_numbers
                                                    from zoho_db_new.sales_orders_line_items a
                                                    
                                                    left join zoho_db_new.sales_orders b
                                                    on a.salesorder_id=b.salesorder_id
                                                    
                                                    left join zoho_db_new.packages_line_item c
                                                    on a.line_item_id=c.so_line_item_id
                                                    
                                                    left join zoho_db_new.invoices_line_item d
                                                    on a.line_item_id=d.salesorder_item_id) x
                                      where serial_numbers IS NOT NULL and serial_numbers not in ("")),
                                                    
                             old AS (select * from (Select b.reference_number,
                                                           a.sku,
                                                           COALESCE(NULLIF(c.serial_numbers, ''), d.serial_numbers) AS serial_numbers
                                                     from zoho_db.sales_orders_line_items a
                                                     
                                                     left join zoho_db.sales_orders b                                                     
                                                     on a.salesorder_id=b.salesorder_id
                                                     
                                                     left join zoho_db.packages_line_item c                                                     
                                                     on a.line_item_id=c.so_line_item_id
                                                     
                                                     left join zoho_db.invoices_line_item d
                                                     on a.line_item_id=d.salesorder_item_id) x
                                     where serial_numbers IS NOT NULL and serial_numbers not in (""))
                                                   
                                                   
                                                   
                             select concat(reference_number," <> ",sku) AS Helper,
                                    serial_numbers  from new a
                                    
                             union all
                             
                             select concat(reference_number," <> ",sku) AS Helper,
                                    serial_numbers from old b
                                    
                               WHERE  NOT EXISTS ( SELECT 1 FROM new c
                                                   WHERE b.reference_number = c.reference_number)
                                                                         
                                                                              ''')


my_result = my_cursor.fetchall()

print("Zoplar POC SQL Query Excecuted", datetime.now())

# Convert MySQL result to a DataFrame
header = [i[0] for i in my_cursor.description]
df_mysql = pd.DataFrame(my_result, columns=header)






#Clear Worksheet
range_to_clear_sheet = 'A:B'
worksheet4.batch_clear([range_to_clear_sheet])
#updating worksheet with data
gsdf.set_with_dataframe(worksheet4, df_mysql )

print("Data Sent to Sheet!", datetime.now())

