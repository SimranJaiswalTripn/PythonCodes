
import gspread
import gspread_dataframe as gsdf
from google.oauth2.service_account import Credentials
import mysql.connector
from datetime import datetime, date
import pandas as pd
import numpy as np

# Load credentials from the JSON key file
# credentials = Credentials.from_service_account_file('datafromdatabase-556db248ebb2.json',
#                                                    scopes=['https://www.googleapis.com/auth/spreadsheets'])


# Load credentials from the JSON key file
credentials = Credentials.from_service_account_file('/home/ubuntu/zoho/datafromdatabase-556db248ebb2.json',
                                                    scopes=['https://www.googleapis.com/auth/spreadsheets'])


# Authorize the client
gc = gspread.authorize(credentials)

# Open a Google Sheets spreadsheet by key
spreadsheet_key = '1Ef2kIL4f_hlcEPsJLRVdpmK1zAlJmNS8NpZY84dieu4'
worksheet2 = gc.open_by_key(spreadsheet_key).worksheet('Hard Paste Cost Price')
worksheet3 = gc.open_by_key(spreadsheet_key).worksheet('BD Validation')
worksheet5= gc.open_by_key(spreadsheet_key).worksheet('Zone')
worksheet6= gc.open_by_key(spreadsheet_key).worksheet('Order Confirmation Date')
worksheet7= gc.open_by_key(spreadsheet_key).worksheet('Old Orders')
worksheet8= gc.open_by_key(spreadsheet_key).worksheet('Old delivery')
worksheet9= gc.open_by_key(spreadsheet_key).worksheet('Delivery')
worksheet10= gc.open_by_key(spreadsheet_key).worksheet('Old Invoice')
worksheet11= gc.open_by_key(spreadsheet_key).worksheet('Invoice')
worksheet12= gc.open_by_key(spreadsheet_key).worksheet('Pricing List')
worksheet15= gc.open_by_key(spreadsheet_key).worksheet('Orders on Hold')



# Open a Google Sheets spreadsheet by key (new wip)
spreadsheet_key = '1rTNogBqgtNLDt316tPAqBdN1I4f46FDWrmaPwrFSiQE'
worksheetP = gc.open_by_key(spreadsheet_key).worksheet('Order Details')



# Open a Google Sheets spreadsheet by key (Orders)
spreadsheet_key = '1KSg7D9MzIniYD2ljvbXGCajg2uIE3__2XW7VHzJvQko'
worksheet4 = gc.open_by_key(spreadsheet_key).worksheet('orders_received')
worksheet13 = gc.open_by_key(spreadsheet_key).worksheet('Order Details')
worksheet16 = gc.open_by_key(spreadsheet_key).worksheet('Discount')



# Open a Google Sheets spreadsheet by key (Dashboard WIP)
spreadsheet_key = '1nCnOtENOw0Wacp7wgEB3hT8tz-_wDFc0gyOy3vI4l98'
worksheetX = gc.open_by_key(spreadsheet_key).worksheet('Order Details')


# Open a Google Sheets spreadsheet by key(sales Dashboard 2.0)
spreadsheet_key = '14AHw1iD6kaapYXAmDpYN8_BQkqaX-rIrFqeU2KhYLVM'
worksheetY = gc.open_by_key(spreadsheet_key).worksheet('Order Details')

# Open a Google Sheets spreadsheet by key(orders2.0)
spreadsheet_key = '1kQBW-XdGaSm_zBK3jomLyUCj0JSdW-UuzJf5HfvcPc8'
worksheetV = gc.open_by_key(spreadsheet_key).worksheet('Order Details')

# Open a Google Sheets spreadsheet by key (Collection Dashboard)
spreadsheet_key = '17hfReleXYzBcCuwXFnkDx3Ps3P_gwumxSuqGxu111og'
worksheetC = gc.open_by_key(spreadsheet_key).worksheet('order_details')






# Connect to MySQL database
my_conn = mysql.connector.connect(
                                    host='3.109.191.111',
                                    user='analytics_user',
                                    password='Analytics@sql@32',
                                    db='lsq_db',
                                  )

print("Connection Query Excecuted", datetime.now())




my_cursor = my_conn.cursor()
my_cursor.execute('''SELECT     TRIM(a.reference_number) AS Order_ID,
		   a.date AS Order_Date,
           a.cf_client_id AS Client_ID,
           a.contact_name AS Client_Name,
           upper(a.billing_city) AS Location,                        
           a.sku AS SKU,
           d.Product_Name,
           d.Brand,
           d.Size_Specification_SS AS Specification,
           a.quantity AS Qty,
           a.rate AS Selling_Price_Per_Unit,
           a.item_total AS Total_Sales,
           round(a.quantity * a.rate,2) AS Revenue,
           a.cf_supply_poc AS Category_POC,
           CASE 
			WHEN a.cf_client_type = 'Hospital : Non - Coporate' THEN 'Hospital : Non - Corporate'
            WHEN a.cf_client_type = 'Others' THEN 'Others'
            WHEN a.cf_client_type = 'Company or Aggregators' THEN 'Company or Aggregators'
            WHEN a.cf_client_type = 'Hospital : Coporate' THEN 'Hospital : Corporate'
            WHEN a.cf_client_type = 'Doctor' THEN 'Doctor'
            WHEN a.cf_client_type = 'Diagnostic/Pathology Lab' THEN 'Diagnostic/Pathology Lab'
            WHEN a.cf_client_type = 'Day Care' THEN 'Day Care'
            WHEN a.cf_client_type = 'Pharmacy' THEN 'Pharmacy'
            WHEN a.cf_client_type = 'Hospital : Non - Corporate' THEN 'Hospital : Non - Corporate'
            WHEN a.cf_client_type = 'Hospital : Corporate' THEN 'Hospital : Corporate'
            ELSE 'Others'
            END AS Client_Type,
            a.cf_inside_sales_poc_name_unformatted AS "Inside Sales POC",
            a.cf_lead_source AS Client_Source,
            CASE WHEN a.status IN ('fulfilled', 'confirmed', 'onhold', 'shipped', 'partially_shipped','Open','overdue',
                                   'invoiced','partially_invoiced'       ) 
		    THEN 'Confirmed'
            WHEN a.status = 'void' THEN 'Cancelled'
            WHEN a.status = 'draft' THEN 'Pending'
	        ELSE '' END  AS Final_Status,
            d.Product_Category,
            a.salesperson_name AS Sales_POC,
            concat(a.reference_number,a.sku) AS helper_so,
            a.tax_percentage AS GST_per,
            f.PO_Rate,
            g.serial_batch_rate,
            e.bcy_rate,
            i.quantity AS returned_quantity,
            i.rate AS return_rate,
            i.item_total AS return_item_total,
            mobile
                                  from (Select x.reference_number,
                                               x.date,
                                               y.sku,
                                               y.quantity-y.quantity_cancelled AS quantity,
                                               y.rate,
                                               y.item_total,
                                               x.cf_supply_poc,
                                               x.cf_inside_sales_poc_name_unformatted,
                                               x.status,
                                               x.salesperson_name,
                                               y.tax_percentage,
                                               x.salesorder_number,
                                               x.cf_order_sample,
                                               x.salesorder_id,
                                               x.customer_id, 
                                               c.cf_client_id,
                                               c.contact_name,
                                               c.billing_city,
                                               c.cf_client_type,
                                               c.cf_lead_source,
                                               c.mobile
                                          from zoho_db_new.sales_orders x
                                          right join zoho_db_new.sales_orders_line_items y
                                          on x.salesorder_id=y.salesorder_id                   
                                          left join zoho_db_new.client c
                                          on x.customer_id=c.client_id                  
                                          
                                          union all
    
                                         Select  x.reference_number,
                                                 x.date,
                                                 y.sku,
                                                 y.quantity,
                                                 y.rate,
                                                 y.item_total,									
                                                 x.cf_supply_poc,
                                                 x.cf_inside_sales_poc_name_unformatted,
                                                 x.status,
                                                 x.salesperson_name,
                                                 y.tax_percentage,
                                                 x.salesorder_number,
                                                 x.cf_order_sample,
                                                 x.salesorder_id,
                                                 x.customer_id,
                                                 c.cf_client_id,
                                                 c.contact_name,
                                                 c.billing_city,
                                                 c.cf_client_type,
                                                 c.cf_lead_source,
                                                 c.mobile
                                      from zoho_db.sales_orders x
                                      right join zoho_db.sales_orders_line_items y
                                      on x.salesorder_id=y.salesorder_id                   
                                      left join zoho_db.client c
                                      on x.customer_id=c.client_id
		            WHERE x.reference_number NOT IN (SELECT z.reference_number FROM zoho_db_new.sales_orders z)
                                       )a
                    
                           left join product_db.sku d
						   on a.sku=d.SKU
                    
					left join (SELECT  distinct SKU, 
									   avg(bcy_rate) AS bcy_rate
					           FROM (select SKU, bcy_rate from zoho_db_new.bills_line_items
									 union all 
                                     select SKU, bcy_rate from zoho_db.bills_line_items)az
							   GROUP BY SKU) e
						       on a.sku=e.sku
                                        
                                        
                    left join (SELECT CONCAT(reference_number, sku) AS Helper,avg(rate) AS PO_Rate
                                       FROM 
                              (select reference_number, sku, rate, status
                                       from  zoho_db_new.purchase_order x
                                       RIGHT JOIN zoho_db_new.purchase_order_line_item y 
                                       ON x.purchaseorder_id = y.purchaseorder_id
							union all
                            select reference_number, sku, rate, status
                                       from  zoho_db.purchase_order x
                                       RIGHT JOIN zoho_db.purchase_order_line_item y 
                                       ON x.purchaseorder_id = y.purchaseorder_id)m
                                       WHERE status <> 'draft'
                                       GROUP BY CONCAT(reference_number, sku) )f
                                      on concat(a.reference_number,a.sku)=f.helper
                                      
                                      
                    left join (select distinct distinct helper , 
                                      avg(serial_batch_rate) AS serial_batch_rate
                                from  (Select y.sku,
                                              y.salesorder_id,
                                              y.serial_batch,
                                              x.bcy_rate AS serial_batch_rate, 
                                              concat(y.sku,y.salesorder_id) AS helper
                                         FROM 
											(SELECT sku,
                                                    b.salesorder_id,
                                                    CASE WHEN batch_number IS NOT NULL THEN batch_number 
                                                    ELSE serial_numbers 
                                                    END AS serial_batch
                                               FROM zoho_db_new.packages_line_item b
                                               LEFT JOIN zoho_db_new.packages_lineitem_batches c 
                                               ON b.line_item_id = c.line_item_id 
                                               
                                               union all 

											SELECT sku,
                                                    b.salesorder_id,
                                                    CASE WHEN batch_number IS NOT NULL THEN batch_number 
                                                    ELSE serial_numbers 
                                                    END AS serial_batch
                                               FROM zoho_db.packages_line_item b
                                               LEFT JOIN zoho_db.packages_lineitem_batches c 
                                               ON b.line_item_id = c.line_item_id) y
                                               
                                               
                                         LEFT JOIN (SELECT sku,
                                                           bcy_rate,
                                                           CASE WHEN batch_number IS NOT NULL THEN batch_number 
                                                           ELSE serial_numbers 
                                                           END AS serial_batch
                                                    FROM zoho_db_new.bills_line_items ab
                                                    LEFT JOIN zoho_db_new.bills_lineitem_batches ac 
                                                    ON ab.line_item_id = ac.line_item_id
                                                    union all
                                                    SELECT sku,
                                                           bcy_rate,
                                                           CASE WHEN batch_number IS NOT NULL THEN batch_number 
                                                           ELSE serial_numbers 
                                                           END AS serial_batch
                                                    FROM zoho_db.bills_line_items ab
                                                    LEFT JOIN zoho_db.bills_lineitem_batches ac
                                                    ON ab.line_item_id = ac.line_item_id) x 
                                        ON CONCAT(x.sku, x.serial_batch) = CONCAT(y.sku, y.serial_batch)
                                             where x.bcy_rate is not null)k
                                             group by  helper )g
                                        on concat(a.sku,a.salesorder_id)=concat(g.helper)
        
        
                                    left join(select rate,
                                                     quantity,
                                                     item_total,
                                                     concat(sku,salesorder_id) AS helper2
                                              from zoho_db_new.salesreturns_line_item bt
                                              right join zoho_db_new.salesreturns at
                                              on at.salesreturn_id =bt.salesreturn_id
                                 where salesreturn_status in ('approved','closed')
                                 
                                 union all
                                 select rate,
                                                     quantity,
                                                     item_total,
                                                     concat(sku,salesorder_id) AS helper2
                                              from zoho_db.salesreturns_line_item bt
                                              right join zoho_db.salesreturns at
                                              on at.salesreturn_id =bt.salesreturn_id
                                 where salesreturn_status in ('approved','closed')
                                  
                                  )i
                                 on concat(a.sku,a.salesorder_id)=i.helper2
                                where a.cf_order_sample='Order Raised' and a.date>='2023-04-29'
                            
        
                    
                    ''')

my_result = my_cursor.fetchall()

print("Order SQL Query Excecuted", datetime.now())


# Convert MySQL result to a DataFrame
header = [i[0] for i in my_cursor.description]
df_mysql = pd.DataFrame(my_result, columns=header)

# Fetch data from the Google Sheets directly into DataFrames
hardpaste = pd.DataFrame(worksheet2.get_all_records())
hardpaste = hardpaste.head(2500)

hardpaste.drop_duplicates(subset=['Helper'], inplace=True)


# Creating hardpaste cost price
df_mysql= df_mysql.merge(hardpaste[['Helper', 'Cost']],
                                   left_on='helper_so', right_on='Helper', how='left')

df_mysql.drop(columns=['Helper'], inplace=True)

# Fetch data from the Google Sheets directly into DataFrames
pricing_list = pd.DataFrame(worksheet12.get_all_records())
pricing_list = pricing_list.head(3000)

# print("pricing list head ", pricing_list)

pricing_list.drop_duplicates(subset=['SKU'], inplace=True)

pricing_list.rename(columns={'Cost Price per Unit (Excluding GST)': 'rate_pricing_list'}, inplace=True)
pricing_list.rename(columns={'SKU': 'SKUP'}, inplace=True)

# Creating pricing list cost price
df_mysql= df_mysql.merge(pricing_list[['SKUP', 'rate_pricing_list']],
                                   left_on='SKU', right_on='SKUP', how='left')

df_mysql.drop(columns=['SKUP'], inplace=True)




priority_order = ['PO_Rate', 'serial_batch_rate', 'Cost', 'bcy_rate', 'rate_pricing_list']
df_mysql['cost_per_unit'] = df_mysql.apply(lambda row: next((row[col] for col in priority_order if not pd.isnull(row[col])), None), axis=1)


# Creating COGS
df_mysql['cost_per_unit'] = pd.to_numeric(df_mysql['cost_per_unit'], errors='coerce')

df_mysql['Qty'] = df_mysql['Qty'].astype(float)
df_mysql['cost_per_unit'] = df_mysql['cost_per_unit'].astype(float)
df_mysql['COGS'] = df_mysql['cost_per_unit'] * df_mysql['Qty']

# Creating Tentative Gross Margin
df_mysql['Tentative Gross Margin'] = df_mysql['Revenue'] - df_mysql['COGS']

# Creating Tentative Margin %
df_mysql['Tentative Margin %'] = np.where(
    df_mysql['Tentative Gross Margin'].notnull(),
    (df_mysql['Tentative Gross Margin'] * 100 / df_mysql['Revenue']).round(2).astype(str) + "%",
    ''  
)

# Fetch data from the Google Sheets directly into DataFrames
BD = pd.DataFrame(worksheet3.get_all_records())
BD = BD.head(40)

df_mysql= df_mysql.merge(BD[['BD Name', 'Inside Sales Flag']],
                                   left_on='Sales_POC', right_on='BD Name', how='left')



# Fetch data from the Google Sheets directly into DataFrames
orders_received = pd.DataFrame(worksheet4.get_all_records())
orders_received = orders_received.head(2000)

orders_received.drop_duplicates(subset=['Order ID'], inplace=True)
orders_received['Order ID'] = orders_received['Order ID'].astype(str).str.strip()
                 

df_mysql['Order_ID'] = df_mysql['Order_ID'].astype(str)
orders_received['Order ID'] = orders_received['Order ID'].astype(str)

# df_mysql= df_mysql.merge(orders_received[['Order ID', 'Inside Sales POC']],
#                                    left_on='Order_ID', right_on='Order ID', how='left')

# df_mysql.drop(columns=['Order ID'], inplace=True)


# Creating Inside Sales POC

df_mysql['Inside_Sales_POC'] = np.where(
    df_mysql['Inside Sales Flag'] == 1,
    df_mysql['Sales_POC'],  
    df_mysql['Inside Sales POC'] 
)


#Creating Net quantity and revenue
df_mysql['Qty'] = pd.to_numeric(df_mysql['Qty'], errors='coerce')
df_mysql['returned_quantity'] = pd.to_numeric(df_mysql['returned_quantity'], errors='coerce')
df_mysql['Net_Quantity'] = df_mysql['Qty'].fillna(0).astype(int) - df_mysql['returned_quantity'].fillna(0).astype(int)

df_mysql['Total_Sales'] = pd.to_numeric(df_mysql['Total_Sales'], errors='coerce')
df_mysql['return_item_total'] = pd.to_numeric(df_mysql['return_item_total'], errors='coerce')
df_mysql['Net_Revenue'] = df_mysql['Total_Sales'].fillna(0).astype(int) - df_mysql['return_item_total'].fillna(0).astype(int)


#Updating Final status
def update_final_status(row):
    if row['Final_Status'] in ('Confirmed','Fulfilled'):
        if row['returned_quantity'] > 0:
            if row['Net_Quantity'] == 0:
                return 'Returned'
            else:
                return 'Partially Returned'
        if row['Net_Quantity'] == 0:
            return 'Cancelled'
        return row['Final_Status']

# Apply the function to update the 'Final_Status' column for the filtered DataFrame
df_mysql['Final_Status'] = df_mysql.apply(update_final_status, axis=1)


###

# Fetch data from the Google Sheets directly into DataFrames
New_Order_Status = pd.DataFrame(worksheet15.get_all_records())
New_Order_Status = New_Order_Status.head(20)

New_Order_Status['Order_ID'] = New_Order_Status['Order_ID'].astype(str)

# Merge df_mysql with New_Order_Status on Order_ID
merged_df = df_mysql.merge(New_Order_Status, on='Order_ID', how='left')

# Update Final_Status in df_mysql with NewStatus if NewStatus is not NaN
merged_df['Final_Status'] = merged_df['NewStatus'].combine_first(merged_df['Final_Status'])

# Drop the NewStatus column as it is no longer needed
merged_df = merged_df.drop(columns=['NewStatus'])

# Assign the updated DataFrame back to df_mysql
df_mysql = merged_df

# ###



# Creating GMV Flag
df_mysql['GMV Counting Flag'] = np.where(
    (df_mysql['Final_Status'] == "Cancelled") | 
    (df_mysql['Final_Status'] == "Returned") | 
    (df_mysql['Final_Status'] == "Pending") |
    (df_mysql['Final_Status'] == "Foot in the Door"),
    0, 
    1   
    
)

# Fetch data from the Google Sheets directly into DataFrames
zone = pd.DataFrame(worksheet5.get_all_records())
zone = zone.head(2000)



# Creating Zone
df_mysql['Location'] = df_mysql['Location'].astype(str)
zone['Billing City'] = zone['Billing City'].astype(str)

zone.drop_duplicates(subset=['Billing City'], inplace=True)

df_mysql= df_mysql.merge(zone[['Billing City', 'Zone']],
                                   left_on='Location', right_on='Billing City', how='left')

# Fetch data from the Google Sheets 
order_date = pd.DataFrame(worksheet6.get_all_records())
order_date = order_date.head(2000)

# Creating Order Date
order_date['Order ID'] = order_date['Order ID'].astype(str)
order_date.drop_duplicates(subset=['Order ID'], inplace=True)

df_mysql= df_mysql.merge(order_date[['Order ID', 'Order Confirmation Date']],
                                   left_on='Order_ID', right_on='Order ID', how='left')

df_mysql.drop(columns=['Order ID'], inplace=True)

# Creating Revenue (including GST)
df_mysql['GST_per1'] = df_mysql['GST_per'].str.replace('%', '')
df_mysql['GST_per1'] = pd.to_numeric(df_mysql['GST_per1'], errors='coerce')
df_mysql['Revenue1'] = pd.to_numeric(df_mysql['Revenue'], errors='coerce')
df_mysql['Revenue (including GST)'] = df_mysql['Revenue1'] * (1 + (df_mysql['GST_per1'] / 100))

df_mysql.drop(columns=['GST_per1'], inplace=True)
df_mysql.drop(columns=['Revenue1'], inplace=True)


# Creating Order / Sample Flag
df_mysql= df_mysql.merge(orders_received[['Order ID', 'Order / Sample Flag']],
                                   left_on='Order_ID', right_on='Order ID', how='left')


df_mysql.drop(columns=['Order ID'], inplace=True)



# Creating Confirmed Flag (Zoho)
df_mysql['Confirmed Flag (Zoho)'] = df_mysql['GMV Counting Flag'].apply(lambda x: 1 if x == 1 else 0)


# Creating Confirmed Flag (Zoho)
df_mysql['Confirmed Flag (Zoho)'] = df_mysql['GMV Counting Flag'].apply(lambda x: 1 if x == 1 else 0)



# Creating Invoice Flag

# Fetch data from the Google Sheets 
old_invoice = pd.DataFrame(worksheet10.get_all_records())
old_invoice = old_invoice.head(1500)
old_invoice['Enter Order ID'] = old_invoice['Enter Order ID'].astype(str)

old_invoice.drop_duplicates(subset=['Enter Order ID'], inplace=True)

#Creating Old Invoice Flag
df_mysql= df_mysql.merge(old_invoice[['Enter Order ID', 'Flag']],
                                   left_on='Order_ID', right_on='Enter Order ID', how='left')

df_mysql.drop(columns=['Enter Order ID'], inplace=True)

# Fetch data from the Google Sheets 
invoice = pd.DataFrame(worksheet11.get_all_records())
invoice = invoice.head(2300)


my_cursor = my_conn.cursor()
my_cursor.execute(''' SELECT concat(reference_number,sku) AS Helper, 
       1 AS Flag
FROM zoho_db_new.invoices_line_item a
LEFT JOIN zoho_db_new.invoices b
ON a.invoice_id = b.invoice_id
WHERE status <> 'draft' 
AND sku IS NOT NULL 
AND sku <> ""  ''')

my_result1 = my_cursor.fetchall()


# Convert MySQL result to a DataFrame
header = [i[0] for i in my_cursor.description]
invsql = pd.DataFrame(my_result1, columns=header)


invoice = pd.concat([invoice, invsql], ignore_index=True)

invoice.drop_duplicates(subset=['helper'], inplace=True)

df_mysql['OrderSKU'] = df_mysql['Order_ID'].astype(str)  + df_mysql['SKU'].astype(str)

#Creating invoice Flag
df_mysql= df_mysql.merge(invoice[['helper', 'Invoiced Flag']],
                                   left_on='OrderSKU', right_on='helper', how='left')

df_mysql.drop(columns=['OrderSKU'], inplace=True)
df_mysql.drop(columns=['helper'], inplace=True)

df_mysql['invoice Flag (Zoho)'] = df_mysql['Invoiced Flag'].fillna(df_mysql['Flag'])

df_mysql.drop(columns=['Invoiced Flag'], inplace=True)
df_mysql.drop(columns=['Flag'], inplace=True)


# Creating Delivery Flag
 

# Fetch data from the Google Sheets 
old_delivery = pd.DataFrame(worksheet8.get_all_records())
old_delivery = old_delivery.head(200)
old_delivery['Order ID'] = old_delivery['Order ID'].astype(str)

old_delivery.drop_duplicates(subset=['Order ID'], inplace=True)

#Creating Old Delivery Flag
df_mysql= df_mysql.merge(old_delivery[['Order ID', 'Order Delivered Flag']],
                                   left_on='Order_ID', right_on='Order ID', how='left')

df_mysql.drop(columns=['Order ID'], inplace=True)


# Fetch data from the Google Sheets 
delivery = pd.DataFrame(worksheet9.get_all_records())
delivery = delivery.head(3000)

delivery.drop_duplicates(subset=['helper'], inplace=True)

df_mysql['OrderSKU'] = df_mysql['Order_ID'].astype(str)  + df_mysql['SKU'].astype(str)

#Creating Delivery Flag
df_mysql= df_mysql.merge(delivery[['helper', 'Delivered Flag']],
                                   left_on='OrderSKU', right_on='helper', how='left')

df_mysql.drop(columns=['OrderSKU'], inplace=True)
df_mysql.drop(columns=['helper'], inplace=True)

df_mysql['Delivery Flag (Zoho)'] = df_mysql['Delivered Flag'].fillna(df_mysql['Order Delivered Flag'])

df_mysql.drop(columns=['Delivered Flag'], inplace=True)
df_mysql.drop(columns=['Order Delivered Flag'], inplace=True)


# Creating Order Fulfillment Flag

df_mysql['invoice Flag (Zoho)'] = pd.to_numeric(df_mysql['invoice Flag (Zoho)'], errors='coerce')
df_mysql['Delivery Flag (Zoho)'] = pd.to_numeric(df_mysql['Delivery Flag (Zoho)'], errors='coerce')

df_mysql['Order Fulfillment Flag'] = df_mysql['invoice Flag (Zoho)'] * df_mysql['Delivery Flag (Zoho)']

# Creating Opp ID
df_mysql= df_mysql.merge(orders_received[['Order ID', 'Opportunity ID']],
                                   left_on='Order_ID', right_on='Order ID', how='left')

df_mysql.drop(columns=['Order ID'], inplace=True)

df_mysql['Final_Status'] = np.where(df_mysql['Order Fulfillment Flag'] == 1, 'Fulfilled', df_mysql['Final_Status'])




columns_to_keep=['Order_ID',
                 'Order_Date',
                 'Client_ID',
                 'Client_Name',
                 'Location',
                 'SKU',
                 'Product_Name',
                 'Brand',
                 'Specification',
                 'Qty',
                 'Selling_Price_Per_Unit',
                 'Total_Sales',
                 'Revenue',
                 'cost_per_unit',
                 'COGS',
                 'Tentative Gross Margin',
                 'Tentative Margin %',
                 'Category_POC',
                 'Sales_POC',
                 'Inside_Sales_POC',
                 'Final_Status',
                 'GMV Counting Flag',
                 'GST_per',
                 'Client_Type',
                 'Product_Category',
                 'Client_Source',
                 'Zone' ,
                 'Order Confirmation Date',
                 'Revenue (including GST)',
                 'Order / Sample Flag',
                 'Confirmed Flag (Zoho)',
                 'invoice Flag (Zoho)',
                 'Delivery Flag (Zoho)',
                 'Order Fulfillment Flag',
                 'Opportunity ID',                 
                'returned_quantity',
                'return_rate',
                 'return_item_total',
                 'Net_Quantity',
                 'Net_Revenue'
                ]

new_df = df_mysql[columns_to_keep].copy()

# Fetch data from the Google Sheets directly into DataFrames
old_order = pd.DataFrame(worksheet7.get_all_records())
old_order = old_order.head(403)

old_order['Order_ID'] = old_order['Order_ID'].astype(str)

                 
# Append Old & New Orders                 
df_mysql = pd.concat([old_order, new_df], ignore_index=True)


# Creating Client Data for date & repeat client
columns_to_keep = ['Order_ID', 'Client_ID', 'GMV Counting Flag', 'Order Confirmation Date']
filtered_df = df_mysql[df_mysql['GMV Counting Flag'] == 1]
client_data = filtered_df[columns_to_keep].copy()
client_data.drop_duplicates(subset=['Order_ID'], inplace=True)
client_data['Order_ID'] = client_data['Order_ID'].str.strip()

df_mysql.drop(columns=['Order Confirmation Date'], inplace=True)

# Create Client type for specific Order ID
client_data['Client_Type1'] = ''

# Iterate over the DataFrame rows
for index, row in client_data.iterrows():
    current_client_id = row['Client_ID']
    if current_client_id in client_data.loc[:index-1, 'Client_ID'].values:
        client_data.at[index, 'Client_Type1'] = 'Repeat Client'
    else:
        client_data.at[index, 'Client_Type1'] = 'New Client'
        
        
# Creating First & Last Order Date
client_data['Order Confirmation Date'] = pd.to_datetime(client_data['Order Confirmation Date'], format='%d/%m/%Y')
client_data['First_Order_Date'] = client_data.groupby('Client_ID')['Order Confirmation Date'].transform('min')
client_data['Last_Order_Date'] = client_data.groupby('Client_ID')['Order Confirmation Date'].transform('max')




df_mysql= df_mysql.merge(client_data[['Order_ID', 'Client_Type1','First_Order_Date','Last_Order_Date','Order Confirmation Date']],
                                   left_on='Order_ID', right_on='Order_ID', how='left')

df_mysql['Order Confirmation Date'] = pd.to_datetime(df_mysql['Order Confirmation Date']).dt.date
df_mysql['First_Order_Date'] = pd.to_datetime(df_mysql['First_Order_Date']).dt.date
df_mysql['Last_Order_Date'] = pd.to_datetime(df_mysql['Last_Order_Date']).dt.date

df_mysql = df_mysql.sort_values(by='Order_ID')

df_mysql['Order_ID'] = pd.to_numeric(df_mysql['Order_ID'], errors='coerce')

df_mysql = df_mysql[df_mysql['Order_ID'].notnull() & (df_mysql['Order_ID'] > 100000)]


desired_order = [
    'Order_ID', 'Order_Date', 'Client_ID', 'Client_Name', 'Location', 'SKU', 'Product_Name',
    'Brand', 'Specification', 'Net_Quantity', 'Selling_Price_Per_Unit', 'Net_Revenue', 'Net_Revenue',
    'cost_per_unit', 'COGS', 'Tentative Gross Margin', 'Tentative Margin %', 'Category_POC',
    'Sales_POC', 'Inside_Sales_POC', 'Final_Status', 'GMV Counting Flag', 'GST_per',
    'Client_Type', 'Product_Category', 'Client_Source', 'Zone', 'Client_Type1',
    'First_Order_Date', 'Last_Order_Date', 'Order Confirmation Date', 'Revenue (including GST)',
    'Order / Sample Flag', 'Confirmed Flag (Zoho)', 'invoice Flag (Zoho)', 'Delivery Flag (Zoho)',
    'Order Fulfillment Flag', 'Opportunity ID', 'returned_quantity', 'return_rate',
    'return_item_total', 'Qty', 'Total_Sales'
]

# Rearrange columns in the DataFrame
df_mysql = df_mysql[desired_order].drop_duplicates()


#Clear Worksheet
range_to_clear_sheet = 'A:AL'
worksheetP.batch_clear([range_to_clear_sheet])
#updating worksheet with data
gsdf.set_with_dataframe(worksheetP, df_mysql )


#Clear Worksheet
range_to_clear_sheet = 'A:AL'
worksheetX.batch_clear([range_to_clear_sheet])
#updating worksheet with data
gsdf.set_with_dataframe(worksheetX, df_mysql )




#Clear Worksheet
range_to_clear_sheet = 'A:AL'
worksheetV.batch_clear([range_to_clear_sheet])
#updating worksheet with data
gsdf.set_with_dataframe(worksheetV, df_mysql )


#Clear Worksheet
range_to_clear_sheet = 'A:AL'
worksheet13.batch_clear([range_to_clear_sheet])
#updating worksheet with data
gsdf.set_with_dataframe(worksheet13, df_mysql )



#Clear Worksheet
range_to_clear_sheet = 'A:AL'
worksheetY.batch_clear([range_to_clear_sheet])
#updating worksheet with data
gsdf.set_with_dataframe(worksheetY, df_mysql )


#Clear Worksheet
range_to_clear_sheet = 'A:AL'
worksheetC.batch_clear([range_to_clear_sheet])
#updating worksheet with data
gsdf.set_with_dataframe(worksheetC, df_mysql )





my_cursor = my_conn.cursor()
my_cursor.execute('''select reference_number AS OrderID,
                            adjustment AS discount
                    from zoho_db_new.sales_orders
                    where adjustment<>0.0
                     ''')

my_result1 = my_cursor.fetchall()

# Convert MySQL result to a DataFrame
header = [i[0] for i in my_cursor.description]
df_mysql1 = pd.DataFrame(my_result1, columns=header)


#Clear Worksheet
range_to_clear_sheet = 'A:B'
worksheet16.batch_clear([range_to_clear_sheet])
#updating worksheet with data
gsdf.set_with_dataframe(worksheet16, df_mysql1)



# Close MySQL connection
my_conn.close()






