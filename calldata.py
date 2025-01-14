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

#Load credentials from the JSON key file
#credentials = Credentials.from_service_account_file('datafromdatabase-556db248ebb2.json',
#                                                    scopes=['https://www.googleapis.com/auth/spreadsheets'])



# # Load credentials from the JSON key file
credentials = Credentials.from_service_account_file('/home/ubuntu/simran/datafromdatabase-556db248ebb2.json',
                                                    scopes=['https://www.googleapis.com/auth/spreadsheets'])



# Authorize the client
gc = gspread.authorize(credentials)


# Open a Google Sheets spreadsheet by key (new wip)
spreadsheet_key = '1q5IBvFJOA5GzXuFPQ2vx1ULTKOwdmzgScWHXVT6svvo'
worksheetP = gc.open_by_key(spreadsheet_key).worksheet('Call')


# Connect to MySQL database
my_conn = mysql.connector.connect(
                                    host='3.109.191.111',
                                    user='analytics_user',
                                    password='Analytics@sql@32',
                                    db='lsq_db',
                                  )

print("Connection Query Excecuted", datetime.now())


# In[5]:


my_cursor = my_conn.cursor()
my_cursor.execute(''' WITH  
                          Inbound AS (Select Phone,
                                             RelatedProspectId,
                                             status,
                                             FirstName,
                                             min(CreatedOn) AS CreatedOn                                            
                                        from(
                                              SELECT  i.ID, 
                                              CONVERT_TZ(STR_TO_DATE(i.CreatedOnString, '%Y-%m-%d %H:%i:%s'), '+00:00','+05:30') AS CreatedOn,
                                              i.Name, 
                                              i.Status, 
                                              i.`Call Duration`, 
                                              i.CreatedByName, 
                                              CONVERT_TZ(i.ModifiedOn, '+00:00','+05:30') AS ModifiedOn, 
                                              l.FirstName, 
                                              CONVERT(l.phone, CHAR) AS Phone,
                                              i.RelatedProspectId,
                                              DATE(CONVERT_TZ(STR_TO_DATE(i.CreatedOnString, '%Y-%m-%d %H:%i:%s'), '+00:00','+05:30')) AS Date,
                                              ROW_NUMBER() OVER (
                                                                  PARTITION BY i.RelatedProspectId
                                                                  ORDER BY CONVERT_TZ(STR_TO_DATE(i.CreatedOnString, '%Y-%m-%d %H:%i:%s'), 
                                                                                                  '+00:00', '+05:30') DESC
                                                                  ) AS rn
                                        FROM 
                                            lsq_db.activity_phone_call_inbound i
                                        LEFT JOIN 
                                                  lsq_db.leads l ON i.RelatedProspectId = l.ProspectID
                                        WHERE 
                                               STR_TO_DATE(i.CreatedOnString, '%Y-%m-%d %H:%i:%s') >= DATE('2024-04-01'))a
                                       where rn=1
                                       group by Phone,RelatedProspectId,status,FirstName,Date) ,

                            Lead_Owner AS  (Select b.ProspectID, 
                                                   b.OwnerIdName,
                                                   b.Phone, 
                                                   b.Allocated_Time As FromTime ,
                                                   COALESCE( LEAD(b.Allocated_Time,1) OVER (partition by  b.ProspectID ORDER BY b.Allocated_Time asc), 
                                                              CONVERT_TZ(NOW() , '+00:00','+05:30') ) AS ToTime
                                             FROM( SELECT  a.ProspectID,
                                                           a.OwnerIdName,
                                                           a.Phone,
                                                           min(DATE_SUB(CONVERT_TZ(ModifiedOn, '+00:00','+05:30'), INTERVAL 10 MINUTE)) AS Allocated_Time 
                                                  FROM lsq_db.leads_data a
                                                   group by a.ProspectID, a.OwnerIdName,a.Phone ) b) ,
                     
                            Outboound_Call AS (SELECT o.ID, 
                                                       CONVERT_TZ(STR_TO_DATE(o.CreatedOnString, '%Y-%m-%d %H:%i:%s'), '+00:00','+05:30') AS OutboundTime,
                                                      o.Status, 
                                                      o.CreatedByName,
                                                      o.RelatedProspectId
                                               FROM lsq_db.activity_phone_call_outbound o)    
    
                               SELECT 
                                       right(x.Phone,10) AS Phone,
                                       x.CreatedOn AS Inbound_Call_Time,
                                       x.status AS Call_Status,
                                       x.FirstName,
                                       x.RelatedProspectId AS ProspectId,
                                       y.OwnerIdName AS LeadOwner,
                                       z.ID AS OutBoundCallID,
                                       z.OutboundTime,
                                       z.Status AS Outbound_Call_Status,
                                       z.CreatedByName AS OutBoundCallDoneBy
                                                
                                               FROM   Inbound x
                                               LEFT JOIN Lead_Owner  y
                                                       on x.RelatedProspectId = y.ProspectID
                                                       and x.CreatedOn>=y.FromTime
                                                       and x.CreatedOn<=y.ToTime
                                             LEFT JOIN Outboound_Call z
                                                       ON x.RelatedProspectId = z.RelatedProspectId
                                                       AND x.CreatedOn <= z.OutboundTime
 #                                                      AND DATE_ADD(x.CreatedOn, INTERVAL 8 HOUR) >= z.OutboundTime
                                            where x.Phone <> "+91-7942554266" 
                                                        ''')




my_result = my_cursor.fetchall()

print("Order SQL Query Excecuted", datetime.now())


# Convert MySQL result to a DataFrame
header = [i[0] for i in my_cursor.description]
df_mysql = pd.DataFrame(my_result, columns=header)

# print(df_mysql)





#Close MySQL connection
my_conn.close()


# In[7]:


#Clear Worksheet
range_to_clear_sheet = 'A:M'
worksheetP.batch_clear([range_to_clear_sheet])
#updating worksheet with data
gsdf.set_with_dataframe(worksheetP, df_mysql )


# In[ ]:




