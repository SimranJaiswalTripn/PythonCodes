#!/usr/bin/env python
# coding: utf-8

# In[12]:


import gspread
import gspread_dataframe as gsdf
from google.oauth2.service_account import Credentials
import mysql.connector
from datetime import datetime, date
import pandas as pd
import numpy as np

#Load credentials from the JSON key file
# credentials = Credentials.from_service_account_file('datafromdatabase-556db248ebb2.json',
#                                                     scopes=['https://www.googleapis.com/auth/spreadsheets'])



# Load credentials from the JSON key file
credentials = Credentials.from_service_account_file('/home/ubuntu/simran/datafromdatabase-556db248ebb2.json',
                                                    scopes=['https://www.googleapis.com/auth/spreadsheets'])



# Authorize the client
gc = gspread.authorize(credentials)


# Open a Google Sheets spreadsheet by key (new wip)
spreadsheet_key = '1vIQqGt6KU2Pwdnbnzl9X0mXEZkWcaXgnqr6YzRAbGag'
worksheetP = gc.open_by_key(spreadsheet_key).worksheet('Calling_Master')


# Connect to MySQL database
my_conn = mysql.connector.connect(
                                    host='3.109.191.111',
                                    user='analytics_user',
                                    password='Analytics@sql@32',
                                    db='lsq_db',
                                  )

print("Connection Query Excecuted", datetime.now())


# In[13]:


my_cursor = my_conn.cursor()
my_cursor.execute('''  WITH combined_calls AS (
                         SELECT  RelatedProspectId,
								 CONVERT_TZ(STR_TO_DATE(CreatedOnString, '%Y-%m-%d %H:%i:%s'), '+00:00','+05:30') AS CallTime,
                                 `Call Duration`,
                                 Status
                           FROM  lsq_db.activity_phone_call_inbound
                           UNION ALL
                           SELECT  RelatedProspectId,
								   CONVERT_TZ(STR_TO_DATE(CreatedOnString, '%Y-%m-%d %H:%i:%s'), '+00:00','+05:30') AS CallTime,
                                  `Call Duration`,
                                   Status
						 FROM  lsq_db.activity_phone_call_outbound),
ranked_calls AS (
    SELECT 
        RelatedProspectId,
        CallTime,
        `Call Duration`,
        ROW_NUMBER() OVER (PARTITION BY RelatedProspectId ORDER BY CallTime ASC) AS rn_first,
        ROW_NUMBER() OVER (PARTITION BY RelatedProspectId ORDER BY CallTime DESC) AS rn_last
    FROM 
        combined_calls
),
 
 FirstCall AS (SELECT RelatedProspectId,
                      CallTime AS FirstCallTime,
                     `Call Duration` AS FirstCallDuration
              FROM ranked_calls
              WHERE rn_first=1
),
     LastCall AS ( SELECT RelatedProspectId,
                      CallTime AS LastCallTime,
                     `Call Duration` AS LastCallDuration
              FROM ranked_calls
              WHERE rn_last=1
    ),
    TotalCall AS ( Select RelatedProspectId,
                          count(RelatedProspectId) AS TotalCall,
                          Sum(`Call Duration`) As TotalCallDuration
					from combined_calls
                    group by RelatedProspectId
                          ),
	InboundCall AS ( Select RelatedProspectId,
                          count(RelatedProspectId) AS InboundCall,
                          Sum(`Call Duration`) As InboundCallDuration
					from lsq_db.activity_phone_call_inbound
                    group by RelatedProspectId
                          ),
	OutboundCall AS ( Select RelatedProspectId,
                          count(RelatedProspectId) AS OutboundCall,
                          Sum(`Call Duration`) As OutboundCallDuration
					from lsq_db.activity_phone_call_outbound
                    group by RelatedProspectId
                          ),
    ConnectedCall AS (select RelatedProspectId,
                          count(RelatedProspectId) AS ConnectedCall,
                          sum(`Call Duration`) AS ConnectedCallDuration
                    from combined_calls
                     where Status='Answered'
                     group by RelatedProspectId)
                          
SELECT date(CONVERT_TZ(a.CreatedOn, '+00:00','+05:30')) as CreatedOn,
       a.ProspectID,
       a.ProspectAutoId,
       a.FirstName,
       a.LastName,
       right(a.Phone,10)  AS Phone,
       a.Source,
       a.mx_Warmth,
       a.ProspectStage,
       a.OwnerIdName,
       a.mx_Meta_Ad_Name,
       b.TotalCall,
       (b.TotalCallDuration/60) AS TotalCallDuration ,
       c.InboundCall,
       (c.InboundCallDuration/60) AS InboundCallDuration,
       d.OutboundCall,
       (d.OutboundCallDuration/60) AS OutboundCallDuration,
       e.FirstCallTime,
       (e.FirstCallDuration/60) AS FirstCallDuration,
       f.LastCallTime,
       (f.LastCallDuration/60) AS LastCallDuration,
       g.ConnectedCall,
       (g.ConnectedCallDuration/60) AS ConnectedCallDuration
       
FROM lsq_db.leads a 
left join TotalCall b
on a.ProspectID=b.RelatedProspectId
left join InboundCall c
on a.ProspectID=c.RelatedProspectId
left join OutboundCall d
on a.ProspectID=d.RelatedProspectId
left join FirstCall e
on a.ProspectID=e.RelatedProspectId
left join LastCall f
on a.ProspectID=f.RelatedProspectId
left join ConnectedCall g
on a.ProspectID=g.RelatedProspectId

where a.CreatedOn >= '2024-01-01 00:00:00' ''')

my_result = my_cursor.fetchall()

print("Call SQL Query Excecuted", datetime.now())


# Convert MySQL result to a DataFrame
header = [i[0] for i in my_cursor.description]
df_mysql = pd.DataFrame(my_result, columns=header)

# print(df_mysql)





# #Close MySQL connection
# my_conn.close()

#Clear Worksheet
range_to_clear_sheet = 'A:V'
worksheetP.batch_clear([range_to_clear_sheet])
#updating worksheet with data
gsdf.set_with_dataframe(worksheetP, df_mysql )


# In[ ]:




