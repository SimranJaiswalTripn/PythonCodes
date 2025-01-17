#!/usr/bin/env python
# coding: utf-8

# In[31]:


import gspread
import gspread_dataframe as gsdf
from google.oauth2.service_account import Credentials
import mysql.connector
from datetime import datetime, date
import pandas as pd
import numpy as np

#Load credentials from the JSON key file
#credentials = Credentials.from_service_account_file('datafromdatabase-556db248ebb2.json',
#                                                     scopes=['https://www.googleapis.com/auth/spreadsheets'])


# Load credentials from the JSON key file
credentials = Credentials.from_service_account_file('/home/ubuntu/lsq/datafromdatabase-556db248ebb2.json',
                                                     scopes=['https://www.googleapis.com/auth/spreadsheets'])




# Authorize the client
gc = gspread.authorize(credentials)

# Open a Google Sheets spreadsheet by key
spreadsheet_key = '14AHw1iD6kaapYXAmDpYN8_BQkqaX-rIrFqeU2KhYLVM'
worksheet = gc.open_by_key(spreadsheet_key).worksheet('master_raw')
worksheetCall = gc.open_by_key(spreadsheet_key).worksheet('knowlarity (sorted)')



# Open a Google Sheets spreadsheet by key(Field Sale)
spreadsheet_key = '1c_tG4EZYUebGgsJdW3SESXZJI4xO-5b6UpEA1WzVEhk'
worksheetX = gc.open_by_key(spreadsheet_key).worksheet('master_raw')


# Open a Google Sheets spreadsheet by key(Field Sale)
spreadsheet_key = '1pG5mlEkD37-YolYhbieJl_DtwfaY5NhhQ3UozDL_-LA'
worksheetCat = gc.open_by_key(spreadsheet_key).worksheet('master_raw')

# Open a Google Sheets spreadsheet by key(Lead Data (Others))
spreadsheet_key = '1nDNwnl3CYtufoB-0ZIsmCW1TCNbtDJEXsnV8DSf_Ux4'
worksheetP = gc.open_by_key(spreadsheet_key).worksheet('master_raw')


# Open a Google Sheets spreadsheet by key(Lead Data (Team Karna))
spreadsheet_key = '1_ELNXU_EhuQ6x24aU2aH7nrLH9CrR6wYczfT48mkQFQ'
worksheetQ = gc.open_by_key(spreadsheet_key).worksheet('master_raw')


# Open a Google Sheets spreadsheet by key(Lead Data (Team Arjuna))
spreadsheet_key = '1jZLTE7T_9heiGzT2FupQ9AJWx-8pfHiWZ-81bNAeEDU'
worksheetR = gc.open_by_key(spreadsheet_key).worksheet('master_raw')


# Open a Google Sheets spreadsheet by key(Lead Data (Inside Sales))
spreadsheet_key = '1EXLQGE7VrOzWEwy9gsIF_xyY_PIU5HIMU96SitaDhC4'
worksheetS = gc.open_by_key(spreadsheet_key).worksheet('master_raw')

# Open a Google Sheets spreadsheet by key(Lead Data (Service Dashbaord))
spreadsheet_key = '1-s4xeIfCgR0pEG76TGvurZtDuj2iAXLPLjD4YZTEaFE'
worksheetSer = gc.open_by_key(spreadsheet_key).worksheet('master_raw')


# Open a Google Sheets spreadsheet by key (System Warmth)
spreadsheet_key = '1P9LHpC4TAJwprN6OvY9sfLNiEYRzkLN3CRnoNhqp_ZY'
worksheet_i = gc.open_by_key(spreadsheet_key).worksheet('i_master_raw')


# Open the Google Sheets spreadsheets by key and worksheet
spreadsheet_key_sheet2 = '1M9AcLDZ48o_0C4Rp3AszzkTu9NIOY972A1JeiKEic74'


worksheet_sheet2 = gc.open_by_key(spreadsheet_key_sheet2).worksheet('city&state')
worksheet_sheet3 = gc.open_by_key(spreadsheet_key_sheet2).worksheet('region validation')
worksheet_sheet4 = gc.open_by_key(spreadsheet_key_sheet2).worksheet('Campaign')
worksheet_sheet5 = gc.open_by_key(spreadsheet_key_sheet2).worksheet('BD Mapping')
worksheet_sheet6 = gc.open_by_key(spreadsheet_key_sheet2).worksheet('BD Flag')
worksheet_sheet7 = gc.open_by_key(spreadsheet_key_sheet2).worksheet('orders received')






# Connect to MySQL database
my_conn = mysql.connector.connect(
                                    host='3.109.191.111',
                                    user='analytics_user',
                                    password='Analytics@sql@32',
                                    db='lsq_db',
                                  )

print("Connection Query Excecuted", datetime.now())


# In[32]:


my_cursor = my_conn.cursor()
my_cursor.execute('''select a.*,
                     b.*,
                     c.opportunityId,
                     c.OppCreateDate,
                     c.OppWarmth,
                     c.OpportunityProductDetails,
                     c.CategoryOfProduct,
                     c.OppStage,
                     c.Next_Step,
                     c.OppStatus,
                     c.Demo_Outcome,
                     c.Demo_Cancelled_Reason,
                     c.Demo_Not_Interested_Reason,
                     c.Sample_Cancelled_Reason,
                     c.MeetingNot_Interested_Reason,
                     c.Next_Step__Quotation,
                     d.FollowUpLeadActivityId,
                     d.FollowUpLeadActivityDate,
                     d.FollowUpLeadActivityOutcome,
                     e.FirstContactActivityId,
                     e.FirstContactActivityDate,
                     f.FollowUpOppActivityId,
                     f.FollowUpOppActivityIdDate,
                     f.FollowUpOpportunityStage,
                     f.FollowUpOutcome,
                     f.FollowUpLostDisposition,
                     f.FollowUpOppActivityAddedBy,
                     g.QuotationSharedActivity,
                     g.QuotationSharedActivityDate,
                     g.QuotationSharedActivityDoneBy,
                     g.QuotationNotAcceptedReason,
                     h. MeetingScheduledID,
                     h.MeetingTaskScheduledDate,
                     h.MeetingContext,
                     h.MeetingScheduledActivityOwner,
                     i.meeting_ID,
                     i.MeetingDoneDate,
                     i.MeetingDoneOwner,
                     j.DemoRequestedDate,
                     k.DemoDoneDate,
                     TIMESTAMPDIFF(DAY, LastActivityDate,   CONVERT_TZ(Now(), '+00:00','+05:30')) AS LastActivityAging,
                     TIMESTAMPDIFF(HOUR, CreatedOn,  CONVERT_TZ(Now(), '+00:00','+05:30')) AS ProspectAging,
                     TIMESTAMPDIFF(HOUR, FirstContactActivityDate,  CONVERT_TZ(Now(), '+00:00','+05:30')) AS FirstContactAging,
                     TIMESTAMPDIFF(HOUR, OppCreateDate,  CONVERT_TZ(Now(), '+00:00','+05:30')) AS InterestedAging,
                     TIMESTAMPDIFF(HOUR, CreatedOn, FirstContactActivityDate) AS ProspectToFirstContact,
                     m.TotalTasksNotFirstContact,
                     m.TotalApplicableTasksNotFirstContact,
                     m.TotalTasksCompletedNotFirstContact,
                     m.TotalTaskCompletedOnTime,
                     TIMESTAMPDIFF(MINUTE, FirstContactTaskCreateTime, FirstContactActivityDate)/60.0 AS FirstContactTimeInHrs,
                     n.FirstOppDate,
                     q.Totalfollowup,
                     q.TotalFollowUpBeforeFirstContact,
                     FirstAct AS FirstContactActivityTime



                     from
                     (
                     SELECT ProspectID,
                     ProspectAutoId as LeadNumber,
                     CONVERT_TZ(LeadLastModifiedOn , '+00:00','+05:30') as RecentlyModifiedOn ,
                     OwnerIdName as Owner,
                     ModifiedByName as ModifiedBy,
                     FirstName as LeadName,
                     CONVERT(phone, CHAR) AS Phone ,
                     Source as LeadSource,
                     SourceCampaign as MetaCampaignName,
                     ProspectStage as LeadStage,
                     Score as LeadScore,
                     EngagementScore ,
                     CONVERT_TZ(ProspectActivityDate_Min, '+00:00','+05:30') as FirstActivityDate,
                     ProspectActivityName_Max as LastActivity,
                     CONVERT_TZ(ProspectActivityDate_Max, '+00:00','+05:30') as LastActivityDate,
                     CONVERT_TZ(CreatedOn, '+00:00','+05:30') as CreatedOn,
                     CONVERT_TZ(ModifiedOn, '+00:00','+05:30') as ModifiedOn,
                     mx_Product_Details as ProductDetails,
                     mx_Is_this_an_Urgent_Query,
                     CONVERT_TZ(mx_Follow_Up_Date, '+00:00','+05:30') as Follow_Up_Date,
                     mx_POC_Designation,
                     mx_New_State,
                     mx_New_City,
                     CONVERT_TZ(mx_Meeting_Date_and_Time, '+00:00','+05:30') as Meeting_Date_and_Time,
                     mx_Context_for_Meeting,
                     mx_Warmth,
                     mx_Call_Outcome,
                     mx_Not_Interested,
                     mx_Category_of_Product,
                     mx_Requirement_Gathering_Meeting,
                     mx_Meeting_Not_Interested_Reason,
                     CASE 
                           WHEN date(mx_Old_Lead_Date) = '2024-07-20' THEN NULL
                           ELSE CONVERT_TZ(mx_Old_Lead_Date, '+00:00', '+05:30')
                     END AS Old_Lead_Date,
                     mx_Meta_LeadGen_ID,
                     mx_Meta_Campaign_Id,
                     mx_Meta_City,
                     CONVERT_TZ(mx_Meta_Lead_Created_Date, '+00:00','+05:30') as Meta_Lead_Created_Date,
                     mx_Meta_Product_ID,
                     CONVERT_TZ(NotableEvent, '+00:00','+05:30') as NotableEvent,
                     CONVERT_TZ(LastVisitDate, '+00:00','+05:30') as LastVisitDate,
                     CreatedByName AS LeadCreatedByName,
                     concat(ProspectID,' First Contact (Phone Call)') as helper,
                     right(Phone,10) as Phone2,
                     UPPER(TRIM(CASE WHEN mx_New_City <> '' THEN mx_New_City
                                     ELSE mx_Meta_City
                                 END)) AS City2,
                     mx_Meta_Adset_name,
                     mx_Meta_Adset_Id As Ad_Set_ID,
                     mx_IM_Query_Type,
                     mx_Category_POC,
                     mx_Street1,
		     mx_Meta_Platform,
                     LastModifiedOn,
                     mx_Old_Lead_Date As ReassignDate,
                     mx_Warmth_Remarks,
                     mx_Service_POC
                     FROM lsq_db.leads
                     WHERE ModifiedOn >= '2024-04-01'
                     )a
                      left join



                     (
                     select    concat  (RelatedEntityId,' ',task_name1) as helper,
                     task_name1,
                     StatusCode,
                     CONVERT_TZ(CreatedOn, '+00:00','+05:30') as FirstContactTaskCreateTime,
                     CONVERT_TZ(DueDate, '+00:00','+05:30') as DueDate,
                     CONVERT_TZ(CompletedOn, '+00:00','+05:30') as FirstCallTaskDoneDate,
                     CASE WHEN StatusCode=1 THEN
                          TIMESTAMPDIFF(MINUTE, CreatedOn, CompletedOn)/60.0
                          ELSE null
                      END AS FirstResponseTimeInHours,
                     StatusCode AS FirstContactTaskCallDone,
                     rank() over(partition by  RelatedEntityId order by createdOn asc) as rank10
                     from (
                     SELECT *,
                           CASE
                               WHEN task_name = 'First Contact' THEN 'First Contact (Phone Call)'
                               WHEN task_name = 'First Contact (Phone Call)' THEN 'First Contact (Phone Call)'
                               WHEN task_name = 'First Contact:' THEN 'First Contact (Phone Call)'
                               ELSE task_name
                            END AS task_name1
                     FROM
                        (
                           SELECT *,
                           TRIM( COALESCE(LEFT(name, POSITION(': ' IN name) - 1),name)  ) AS task_name
                           FROM
                                lsq_db.task
                       ) AS task1)
                       AS task2)b
                       on a.helper=b.helper
                       and rank10=1
                       left join



                       (SELECT RelatedProspectId as ProspectId,
                       id as opportunityId,
                       CONVERT_TZ( Created_On, '+00:00','+05:30') as OppCreateDate,
                       Stage as OppStage,Warmth as OppWarmth,
                       Status as OppStatus,
                       Product_Name as OpportunityProductDetails,
                       Category_of_Products_s as CategoryOfProduct,
                       Next_Step,
                       Demo_Outcome,
                       Demo_Cancelled_Reason,
                       Demo_Not_Interested_Reason,
                       Sample_Cancelled_Reason,
                       MeetingNot_Interested_Reason,
                       Next_Step__Quotation,
                       Rank() over (partition by RelatedProspectId order by Created_On desc) as rank1
                       FROM lsq_db.opportunity)c
                       on a.ProspectId=c.ProspectId
                       and rank1=1
                       left join
                        (SELECT Id as FollowUpLeadActivityId,
                        RelatedProspectId as ProspectId,
                        CONVERT_TZ(
                        FROM_UNIXTIME(
                                   CAST(SUBSTRING(createdOn, 7, 13) AS UNSIGNED) / 1000),
                                       '+00:00',
                                       '+05:30'
                        )AS FollowUpLeadActivityDate,
                       `Follow Up Outcome` as FollowUpLeadActivityOutcome,
                        rank() over(partition by RelatedProspectId order by createdOn desc) as rank2
                        FROM lsq_db.activity_follow_up_lead)d
                        on a.ProspectId=d.ProspectId
                        and rank2=1
                        left join
                        (SELECT id as FirstContactActivityId, RelatedProspectId as ProspectId,
                       CONVERT_TZ(createdOnString, '+00:00','+05:30') AS FirstContactActivityDate,
                        rank() over(partition by  RelatedProspectId order by createdOn asc) as rank11
                       FROM lsq_db.activity_first_contact)e
                       on a.ProspectId=e.ProspectId
                       and rank11=1
                       left join




                       (SELECT id as FollowUpOppActivityId,  RelatedProspectId as ProspectId ,
                       CreatedByName as FollowUpOppActivityAddedBy,
                       CONVERT_TZ(
                       FROM_UNIXTIME(
                                    CAST(SUBSTRING(createdOn, 7, 13) AS UNSIGNED) / 1000),
                                    '+00:00',
                                     '+05:30'
                       )AS FollowUpOppActivityIdDate,
                       `Opportunity Stage` as FollowUpOpportunityStage,
                       `Follow Up Outcome` as FollowUpOutcome,
                         `Lost Disposition`as FollowUpLostDisposition,
                       rank() over(partition by  RelatedProspectId order by createdOn desc) as rank3
                       FROM lsq_db.activity_follow_up_opportunity)f
                       on a.ProspectId=f.ProspectId
                       and rank3=1
                       left join
                       (SELECT Id as QuotationSharedActivity,
                        CONVERT_TZ(
                        FROM_UNIXTIME(
                        CAST(SUBSTRING(createdOn, 7, 13) AS UNSIGNED) / 1000),
                               '+00:00',
                               '+05:30'
                            )AS QuotationSharedActivityDate,
                      CreatedByName as QuotationSharedActivityDoneBy,RelatedProspectId as ProspectId,
                      `Not Accepted Reason` as QuotationNotAcceptedReason,
                       rank() over( partition by RelatedProspectId order by CreatedOn desc) as rank4
                       FROM lsq_db.activity_quotation_shared)g
                       on a.ProspectId=g.ProspectId
                       and rank4=1
                       left join
                     (SELECT id as MeetingScheduledID, CreatedByName as MeetingScheduledActivityOwner,
                     RelatedProspectId as ProspectId,
                     CONVERT_TZ(
                     FROM_UNIXTIME(
                         CAST(SUBSTRING(createdOn, 7, 13) AS UNSIGNED) / 1000),
                          '+00:00',
                         '+05:30'
                       )AS MeetingTaskScheduledDate,
                       `context for meeting` AS MeetingContext,
                     rank() over( partition by RelatedProspectId order by CreatedOn desc) as rank5
                     FROM lsq_db.activity_meeting_scheduled_opportunity)h
                     on a.ProspectId=h.ProspectId
                     and rank5=1
                     left join





                     (SELECT ID AS meeting_id,
                        CONVERT_TZ(
                                    FROM_UNIXTIME(
                                                    CAST(SUBSTRING(createdOn, 7, 13) AS UNSIGNED) / 1000),
                                                        '+00:00','+05:30'
                        )AS MeetingDoneDate,
                    CreatedByName AS MeetingDoneOwner,RelatedProspectId AS ProspectID,
                    rank() over( partition by RelatedProspectId order by CreatedOn desc) as rank6
                    FROM lsq_db.activity_meeting_opportunity)i
                    on a.ProspectId=i.ProspectId
                     and rank6=1
                     left join
                     (select ID AS DemoRequestedID,
                    UNIX_TIMESTAMP(CONVERT_TZ(CreatedOn, '+00:00', '+05:30')) AS DemoRequestedDate,
                    RelatedProspectId as ProspectId,
                    rank() over( partition by RelatedProspectId order by CreatedOn desc) as rank7
                    from lsq_db.activity_demo_requested)j
                    on a.ProspectId=j.ProspectId
                     and rank7=1
                     left join
                     (select ID AS DemoDoneID,
                    UNIX_TIMESTAMP(CONVERT_TZ(CreatedOn, '+00:00', '+05:30')) AS DemoDoneDate,
                    RelatedProspectId as ProspectId,
                    rank() over( partition by RelatedProspectId order by CreatedOn desc) as rank8
                    from lsq_db.activity_demo)k
                    on a.ProspectId=k.ProspectId
                     and rank8=1
                     left join
                     (
                      select  RelatedEntityId,
                        COUNT(CASE WHEN task_namex1 <> 'First Contact (Phone Call)' THEN RelatedEntityId END)
                        AS TotalTasksNotFirstContact,
                       COUNT(CASE WHEN task_namex1 <> 'First Contact (Phone Call)'
                       AND status1 <> 'Pending' THEN RelatedEntityId END)
                       AS TotalApplicableTasksNotFirstContact,
                       COUNT(CASE WHEN task_namex1 <> 'First Contact (Phone Call)'
                       AND status1 = 'Complete' THEN RelatedEntityId END)
                       AS TotalTasksCompletedNotFirstContact,
                       COUNT(CASE WHEN task_namex1 <> 'First Contact (Phone Call)'
                       AND status1 = 'Complete' AND TAT_In_Hrs > 1 THEN RelatedEntityId END)
                       AS TotalTaskCompletedOnTime
                       from (SELECT UserTaskId, Name, RelatedEntityId,DueDate,Status, TAT_In_Hrs,
                               CASE WHEN task_namex = 'First Contact' THEN 'First Contact (Phone Call)'
                                                WHEN task_namex = '(Lead)' THEN 'First Contact (Phone Call)'
                                                WHEN task_namex = 'First Contact:' THEN 'First Contact (Phone Call)'
                                                ELSE task_namex
                            END AS task_namex1,
                                           CASE WHEN status="Complete" THEN "Complete"
                                               WHEN DueDate>now() THEN "Pending"
                                               ELSE status
                                               END AS status1
                       FROM (SELECT *,
                           TRIM( COALESCE(LEFT(name, POSITION(': ' IN name) - 1),name)  ) AS task_nameX,
                                           CASE WHEN  StatusCode=0 THEN "Overdue"
                                                WHEN  StatusCode=1 THEN "Complete"
                           END AS status,
                                           CASE
                                       WHEN
                                   StatusCode = 1 THEN TIMESTAMPDIFF(hour, DueDate,CompletedOn)
                           ELSE NULL
                               END AS TAT_In_Hrs
                              FROM lsq_db.task ) AS taskx1)
                            AS taskx2
                                           GROUP BY RelatedEntityId
                     )m
                       on a.ProspectId=m.RelatedEntityId
                       
                       left join(SELECT RelatedProspectId, 
                                        MIN(CONVERT_TZ(CreatedOnString,

                                                                        '+00:00','+05:30'  )) AS FirstOppDate   
                                                           FROM lsq_db.opportunity
                                                          GROUP BY RelatedProspectId) n
                                                          on a.ProspectId=n.RelatedProspectId
                                                          AND e.FirstContactActivityDate <= n.FirstOppDate
                                                          AND DATE_ADD(e.FirstContactActivityDate, INTERVAL 2 HOUR) >= n.FirstOppDate
                                                          
                    left join(SELECT ao.RelatedProspectId,
                                     COUNT(ao.RelatedProspectId)  AS Totalfollowup,
                                     SUM(CASE WHEN CONVERT_TZ(
                                              FROM_UNIXTIME(CAST(SUBSTRING(ao.createdOn, 7, 13) AS UNSIGNED) / 1000),
                                                           '+00:00','+05:30'
                                                              ) 
                                        <  DATE_ADD( ap.FirstAct, INTERVAL 5 MINUTE) THEN 1 
                                        END ) AS TotalFollowUpBeforeFirstContact,FirstAct
                                FROM lsq_db.activity_follow_up_lead ao
                                LEFT JOIN (SELECT RelatedProspectId,
                                                  MIN(CONVERT_TZ(FROM_UNIXTIME(
                                                                  CAST(SUBSTRING(createdOn, 7, 13) AS UNSIGNED) / 1000),
                                                           '+00:00','+05:30' ) ) AS FirstAct
                                            FROM lsq_db.activity_first_contact
                                            GROUP BY RelatedProspectId
                                           ) ap ON ao.RelatedProspectId = ap.RelatedProspectId
                                             GROUP BY ao.RelatedProspectId, 
                                            ap.FirstAct) q
                                            on a.ProspectId=q.RelatedProspectId
                                                       

                              ''')

my_result = my_cursor.fetchall()



print("SQL Query Excecuted", datetime.now())

# Convert MySQL result to a DataFrame
header = [i[0] for i in my_cursor.description]
df_mysql = pd.DataFrame(my_result, columns=header)



# Fetch data from the Google Sheets directly into DataFrames
df_sheet2 = pd.DataFrame(worksheet_sheet2.get_all_records())
limited_rows_df2 = df_sheet2.head(4500)

df_sheet3 = pd.DataFrame(worksheet_sheet3.get_all_records())
limited_rows_df3 = df_sheet3.head(60)

df_sheet4 = pd.DataFrame(worksheet_sheet4.get_all_records())
limited_rows_df4 = df_sheet4.head(60)

df_sheet5 = pd.DataFrame(worksheet_sheet5.get_all_records())
limited_rows_df5 = df_sheet5.head(40)

df_sheet6 = pd.DataFrame(worksheet_sheet6.get_all_records())
limited_rows_df6 = df_sheet6.head(40)

df_sheet7 = pd.DataFrame(worksheet_sheet7.get_all_records())
limited_rows_df7 = df_sheet7.head(2000)
limited_rows_df7.rename(columns={'Inside Sales POC': 'Inside Sales POC Order'}, inplace=True)



#creating actual lead date
df_mysql['ActualLeadDate'] = df_mysql['Old_Lead_Date'].fillna(df_mysql['CreatedOn'])







# Create a First Task Creation Date
df_mysql['FirstcontactTaskCreationDate'] = np.where(df_mysql['FirstContactActivityDate'].notna(),
                                              df_mysql['FirstContactActivityDate'], df_mysql['ActualLeadDate'])


# Creating Region Mapping for LSQ raw Cities
df_mysql['City2'] = df_mysql['City2'].str.strip()
df_mysql = pd.merge(df_mysql, limited_rows_df2, how='left', left_on='City2', right_on='City')
df_mysql = df_mysql.drop(columns=['City'])
fill_missing = lambda x: 'Others/Unmapped' if pd.isnull(x['Region Mapping for LSQ raw Cities']) else x['Region Mapping for LSQ raw Cities']
df_mysql['Region Mapping for LSQ raw Cities'] = df_mysql.apply(fill_missing, axis=1)



#Creating Region Mapping for Validation
df_mysql = pd.merge(df_mysql, limited_rows_df3, how='left', left_on='Region Mapping for LSQ raw Cities', right_on='State')

df_mysql = df_mysql.drop(columns=['State'])

#Creating Campaign Name 2.0
df_mysql = pd.merge(df_mysql, limited_rows_df4, how='left', left_on='MetaCampaignName', right_on='Campaign Name')

df_mysql = df_mysql.drop(columns=['Campaign Name'])


#creating Inside Sales Flag (Lead Create)
df_mysql = df_mysql.merge(limited_rows_df5[['BD Name', 'Inside Sales Flag']],
                                   left_on='Owner', right_on='BD Name', how='left')

df_mysql = df_mysql.drop(columns=['BD Name'])

#creating Inside Sales Flag (Meeting Schedule Owner)
df_mysql = df_mysql.merge(limited_rows_df5[['BD Name', 'Inside Sales Flag']],
                                   left_on='MeetingScheduledActivityOwner', right_on='BD Name', how='left')

df_mysql = df_mysql.rename(columns={'Inside Sales Flag_x': 'Owner Inside Sales Flag'})
df_mysql = df_mysql.rename(columns={'Inside Sales Flag_y': 'Meeting Scheduled Inside Sales Flag'})

df_mysql = df_mysql.drop(columns=['BD Name'])

#creating Inside Sales Flag (Meeting Done Owner)
df_mysql = df_mysql.merge(limited_rows_df5[['BD Name', 'Inside Sales Flag']],
                                   left_on='MeetingDoneOwner', right_on='BD Name', how='left')
df_mysql = df_mysql.rename(columns={'Inside Sales Flag': 'Meeting Done Inside Sales Flag'})
df_mysql = df_mysql.drop(columns=['BD Name'])


#Creating Meeting Count Flag
df_mysql['Meeting Count Flag'] = np.where((df_mysql['Meeting Scheduled Inside Sales Flag'] == 1) &
                                          (df_mysql['Meeting Done Inside Sales Flag'] == 0), 1, 0)

# Creating Conditions for stage of lost stage
def determine_lost_stage(row):
    if not pd.isnull(row['Demo_Not_Interested_Reason']) and row['Demo_Not_Interested_Reason'] != "":
        return "Demo Done"
    elif not pd.isnull(row['Demo_Cancelled_Reason'])  and row['Demo_Cancelled_Reason'] != "":
        return "Demo Scheduled"
    elif not pd.isnull(row['Sample_Cancelled_Reason'])  and row['Sample_Cancelled_Reason'] != "":
        return "Sample Requested"
    elif not pd.isnull(row['MeetingNot_Interested_Reason'])  and row['MeetingNot_Interested_Reason'] != "":
        return "Meeting Done"
    elif not pd.isnull(row['FollowUpLostDisposition'])  and row['FollowUpLostDisposition'] != "":
        return row['FollowUpOpportunityStage']
    elif not pd.isnull(row['QuotationNotAcceptedReason'])  and row['QuotationNotAcceptedReason'] != "":
        return "Quotation Shared"
    elif np.logical_or.reduce([
        (row['mx_Call_Outcome'] == "Not Interested"),
        (row['mx_Call_Outcome'] == "Interested"),
        (row['mx_Requirement_Gathering_Meeting'] == "Not Interested"),
        (row['FollowUpLeadActivityOutcome'] == "Did Not Pick : Mark as Lost"),
        (row['FollowUpLeadActivityOutcome'] == "Not Interested")
    ]):
        return "First Contact"
    elif  row['mx_Call_Outcome'] == "Invalid Number":
        return "Prospect"
    else:
        return ""

df_mysql['LostStage'] = df_mysql.apply(determine_lost_stage, axis=1)



#creating Lost Disposition

def lost_disposition(row):

    if row['mx_Not_Interested'] :
        return row['mx_Not_Interested']
    elif row['mx_Meeting_Not_Interested_Reason'] :
        return row['mx_Meeting_Not_Interested_Reason']
    elif row['Demo_Cancelled_Reason'] :
        return row['Demo_Cancelled_Reason']
    elif row['Demo_Not_Interested_Reason'] :
        return row['Demo_Not_Interested_Reason']
    elif row['Sample_Cancelled_Reason'] :
        return row['Sample_Cancelled_Reason']
    elif row['MeetingNot_Interested_Reason'] :
        return row['MeetingNot_Interested_Reason']
    elif row['FollowUpLostDisposition'] :
        return row['FollowUpLostDisposition']
    elif row['QuotationNotAcceptedReason'] :
        return row['QuotationNotAcceptedReason']
    elif row['mx_Call_Outcome'] == "Invalid Number":
        return "Invalid Number"
    elif row['Next_Step__Quotation']=='Category Cannot Procure (Lost)' :
        return row['Next_Step__Quotation']
    elif row['FollowUpLeadActivityOutcome']=='Did Not Pick : Mark as Lost' :
        return row['FollowUpLeadActivityOutcome']
    else:
        return ""


df_mysql['LostDisposition'] = df_mysql.apply(lost_disposition, axis=1)




#creating Warmth2.0
df_mysql['Warmth2.0'] = df_mysql['OppWarmth'].fillna(df_mysql['mx_Warmth'])

#creating Interested Flag
df_mysql['Interested Flag'] = df_mysql['opportunityId'].apply(lambda x: 1 if pd.notna(x) else 0)

#creating Opportunity Table
my_cursor.execute('''SELECT id ,RelatedProspectId AS leadID,
                    CONVERT_TZ( Created_On, '+00:00','+05:30') as OppCreateDate1,
                    Stage as OppStage1 FROM lsq_db.opportunity''')
opportunitydata = my_cursor.fetchall()


# Convert data to a DataFrame
header = [i[0] for i in my_cursor.description]
opportunitydata = pd.DataFrame(opportunitydata, columns=header)

opportunitydata['OppCreateDate1'] = pd.to_datetime(opportunitydata['OppCreateDate1'])

opportunitydata = opportunitydata.sort_values(by='OppCreateDate1', ascending=True)


# #creating Order Confirmation Date
# orderraise = pd.merge(limited_rows_df7,opportunitydata, how='left', left_on='Opportunity ID', right_on='id')

# orderraise['Order Confirmation Date'] = pd.to_datetime(orderraise['Order Confirmation Date'], format='%d/%m/%Y', errors='coerce')


# min_order_confirmation_date = orderraise.groupby('leadID')['Order Confirmation Date'].min()
# orderdate = min_order_confirmation_date.reset_index(name='Min_Order_Confirmation_Date')
# df_mysql = pd.merge(df_mysql, orderdate, how='left', left_on='ProspectID', right_on='leadID')
# df_mysql = df_mysql.drop(columns=['leadID'])

#creating Order Confirmation Date
orderraise = pd.merge(limited_rows_df7,opportunitydata, how='left', left_on='Opportunity ID', right_on='id')

orderraise['Order Confirmation Date'] = pd.to_datetime(orderraise['Order Confirmation Date'], format='%d/%m/%Y', errors='coerce')


min_order_confirmation_date = orderraise.groupby('leadID').agg({'Order Confirmation Date': 'min', 'Inside Sales POC Order': 'first'})
orderdate = min_order_confirmation_date.reset_index()
orderdate.rename(columns={'Order Confirmation Date': 'Min_Order_Confirmation_Date'}, inplace=True)
df_mysql = pd.merge(df_mysql, orderdate, how='left', left_on='ProspectID', right_on='leadID')
df_mysql = df_mysql.drop(columns=['leadID'])




#Creating First Contact To Interested
def calculate_hours(row):
    opp_create_date = pd.to_datetime(row['OppCreateDate'])
    actual_lead_date = pd.to_datetime(row['ActualLeadDate'])
    first_contact_activity_date = pd.to_datetime(row['FirstContactActivityDate']) if pd.notna(row['FirstContactActivityDate'])else np.nan
    if pd.isna(first_contact_activity_date):
        FirstContactToInterested = max((opp_create_date - actual_lead_date).total_seconds() / 3600, 0)
    else:
        FirstContactToInterested = max((opp_create_date - first_contact_activity_date).total_seconds() / 3600, 0)

    return FirstContactToInterested

df_mysql['FirstContactToInterested'] = df_mysql.apply(calculate_hours, axis=1)


#Creating Interested To Order
df_mysql['HourDifference'] = (df_mysql['Min_Order_Confirmation_Date'] - df_mysql['OppCreateDate']).dt.total_seconds() / 3600
df_mysql['HourDifference'] = df_mysql['HourDifference'].fillna(0)
df_mysql['InterestedToOrder'] = np.where(df_mysql['Min_Order_Confirmation_Date'] != "", df_mysql['HourDifference'], "")
df_mysql = df_mysql.drop(columns=['HourDifference'])


#Creating Opp Sorted Table
oppsorted = opportunitydata.merge(limited_rows_df7[['Opportunity ID', 'Order ID', 'Order Counting Flag',
                                                     'Order Confirmation Date','New or Repeat Flag']],
                                   left_on='id', right_on='Opportunity ID', how='left')


oppsorted = oppsorted.drop(columns=['Opportunity ID'])


oppsorted['OrderType1'] = ((oppsorted['leadID'] == oppsorted['leadID'].shift()) &
                          (oppsorted['Order Counting Flag'] == 1) &
                          (oppsorted['OppStage1'] == 'Won') &
                          (oppsorted['OppCreateDate1'] < oppsorted['OppCreateDate1'].shift()))
oppsorted['OrderType1'] = oppsorted['OrderType1'].map({True: 'Repeat', False: np.nan})
oppsorted['New/Repeat'] = np.where(oppsorted['OrderType1'] == 'Repeat', 'Repeat',
                                   np.where(oppsorted['Order Counting Flag'] == 1, oppsorted['New or Repeat Flag'], np.nan))

oppsorted = oppsorted.drop(columns=['OrderType1'])


#Creating new Order Flag
new_order_df = oppsorted.loc[
    (oppsorted['Order Counting Flag'] == 1) & (oppsorted['New or Repeat Flag'] == 'New')
].groupby('leadID').size().reset_index(name='NewOrderFlag')
df_mysql = pd.merge(df_mysql, new_order_df, how='left', left_on='ProspectID', right_on='leadID')
df_mysql = df_mysql.drop(columns=['leadID'])


# Creating a new column 'Count Of Repeat Order'
repeat_order_df = oppsorted.loc[
    (oppsorted['Order Counting Flag'] == 1) & (oppsorted['New or Repeat Flag'] == 'Repeat')
 ].groupby('leadID').size().reset_index(name='CountOfRepeatOrder')
df_mysql = pd.merge(df_mysql, repeat_order_df, how='left', left_on='ProspectID', right_on='leadID')
df_mysql = df_mysql.drop(columns=['leadID'])
df_mysql[['NewOrderFlag', 'CountOfRepeatOrder']] = df_mysql[['NewOrderFlag', 'CountOfRepeatOrder']].fillna(0)


# Creating First Task Creation Date
df_mysql['FirstTaskCreationDate'] = np.where(df_mysql['FirstContactTaskCreateTime'].notna(),
                                              df_mysql['FirstContactTaskCreateTime'], df_mysql['ActualLeadDate'])


#Creating BD Flag Lead Created
df_mysql = pd.merge(df_mysql, limited_rows_df6, how='left', left_on='LeadCreatedByName', right_on='BD Names')
df_mysql['BD Flag Lead Created'] = df_mysql['BD Flag']
df_mysql = df_mysql.drop(columns=['BD Names', 'BD Flag'])


# Creating Lead To Order
df_mysql['HourDifference1'] = (df_mysql['Min_Order_Confirmation_Date'] -
                                df_mysql['ActualLeadDate']).dt.total_seconds() / 3600
df_mysql['LeadToOrder'] = np.maximum(df_mysql['HourDifference1'], 0)
df_mysql = df_mysql.drop(columns=['HourDifference1'])


# Creating Lead To Interested
df_mysql['HourDifference2'] = (df_mysql['OppCreateDate'] -
                                df_mysql['ActualLeadDate']).dt.total_seconds() / 3600
df_mysql['LeadToInterested'] = np.maximum(df_mysql['HourDifference2'], 0)
df_mysql = df_mysql.drop(columns=['HourDifference2'])


# Create a Interested To Meeting
df_mysql['HourDifference3'] = (df_mysql['OppCreateDate'] -
                                df_mysql['MeetingDoneDate']).dt.total_seconds() / 3600
df_mysql['InterestedToMeeting'] = np.maximum(df_mysql['HourDifference3'], 0)
df_mysql = df_mysql.drop(columns=['HourDifference3'])


# Create a Meeting to Order
df_mysql['HourDifference4'] = (df_mysql['MeetingDoneDate'] -
                                df_mysql['Min_Order_Confirmation_Date']).dt.total_seconds() / 3600
df_mysql['MeetingToOrder'] = np.maximum(df_mysql['HourDifference4'], 0)
df_mysql = df_mysql.drop(columns=['HourDifference4'])


# Changing Actual lead date from timestamp to Date
df_mysql['ActualLeadDate'] = pd.to_datetime(df_mysql['ActualLeadDate'], errors='coerce')
df_mysql['ActualLeadDate'] = df_mysql['ActualLeadDate'].dt.date

df_mysql = df_mysql.sort_values(by='ActualLeadDate')


#creating Inside Sales Flag (Lead Create)
df_mysql = df_mysql.merge(limited_rows_df5[['BD Name', 'Team']],
                                   left_on='Owner', right_on='BD Name', how='left')

df_mysql = df_mysql.drop(columns=['BD Name'])

# df_mysql['LastModifiedOn'] = pd.to_datetime(df_mysql['LastModifiedOn'], errors='coerce', format='ISO8601')
# df_mysql = df_mysql[df_mysql['LastModifiedOn'] > '2024-01-01']



print("Raw Master Raw Created", datetime.now())


# In[4]:


columns_to_keep=['ActualLeadDate',
                 'ProspectID',
                 'LeadNumber',
#                  'RecentlyModifiedOn',
                 'Owner',
#                  'ModifiedBy',
                 'LeadName',
#                  'Phone',
                 'LeadSource',
                 'MetaCampaignName',
                 'LeadStage',
#                  'LeadScore',
                 'EngagementScore',
#                  'FirstActivityDate',
                 'LastActivity',
                 'LastActivityDate',
                 'CreatedOn',
#                  'ModifiedOn',
                 'ProductDetails',
#                  'mx_Is_this_an_Urgent_Query',
#                  'Follow_Up_Date',
                 'mx_POC_Designation',
                 'mx_New_State',
                 'mx_New_City',
#                  'Meeting_Date_and_Time',
#                  'mx_Context_for_Meeting',
#                  'mx_Warmth',
                 'mx_Call_Outcome',
#                  'mx_Not_Interested',
                 'mx_Category_of_Product',
#                  'mx_Requirement_Gathering_Meeting',
#                  'mx_Meeting_Not_Interested_Reason',
#                  'Old_Lead_Date',
#                  'mx_Meta_LeadGen_ID',
#                  'mx_Meta_Campaign_Id',
                 'mx_Meta_City',
#                  'Meta_Lead_Created_Date',
#                  'mx_Meta_Product_ID',
#                  'NotableEvent',
#                  'LastVisitDate',
#                   'FirstContactTaskCreateTime',
                 'Phone2',
                 'City2',
                 'Region Mapping for LSQ raw Cities',
                 'Region Mapping for Validation',
                 'Campaign Name 2.0',
                 'Owner Inside Sales Flag',
                 'opportunityId',
                 'OppCreateDate',
#                  'OppWarmth',
#                  'OpportunityProductDetails',
                 'CategoryOfProduct',
#                  'OppStage',
#                  'Next_Step',
#                  'Demo_Outcome',
#                  'Demo_Cancelled_Reason',
#                  'Demo_Not_Interested_Reason',
#                  'Sample_Cancelled_Reason',
#                  'MeetingNot_Interested_Reason',
#                  'FollowUpLeadActivityId',
#                  'FollowUpLeadActivityDate',
#                  'FollowUpLeadActivityOutcome',
#                  'FirstContactActivityId',
#                  'FirstContactActivityDate',
#                  'FollowUpOppActivityId',
#                  'FollowUpOppActivityIdDate',
#                  'FollowUpOpportunityStage',
#                  'FollowUpOutcome',
#                  'FollowUpLostDisposition',
#                  'FollowUpOppActivityAddedBy',
#                  'QuotationSharedActivity',
#                  'QuotationSharedActivityDate',
#                  'QuotationSharedActivityDoneBy',
#                  'QuotationNotAcceptedReason',
#                  'MeetingScheduledID',
#                  'MeetingTaskScheduledDate',
#                  'MeetingContext',
#                  'MeetingScheduledActivityOwner',
#                  'Meeting Scheduled Inside Sales Flag',
#                  'meeting_ID',
#                  'MeetingDoneDate',
#                  'MeetingDoneOwner',
#                  'Meeting Done Inside Sales Flag',
#                  'Meeting Count Flag',
                 'FirstContactTaskCallDone',
#                  'TotalTasksNotFirstContact',
                 'TotalApplicableTasksNotFirstContact',
                 'TotalTasksCompletedNotFirstContact',
                 'TotalTaskCompletedOnTime',
                 'LastActivityAging',
                 'LostStage',
                 'LostDisposition',
                 'Warmth2.0',
#                  'Interested Flag',
                 'FirstCallTaskDoneDate',
                 'FirstResponseTimeInHours',
#                  'OppStatus',
                 'Min_Order_Confirmation_Date',
#                  'ProspectAging',
#                  'FirstContactAging',
#                  'InterestedAging',
#                  'ProspectToFirstContact',
#                  'FirstContactToInterested',
#                  'InterestedToOrder',
                 'NewOrderFlag',
                 'CountOfRepeatOrder',
#                  'FirstTaskCreationDate',
#                  'LeadCreatedByName',
#                  'BD Flag Lead Created',
                 'mx_Meta_Adset_name',
#                  'DemoRequestedDate',
#                  'DemoDoneDate',
#                  'LeadToOrder',
#                  'LeadToInterested',
#                  'InterestedToMeeting',
#                  'MeetingToOrder',
                 'Ad_Set_ID',
                 'mx_IM_Query_Type',
                 'mx_Category_POC',
                 'Inside Sales POC Order',
                 'mx_Street1',
                 'mx_Meta_Platform',
                 'FirstContactTimeInHrs',
                 'FirstOppDate',
                 'Totalfollowup',
                 'TotalFollowUpBeforeFirstContact',
                 'FirstContactActivityTime',
                 'Team',
                 'ReassignDate',
                 'mx_Warmth_Remarks',
                 'mx_Service_POC'
                ]

new_df = df_mysql[columns_to_keep].copy()


print("Master Raw Created", datetime.now())



#Clear Worksheet sales dashboard
range_to_clear_sheet1 = 'A:BB'
worksheet.batch_clear([range_to_clear_sheet1])

#updating worksheet with data
gsdf.set_with_dataframe(worksheet, new_df)


print("Master Raw Sent To Sales Dashboard 2.0", datetime.now())







#Clear Worksheet sales dashboard

condition = (df_mysql['LeadSource'] == 'Service Referral')
Service = df_mysql[condition][columns_to_keep].copy()

range_to_clear_sheet1 = 'A:BB'
worksheetSer.batch_clear([range_to_clear_sheet1])

#updating worksheet with data
gsdf.set_with_dataframe(worksheetSer, Service )


print("Master Raw Sent To Service Dashboard 2.0", datetime.now())




# For Others
condition = (df_mysql['Team'] == 'Others')
Others = df_mysql[condition][columns_to_keep].copy()

#Clear Worksheet Lead Data (Others)
range_to_clear_sheet1 = 'A:BB'
worksheetP.batch_clear([range_to_clear_sheet1])

#updating worksheet with data
gsdf.set_with_dataframe(worksheetP, Others)


# For Team Karna
condition = (df_mysql['Team'] == 'Team Karna')
TeamKarna = df_mysql[condition][columns_to_keep].copy()

#Clear Worksheet Lead Data (Team Karna)
range_to_clear_sheet1 = 'A:BB'
worksheetQ.batch_clear([range_to_clear_sheet1])

#updating worksheet with data
gsdf.set_with_dataframe(worksheetQ, TeamKarna)



# For Team Arjuna
condition = (df_mysql['Team'] == 'Team Arjuna')
TeamArjuna = df_mysql[condition][columns_to_keep].copy()

#Clear Worksheet Lead Data (Team Arjuna)
range_to_clear_sheet1 = 'A:BB'
worksheetR.batch_clear([range_to_clear_sheet1])

#updating worksheet with data
gsdf.set_with_dataframe(worksheetR, TeamArjuna)



# For Inside Sales

date_threshold = datetime.strptime('2024-03-01', '%Y-%m-%d').date()

# Filter the DataFrame
condition = (df_mysql['Team'] == 'Inside Sales') & (df_mysql['ActualLeadDate'] >= date_threshold)
InsideSales = df_mysql[condition][columns_to_keep].copy()


#Clear Worksheet Lead Data (Inside Sales)
range_to_clear_sheet1 = 'A:BB'
worksheetS.batch_clear([range_to_clear_sheet1])

#updating worksheet with data
gsdf.set_with_dataframe(worksheetS, InsideSales)


#Clear Worksheet Lead Data (Inside Sales)
range_to_clear_sheet1 = 'A:BB'
worksheet_i.batch_clear([range_to_clear_sheet1])

#updating worksheet with data
gsdf.set_with_dataframe(worksheet_i, new_df)




columns_to_keep1=['ActualLeadDate',
                 'ProspectID',
                 'LeadNumber',
                 'RecentlyModifiedOn',
                 'Owner',
                 'ModifiedBy',
                 'LeadName',
                 'Phone',
                 'LeadSource',
                 'MetaCampaignName',
                 'LeadStage',
                 'LeadScore',
                 'EngagementScore',
                 'FirstActivityDate',
                 'LastActivity',
                 'LastActivityDate',
                 'CreatedOn',
                 'ModifiedOn',
                 'ProductDetails',
                 'mx_Is_this_an_Urgent_Query',
                 'Follow_Up_Date',
                 'mx_POC_Designation',
                 'mx_New_State',
                 'mx_New_City',
                 'Meeting_Date_and_Time',
                 'mx_Context_for_Meeting',
                 'mx_Warmth',
                 'mx_Call_Outcome',
                 'mx_Not_Interested',
                 'mx_Category_of_Product',
                 'mx_Requirement_Gathering_Meeting',
                 'mx_Meeting_Not_Interested_Reason',
                 'Old_Lead_Date',
                 'mx_Meta_LeadGen_ID',
                 'mx_Meta_Campaign_Id',
                 'mx_Meta_City',
                 'Meta_Lead_Created_Date',
                 'mx_Meta_Product_ID',
                 'NotableEvent',
                 'LastVisitDate',
                  'FirstContactTaskCreateTime',
                 'Phone2',
                 'City2',
                 'Region Mapping for LSQ raw Cities',
                 'Region Mapping for Validation',
                 'Campaign Name 2.0',
                 'Owner Inside Sales Flag',
                 'opportunityId',
                 'OppCreateDate',
                 'OppWarmth',
                 'OpportunityProductDetails',
                 'CategoryOfProduct',
                 'OppStage',
                 'Next_Step',
                 'Demo_Outcome',
                 'Demo_Cancelled_Reason',
                 'Demo_Not_Interested_Reason',
                 'Sample_Cancelled_Reason',
                 'MeetingNot_Interested_Reason',
                 'FollowUpLeadActivityId',
                 'FollowUpLeadActivityDate',
                 'FollowUpLeadActivityOutcome',
                 'FirstContactActivityId',
                 'FirstContactActivityDate',
                 'FollowUpOppActivityId',
                 'FollowUpOppActivityIdDate',
                 'FollowUpOpportunityStage',
                 'FollowUpOutcome',
                 'FollowUpLostDisposition',
                 'FollowUpOppActivityAddedBy',
                 'QuotationSharedActivity',
                 'QuotationSharedActivityDate',
                 'QuotationSharedActivityDoneBy',
                 'QuotationNotAcceptedReason',
                 'MeetingScheduledID',
                 'MeetingTaskScheduledDate',
                 'MeetingContext',
                 'MeetingScheduledActivityOwner',
                 'Meeting Scheduled Inside Sales Flag',
                 'meeting_ID',
                 'MeetingDoneDate',
                 'MeetingDoneOwner',
                 'Meeting Done Inside Sales Flag',
                 'Meeting Count Flag',
                 'FirstContactTaskCallDone',
                 'TotalTasksNotFirstContact',
                 'TotalApplicableTasksNotFirstContact',
                 'TotalTasksCompletedNotFirstContact',
                 'TotalTaskCompletedOnTime',
                 'LastActivityAging',
                 'LostStage',
                 'LostDisposition',
                 'Warmth2.0',
                 'Interested Flag',
                 'FirstCallTaskDoneDate',
                 'FirstResponseTimeInHours',
                 'OppStatus',
                 'Min_Order_Confirmation_Date',
                 'ProspectAging',
                 'FirstContactAging',
                 'InterestedAging',
                 'ProspectToFirstContact',
                 'FirstContactToInterested',
                 'InterestedToOrder',
                 'NewOrderFlag',
                 'CountOfRepeatOrder',
                 'FirstTaskCreationDate',
                 'LeadCreatedByName',
                 'BD Flag Lead Created',
                 'mx_Meta_Adset_name',
                 'DemoRequestedDate',
                 'DemoDoneDate',
                 'LeadToOrder',
                 'LeadToInterested',
                 'InterestedToMeeting',
                 'MeetingToOrder',
                 'mx_IM_Query_Type',
                 'mx_Category_POC',
                  
                ]

new_df1 = df_mysql[columns_to_keep1].copy()

condition = (df_mysql['Owner Inside Sales Flag'] == 0)
aa = df_mysql[condition][columns_to_keep].copy()

print("Master Raw Sent To Field sales", datetime.now())

#Clear Worksheet
range_to_clear_sheet2 = 'A:DN'
worksheetX.batch_clear([range_to_clear_sheet2])
#updating worksheet with data
gsdf.set_with_dataframe(worksheetX, aa)


print("Master Raw Sent To field raw  data", datetime.now())






new_df1 = df_mysql[columns_to_keep1].copy()

condition1 = (df_mysql['LeadSource'] == 'Category Referral')
Category = df_mysql[condition1][columns_to_keep].copy()

#Clear Worksheet
range_to_clear_sheet3 = 'A:BA'
worksheetCat.batch_clear([range_to_clear_sheet3])
#updating worksheet with data
gsdf.set_with_dataframe(worksheetCat, Category)


print("Master Raw Sent To category sheet", datetime.now())


my_cursor = my_conn.cursor()
my_cursor.execute(''' SELECT LEFT(start_time,10) AS DateAndTime,
                             CASE
                                  WHEN Call_Type=1 THEN "outgoing"
                                  WHEN Call_Type=0 THEN "incoming" 
                             END AS CallDirection,
                             CASE
                                  WHEN agent_number LIKE '%Missed%' THEN "Missed"
		                  WHEN agent_number='User Disconnected' THEN "Abandoned"
                                  WHEN agent_number='Did Not Process' THEN "Did Not Process"
                                  ELSE "Answered"
                             END AS CallStatus,
                             customer_number,
                             destination AS AgentNumber,
                             CONCAT(first_name," ", last_name) AS AgentName,
                             call_duration,
                             left(c.CreatedOn,10) AS LeadDate,
                             DATEDIFF(CURDATE(), DATE(c.CreatedOn)) AS Aging
                     FROM knowlarity_db.callHistory a
                     LEFT JOIN knowlarity_db.AgentDetails b
                     ON RIGHT(a.destination,10)=b.phone
                     LEFT JOIN lsq_db.leads c
                     ON RIGHT(a.customer_number,10)=right(c.Phone,10)
                     WHERE a.start_time > CURDATE() - INTERVAL 7 DAY  
                 ''')


my_resultc = my_cursor.fetchall()



print("SQL Query Excecuted", datetime.now())

# Convert MySQL result to a DataFrame
header = [i[0] for i in my_cursor.description]
df_call = pd.DataFrame(my_resultc, columns=header)


#Clear Worksheet sales dashboard
range_to_clear_sheetc = 'A:I'
worksheetCall.batch_clear([range_to_clear_sheetc])

#updating worksheet with data
gsdf.set_with_dataframe(worksheetCall, df_call)


# Close MySQL connection
my_conn.close()
