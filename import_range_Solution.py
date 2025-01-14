



import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta
import pandas as pd
import gspread_dataframe as gsdf

def fetch_data_from_sheet(key, sheet_name):
    credentials = Credentials.from_service_account_file('/home/ubuntu/simran/datafromdatabase-556db248ebb2.json',
                                                    scopes=['https://www.googleapis.com/auth/spreadsheets'])

    gc = gspread.authorize(credentials)
    worksheet = gc.open_by_key(key).worksheet(sheet_name)
    data = worksheet.get_all_values()
    df_sheet = pd.DataFrame(data[1:], columns=data[0])
    return df_sheet, gc

def convert_to_same_datatypes(data):
    # Convert all elements to their respective data types
    converted_data = []
    for row in data:
        converted_row = []
        for value in row:
            if value.isdigit():
                converted_row.append(int(value))
            elif value.replace('.', '', 1).isdigit():  # Check for floats
                converted_row.append(str(value))  # Convert to string
            else:
                converted_row.append(value)
        converted_data.append(converted_row)
    return converted_data

def update_sheet_with_dataframe(worksheet, dataframe, start_cell):
    # Get the range to update
    cell_range = f"{start_cell[0]}{start_cell[1]}"
    # Convert DataFrame to list of lists
    data = [dataframe.columns.tolist()] + dataframe.values.tolist()
    # Update the worksheet with the data
    worksheet.update(cell_range, data, value_input_option='USER_ENTERED')

spreadsheet_key = '1iqVCN4M4K1cRdWHQVMclq6355ccxD34tix1xy_IolFY'
worksheet_name = 'import'

mydf, gc = fetch_data_from_sheet(spreadsheet_key, worksheet_name)

# Add a new column 'processed' to mark rows as processed
mydf['processed'] = False

for index, row in mydf.iterrows():
    try:
        if row['run_flag'] == '1' and not row['processed']:
            source_key = row['from_spreadsheet_key']
            source_worksheet_name = row['from_worksheet']
            source_range = row['from_range'] 
            
            source_worksheet = gc.open_by_key(source_key).worksheet(source_worksheet_name)
            data = source_worksheet.get(source_range)
            
            
            destination_key = row['to_spreadsheet_key']
            destination_worksheet_name = row['to_worksheet']
            destination_range = row['to_range']
            
            destination_worksheet = gc.open_by_key(destination_key).worksheet(destination_worksheet_name)
            
            # Clear the destination range before updating it with new data
            destination_worksheet.batch_clear([destination_range])
            
            # Ensure 'data' is a list of lists with the same data types as the source
            converted_data = convert_to_same_datatypes(data)
            
            
            # Convert the converted data to DataFrame
            converted_df = pd.DataFrame(converted_data[1:], columns=converted_data[0])
            
        
            # Update the destination range with new data 
            gsdf.set_with_dataframe(destination_worksheet, converted_df)
            
            
#             update_sheet_with_dataframe(destination_worksheet, converted_df, ('A', 1))
            
            # Update 'Last Run Time' column with current timestamp
            mydf.at[index, 'Last Run Time'] = (datetime.now() + timedelta(minutes=330)).strftime("%Y-%m-%d %H:%M:%S")
            
            print(f"Data copied from {source_worksheet.title} to {destination_worksheet.title} at {datetime.now()}")


            # Mark row as processed
            mydf.at[index, 'processed'] = True

    except gspread.exceptions.APIError as e:
        print(f"Google Sheets API Error: {e}")
        
    except Exception as e:
        print(f"An error occurred: {e}")
        
# Drop the 'run_flag' and 'Time Since Last Run' columns from DataFrame
columns_to_drop = ['run_flag', 'Time Since Last Run','Frequency (mins)','processed','Owner','Remarks']
mydf = mydf.drop(columns=columns_to_drop, errors='ignore')

# Update the original Google Sheet with the modified DataFrame
worksheet = gc.open_by_key(spreadsheet_key).worksheet(worksheet_name)
update_sheet_with_dataframe(worksheet, mydf, ('A', 1))  # Assuming data starts from A2

print("DataFrame updated back to Google Sheets")

