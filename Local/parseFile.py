import glob
import os
import os.path
import csv
import pandas as pd
from datetime import date, datetime
from Resources import connections



# Import file
def import_file(file):
    import_file = pd.read_csv(file)
    df = pd.DataFrame(import_file)

    return df


# Parse file
def parse_file(df_input):

    # Filter for 'Status' = 4
    df_input = df_input.loc[df_input['Status'] == 4]

    # Create blank df to paste into
    df = pd.DataFrame()

    try:
        df['EmployeeNumber'] = df_input['Student Email'].str.lower() # Remove for prod (because of sql)
        # df['EmployeeNumber'] = '' # Uncomment for prod

        df['ActivityCode'] = df_input['ActivityCode']
        # df['ClassStartDate'] = ''


        # Reformat dates -per chat gpt
        # df['DeleteThisDate'] = pd.to_datetime(df_input['CompletionDate'], format='%m/%d/%Y %H:%M')
        df['strDate'] = df_input['CompletionDate']
        df['DeleteThisDate'] = df['strDate'].str[:-6]
        df['CompletionDate'] =  pd.to_datetime(df['DeleteThisDate'])
        df['DataSource'] = 'TraCorp'

        # Drop DataSource column
        df = df.drop(['DeleteThisDate'], axis=1)


        # Remove trailing whitespaces -per chat gpt
        df['EmployeeNumber'] = df['EmployeeNumber'].str.strip()
        df['ActivityCode'] = df['ActivityCode'].str.strip()

        # df['CompletionDate'] = df['RegistrationDate']

        # df['FirstLaunchDate'] = ''
        # df['Score'] = ''
        # df['Passed'] = '1'
        # df['CancellationDate'] = ''
        # df['PaymentTerm'] = ''
        # df['Cost'] = ''
        # df['Curency'] = ''
        # df['Timezone'] = 'America/Phoenix'
        # df['Status'] = '4'
        # df['Notes'] = ''
        # df['SubscriptionSourceActivityCode'] = ''
        # df['SubscriptionSourceActivityStartDate'] = ''
        # df['ElapsedTime'] = ''
        # df['CompletionStatus'] = '1'
        # df['Location_Name'] = ''
        # df['Slotstart_Date'] = ''
        # df['Slotend_Date'] = ''
        # df['EmpID'] = df_input['Student Username']
        # df['DataSource'] = 'TraCorp'

        # # Sort by columns
        # df = df.sort_values(by = ['EmployeeNumber', 'ActivityCode', 'DeleteThisDate'], ascending=[True, True, True])

        # # Remove 'DeleteThisDate' column
        # df = df.drop(['DeleteThisDate'], axis=1)

        # # Delete duplicate rows
        # df = df.drop_duplicates(subset=['EmpID', 'ActivityCode', 'CompletionDate'], keep='first')

        # # Remove 'EmpID' column
        # df = df.drop(['EmpID'], axis=1)

    except: print("Failure: parse_file()")

    else: 
        print("Success: parse_file()")
        print(df)
        # input()
    return df



# Export CSV
def export_csv(df_input, file_out):
    # declare date to append to end of file name
    time_stamp = datetime.now()

    this_year = time_stamp.strftime("%Y")
    # print(this_year)
    this_month = time_stamp.strftime("%m")
    # print(this_month)
    this_day = time_stamp.strftime("%d") 
    # print(this_day)
    this_hour = time_stamp.strftime("%H") 
    # print(this_hour)
    this_minute = time_stamp.strftime("%M") 
    # print(this_minute)
    this_second = time_stamp.strftime("%S") 
    # print(this_second)

    right_now = this_year + this_month + this_day + this_hour + this_minute + this_second

    # print(right_now)



    csv_output_file = file_out + right_now + ".csv"

    try:
        df_input.to_csv(csv_output_file, sep=',', index=False)

    except:
        print("Failure: export_csv()")
    
    else:
        print("Success: export_csv()")






# Main
def main():

    # Define vars
    inputFile = connections.input_file
    # reportFile = connections.report_file
    outputFile = connections.output_file


    # Execute functions

    # Input file
    df_input_File = import_file(inputFile)
    df_inputFile = parse_file(df_input_File)

    # Report file
    # df_input_Report = import_report(reportFile)
    # df_inputReport = parse_report(df_input_Report)

    # Merge dfs & remove duplicates
    # df_merged = merge_dfs(df_inputFile, df_inputReport)

    # Export CSV
    csv_file = export_csv(df_inputFile, outputFile)


    print("DONE")
    





# Run main
if __name__ == "__main__":

    # Run main
    main()