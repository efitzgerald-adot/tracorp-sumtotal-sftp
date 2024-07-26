import glob
import os
import os.path
import csv
import pandas as pd
from datetime import date, datetime
from Resources import connections
import xlrd


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
        df['ClassStartDate'] = ''
        df['RegistrationDate'] = ''

        df['DeleteThisDate'] = pd.to_datetime(df_input['CompletionDate'])
        df['CompletionDate'] = df['DeleteThisDate'].dt.date

        df['FirstLaunchDate'] = ''
        df['Score'] = df_input['Score']
        df['Passed'] = '1'
        df['CancellationDate'] = ''
        df['PaymentTerm'] = ''
        df['Cost'] = ''
        df['Curency'] = ''
        df['Timezone'] = 'America/Phoenix'
        df['Status'] = '4'
        df['Notes'] = ''
        df['SubscriptionSourceActivityCode'] = ''
        df['SubscriptionSourceActivityStartDate'] = ''
        df['ElapsedTime'] = ''
        df['CompletionStatus'] = '1'
        df['Location_Name'] = ''
        df['Slotstart_Date'] = ''
        df['Slotend_Date'] = ''
        df['EmpID'] = df_input['Student Username']
        df['DataSource'] = 'TraCorp'

        # Sort by columns
        df = df.sort_values(by = ['EmployeeNumber', 'ActivityCode', 'DeleteThisDate'], ascending=[True, True, True])

        # Delete duplicate rows
        df = df.drop_duplicates(subset=['EmpID', 'ActivityCode', 'CompletionDate'], keep='first')

        # Reset index
        df = df.reset_index(drop=True)


    except: print("Failure: parse_file()")

    else: 
        print("Success: parse_file()")
        # print(df)
    return df





def import_report(dfr_input):
    # Create blank df to paste into
    df = pd.DataFrame()

    try:
        import_report = pd.read_excel(dfr_input, index_col=None)
        dfr = pd.DataFrame(import_report)

        df['Email'] = dfr['Student ID']
        df['ActivityCode'] = dfr['Activity Code']

        df['DeleteThisDate'] = pd.to_datetime(dfr['Completion/Status Date'])
        df['CompletionDate'] = df['DeleteThisDate'].dt.date

        df['DataSource'] = 'SumTotalReport'

    except:
        print("Failre: import_report()")

    else:
        # print(df)
        print("Success: import_report()")

    return df


def compare_dfs(file, report):
    # Create blank df to paste into
    df_file = pd.DataFrame()
    df_report = pd.DataFrame()

    try:
        df_file['Email'] = file['EmployeeNumber'].str.lower()
        df_file['Email'] = df_file['Email'].str.strip()
        df_file['ActivityCode'] = file['ActivityCode'].str.strip()

        df_file['RegistrationDate'] = ''

        df_file['CompletionDate'] = file['CompletionDate']
        df_file['DataSource'] = 'Tracorp'

        df_report['Email'] = report['Email'].str.lower()
        df_report['Email'] = df_report['Email'].str.strip()
        df_report['ActivityCode'] = report['ActivityCode'].str.strip()
        df_report['CompletionDate'] = report['CompletionDate']
        df_report['DataSource'] = 'SumTotalReport'


        # Restore remaining columns to df_file
        df_file['FirstLaunchDate'] = ''
        df_file['Score'] = file['Score']
        df_file['Passed'] = '1'
        df_file['CancellationDate'] = ''
        df_file['PaymentTerm'] = ''
        df_file['Cost'] = ''
        df_file['Curency'] = ''
        df_file['Timezone'] = 'America/Phoenix'
        df_file['Status'] = '4'
        df_file['Notes'] = ''
        df_file['SubscriptionSourceActivityCode'] = ''
        df_file['SubscriptionSourceActivityStartDate'] = ''
        df_file['ElapsedTime'] = ''
        df_file['CompletionStatus'] = '1'
        df_file['Location_Name'] = ''
        df_file['Slotstart_Date'] = ''
        df_file['Slotend_Date'] = ''
        df_file['EmpID'] = file['EmpID']


        # Merge

        # Left - per chatgpt
        df_merged = df_file.merge(df_report, on=['Email', 'ActivityCode', 'CompletionDate'], how='left', indicator=True)
        df = df_merged[df_merged['_merge'] == 'left_only'].drop(columns=['_merge', 'DataSource_x', 'DataSource_y'])
        df = df.sort_values(by = ['Email', 'ActivityCode', 'CompletionDate'], ascending=[True, True, True])

        # Reset index
        df = df.reset_index(drop=True)

    except:
        print("Failre: compare_dfs()")

    else:
        # print(df)
        print("Success: compare_dfs()")

    return df



# Parse CSV
def parse_csv(file):
    # Create blank df to paste into
    df_date_col = pd.DataFrame()
    df = pd.DataFrame()

    try:
        df_date_col['DateCopy'] = pd.to_datetime(file['CompletionDate']).dt.strftime('%m/%d/%Y') + ' 09:00'

        df_in = pd.DataFrame(file)

        df['EmployeeNumber'] = df_in['Email']
        df['ActivityCode'] = df_in['ActivityCode']
        df['ClassStartDate'] = ''
        df['RegistrationDate'] = ''
        df['CompletionDate'] = df_date_col['DateCopy']
        df['FirstLaunchDate'] = ''
        df['Score'] = df_in['Score']
        df['Passed'] = '1'
        df['CancellationDate'] = ''
        df['PaymentTerm'] = ''
        df['Cost'] = ''
        df['Curency'] = ''
        df['Timezone'] = 'America/Phoenix'
        df['Status'] = '4'
        df['Notes'] = ''
        df['SubscriptionSourceActivityCode'] = ''
        df['SubscriptionSourceActivityStartDate'] = ''
        df['ElapsedTime'] = ''
        df['CompletionStatus'] = '1'
        df['Location_Name'] = ''
        df['Slotstart_Date'] = ''
        df['Slotend_Date'] = ''
        df['EmpID'] = df_in['EmpID']
        df['Email'] = df_in['Email']


    except:
        print("Failure: parse_csv()")

    else:
        # print(df)
        print("Success: parse_csv()")

    return df


# Export CSV
def export_csv(df_input, current_time, file_out):
    csv_output_file = file_out + current_time + ".csv"

    try:
        df_input.to_csv(csv_output_file, sep=',', index=False)

    except:
        print("Failure: export_csv()")
    
    else:
        print("Success: export_csv()")



# Export tmp csv
def export_tmp_csv(file, tmp_file_out):
    try:
        # Remove extra columns
        df = file.drop(['EmpID', 'Email'], axis=1)

        df.to_csv(tmp_file_out, sep='|', index=False)

    except:
        print("Failre: export_tmp_csv()")

    else:
        print("Success: export_tmp_csv()")



# Export TXT
def export_txt(read, write_path):
    time_stamp = connections.right_now
    write = write_path + time_stamp + ".txt"

    try:
        with open(write, "w") as my_output_file:
            with open(read, "r") as my_input_file:
                [my_output_file.write(" ".join(row)+'\n')
                 for row in csv.reader(my_input_file)]
            my_output_file.close()

        # Delete tmp csv
        os.remove(read)

    except:
        print("fail")

    else:
        print("success")
    return write



# Main
def main():

    # Define vars
    inputFile = connections.input_file
    reportFile = connections.report_xlsx_file
    outputFile = connections.output_file
    tmpCsvFile = connections.tmp_csv_output_file
    rightNow = connections.right_now

    # Execute functions

    # Input file
    df_input_File = import_file(inputFile)
    df_inputFile = parse_file(df_input_File)

    # Report file
    df_input_Report = import_report(reportFile)

    # Merge dfs & remove duplicates
    df_merged = compare_dfs(df_inputFile, df_input_Report)

    # Parse CSV
    parsed_csv = parse_csv(df_merged)

    # Export CSV
    csv_file = export_csv(parsed_csv, rightNow, outputFile)

    # Tmp CSV
    csv_tmp = export_tmp_csv(parsed_csv, tmpCsvFile)


    # Export Txt
    txt_file = export_txt(tmpCsvFile, outputFile)

    print("DONE: Main")
    





# Run main
if __name__ == "__main__":

    # Run main
    main()