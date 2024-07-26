import glob
import os
import os.path
import csv
import pandas as pd
from datetime import date, datetime
from Resources import connections



# Import xlsx report A
def import_reportA(file):
    # Create blank df to paste into
    df = pd.DataFrame()
    import_report = pd.read_excel(file, index_col=None)

    try:
        dfr = pd.DataFrame(import_report)

        df['Email'] = dfr['Student ID']
        df['Email'] = df['Email'].str.strip()

        df['ActivityCode'] = dfr['Activity Code'].str.strip()

        df['DeleteThisDate'] = pd.to_datetime(dfr['Completion/Status Date'])
        df['CompletionDate'] = df['DeleteThisDate'].dt.date

        df['DataSource'] = 'SumTotalReport'

        # Remove 'DeleteThisDate' column
        df = df.drop(['DeleteThisDate'], axis=1)

    except:
        print("Failre: import_report()")

    else:
        # print(df)
        print("Success: import_report()")

    return df


# Import xlsx report B
def import_reportB(file):
    # Create blank df to paste into
    df = pd.DataFrame()

    try:
        import_report = pd.read_excel(file, index_col=None)
        dfr = pd.DataFrame(import_report)

        df['Email'] = dfr['Student ID']
        df['Email'] = df['Email'].str.strip()

        df['ActivityCode'] = dfr['Activity Code'].str.strip()

        df['DeleteThisDate'] = pd.to_datetime(dfr['Completion/Status Date'])
        df['CompletionDate'] = df['DeleteThisDate'].dt.date

        df['DataSource'] = 'SumTotalReport'

        # Remove 'DeleteThisDate' column
        df = df.drop(['DeleteThisDate'], axis=1)

    except:
        print("Failre: import_report()")

    else:
        # print(df)
        print("Success: import_report()")

    return df



# Merge reports
def merge_reports(dfA, dfB):
    try:
        # Left - per chatgpt
        df_merged = dfA.merge(dfB, on=['Email', 'ActivityCode', 'CompletionDate'], how='left', indicator=True)
        df = df_merged[df_merged['_merge'] == 'left_only'].drop(columns=['_merge', 'DataSource_x', 'DataSource_y'])

        df['DataSource'] = 'SumTotalReport'
        df = df.sort_values(by = ['Email', 'ActivityCode', 'CompletionDate'], ascending=[True, True, True])

        # Reset index
        df = df.reset_index(drop=True)

    except:
        print("Failre: merge_reports()")

    else:
        print("Success: merge_reports()")

    return df




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
        df['Email'] = df_input['Student Email'].str.lower()
        df['Email'] = df['Email'].str.strip()
        df['ActivityCode'] = df_input['ActivityCode'].str.strip()

        df['DeleteThisDate'] = pd.to_datetime(df_input['CompletionDate'])
        df['CompletionDate'] = df['DeleteThisDate'].dt.date

        df['Score'] = df_input['Score']
        df['EmpID'] = df_input['Student Username']
        df['DataSource'] = 'Tracorp'

        # Remove 'DeleteThisDate' column
        df = df.drop(['DeleteThisDate'], axis=1)

    except:
        print("Failure: parse_file()")

    else:
        # print(df)
        print("Success: parse_file()")

    return df



# Merge dfs
def merge_dfs(file_df, report_monster_df):

    try:
        # Left - per chatgpt
        df_merged = file_df.merge(report_monster_df, on=['Email', 'ActivityCode', 'CompletionDate'], how='left', indicator=True)
        df = df_merged[df_merged['_merge'] == 'left_only'].drop(columns=['_merge'])

        df = df.sort_values(by = ['Email', 'ActivityCode', 'CompletionDate'], ascending=[True, True, True])

        df = df.loc[df['DataSource_x'] == "Tracorp"]
        df = df.drop(['DataSource_x'], axis=1)
        df = df.drop(['DataSource_y'], axis=1)

        # Reset index
        df = df.reset_index(drop=True)

    except:
        print("Failre: merge_dfs()")

    else:
        # print(df)
        print("Success: merge_dfs()")

    return df



# Parse merge
def parse_merge(df_in):

    # Create blank df to paste into
    df = pd.DataFrame()
    
    df_date_col = pd.DataFrame()
    df_date_col['DateCopy'] = pd.to_datetime(df_in['CompletionDate']).dt.strftime('%m/%d/%Y') + ' 09:00'

    try:
        # Build columns
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
        df['DateSort'] = df_in['CompletionDate']

        # Sort
        df = df.sort_values(by = ['Email', 'ActivityCode', 'DateSort'], ascending=[True, True, True])

        # Delete extra columns
        df = df.drop(['DateSort', 'Email'], axis=1)

        # Reset index
        df = df.reset_index(drop=True)

    except:
        print("Failre: parse_merge()")

    else:
        print(df)
        print("Success: parse_merge()")

    return df



# Main
def main():

    # Define vars
    report_fileA = connections.report_xlsx_A
    report_fileB = connections.report_xlsx_B
    inputFile = connections.input_file


    # Execute functions

    # Input report A
    df_reportA = import_reportA(report_fileA)


    # Input report B
    df_reportB = import_reportB(report_fileB)


    # Merge reports
    df_reports_merged = merge_reports(df_reportA, df_reportB)


    # Input file
    df_input_File = import_file(inputFile)
    df_inputFile = parse_file(df_input_File)


    # Merge dfs
    dfs_merged = merge_dfs(df_inputFile, df_reports_merged)


    # Parse merge
    df = parse_merge(dfs_merged)




    print("DONE: MAIN")


# Run main
if __name__ == "__main__":

    # Run main
    main()