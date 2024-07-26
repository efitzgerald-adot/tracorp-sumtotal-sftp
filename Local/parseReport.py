import glob
import os
import os.path
import csv
import pandas as pd
from datetime import date, datetime
from Resources import connections


# Import report
def import_report(report):
    import_report = pd.read_csv(report)
    df = pd.DataFrame(import_report)

    # print(df)

    return df


# Parse report
def parse_report(df_input):

    # Create blank df to paste into
    df = pd.DataFrame()

    try:
        # Rename report columns
        df_input.columns.values[1] = "EmployeeNumber"
        df_input.columns.values[2] = "ActivityCode"
        df_input.columns.values[8] = "DeleteThisDate"

        df['EmployeeNumber'] = df_input['EmployeeNumber'].str.lower()
        df['ActivityCode'] = df_input['ActivityCode']
        df['strDate'] = df_input['DeleteThisDate']
        df['CompletionDate'] = pd.to_datetime(df['strDate'])
        df['DataSource'] = 'Report'

        # Remove trailing whitespaces -per chat gpt
        df['EmployeeNumber'] = df['EmployeeNumber'].str.strip()
        df['ActivityCode'] = df['ActivityCode'].str.strip()


        # Drop duplicates
        df = df.drop_duplicates(subset=['EmployeeNumber', 'ActivityCode', 'CompletionDate'], keep=False)

        # Reset index
        df = df.reset_index(drop=True)


    except: print("Failure: parse_report()")

    else: 
        print("Success: parse_report()")

        print(df)

    return df



# Export CSV
def export_csv(df_input, file_out):
    try:
        df_input.to_csv(file_out, sep=',', index=False)

    except:
        print("Failure: export_csv()")
    
    else:
        print("Success: export_csv()")



# Main
def main():

    # Define vars
    # inputFile = connections.input_file
    reportFile = connections.report_file
    outputReport = connections.output_report


    # Execute functions

    # Input file
    # df_input_File = import_file(inputFile)
    # df_inputFile = parse_file(df_input_File)

    # Report file
    df_input_Report = import_report(reportFile)
    df_inputReport = parse_report(df_input_Report)

    # Merge dfs & remove duplicates
    # df_merged = merge_dfs(df_inputFile, df_inputReport)

    # Export CSV
    csv_file = export_csv(df_inputReport, outputReport)


    print("DONE")
    





# Run main
if __name__ == "__main__":

    # Run main
    main()