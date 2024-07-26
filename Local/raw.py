import glob
import os
import os.path
import csv
import pandas as pd
from datetime import date, datetime, timedelta
from Resources import connections



# Import file
def import_file(file):

    pd_csv = pd.read_csv(file)
    df = pd.DataFrame(pd_csv)

    return df



# Parse df
def parse_file(dataframe):

    # Filter for 'Status' = 4
    df_passed = dataframe.loc[dataframe['Status'] == 4]


    # Filter for date
    df_passed.loc[:, 'CompletionDate'] = pd.to_datetime(df_passed['CompletionDate'])
    yesterday = datetime.now() - timedelta(days=4)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    df_filtered = df_passed[df_passed['CompletionDate'] >= yesterday]




    # # Test - print column types
    # print(df_filtered)
    # print("df_passed: " + str(len(df_passed)))
    # print("df_filtered: " + str(len(df_filtered)))
    # input('Press CTRL + C')


    # Create blank df to paste into
    df = pd.DataFrame()

    try:
        df['EmployeeEmail'] = df_filtered['Student Email'].str.lower()
        df.loc[:, 'EmployeeEmail'] = df['EmployeeEmail'].str.strip()
        df['ActivityCode'] = df_filtered['ActivityCode'].str.strip()
        # df['CompletionDate'] = df_filtered['CompletionDate']

        df['CompletionDate'] = pd.to_datetime(df_filtered['CompletionDate'])
        df.loc[:, 'CompletionDate'] = df['CompletionDate'].dt.date
        df['Score'] = df_filtered['Score']
        df['EmpID'] = df_filtered['Student Username']
        df['DataSource'] = 'Tracorp'

    except:
        print("FAIL: parse_file()")

    else:
        print(df)
        print(df.dtypes)
        print("SUCCESS: parse_file()")

    return df









# Main
def main():

    # Define vars
    inputFile = connections.input_file


    # Execute functions

    # Import file
    to_pandas = import_file(inputFile)

    # Parse file
    df = parse_file(to_pandas)



    print("DONE: MAIN")





# Run main
if __name__ == "__main__":

    # Run main
    main()