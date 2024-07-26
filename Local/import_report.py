import glob
import os
import os.path
import csv
import pandas as pd
from datetime import date, datetime
from Resources import connections
import xlrd


# Import report
def import_report(report):
    import_report = pd.read_excel(report)
    df = pd.DataFrame(import_report)

    print(df)

    return df



# Main
def main():

    # Define vars
    inputFile = connections.input_file
    reportFile = connections.report_xlsx_file
    outputFile = connections.output_file

    # Execute functions

    # Report file
    df_input_Report = import_report(reportFile)
    df_inputReport = parse_report(df_input_Report)

    print("SUCCESS DONE")
    





# Run main
if __name__ == "__main__":

    # Run main
    main()