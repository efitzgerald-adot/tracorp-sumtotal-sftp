import glob
import os.path
import csv
import pandas as pd
from datetime import date, datetime
from Resources import connections


# Convert CSV to TXT
def export_txt(read, write_path):
    # Get File path
    # file_path = os.path.dirname(file)
    # Strip .csv from file name and replace with .txt
    # txt_file = file.replace(".csv", ".txt")
    # txt_file = os.path.join(file_path, "TracorpTraining.txt")
    time_stamp = connections.right_now
    write = write_path + time_stamp + ".txt"

    try:
        with open(write, "w") as my_output_file:
            with open(read, "r") as my_input_file:
                [my_output_file.write(" ".join(row)+'\n')
                 for row in csv.reader(my_input_file)]
            my_output_file.close()
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

    # # Tmp CSV
    # csv_tmp = export_tmp_csv(parsed_csv, tmpCsvFile)


    # Export Txt
    txt_file = export_txt(tmpCsvFile, outputFile)

    print("DONE: Main")
    





# Run main
if __name__ == "__main__":

    # Run main
    main()