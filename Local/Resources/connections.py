import pandas as pd
from datetime import date, datetime



# Input files
input_file = "Local/InputFiles/fromTracorp.csv"
output_file = "Local/OutputFiles/toSumtotal"
report_file = "Local/InputFiles/ActivityRecordsAllHistory.csv"
report_xlsx_file = "Local/InputFiles/ActivityRecordsXLSX.xls"
report_xlsx_A = "Local/InputFiles/Successful_TraCorp_Completions_A.xlsx"
report_xlsx_B = "Local/InputFiles/Successful_TraCorp_Completions_B.xlsx"



# Output files
output_report = "Local/OutputFiles/reportModified.csv"
output_file_txt = "Local/OutputFiles/toSumtotal_txt"
tmp_csv_output_file = "Local/OutputFiles/tmp/tmp.csv"




# declare date to append to end of file name
time_stamp = datetime.now()

this_year = time_stamp.strftime("%Y")
this_month = time_stamp.strftime("%m")
this_day = time_stamp.strftime("%d") 
this_hour = time_stamp.strftime("%H") 
this_minute = time_stamp.strftime("%M") 
this_second = time_stamp.strftime("%S") 

right_now = this_year + this_month + this_day + this_hour + this_minute + this_second
