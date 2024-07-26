import glob
import os.path
import csv
import pandas as pd
from datetime import date, datetime


# Parse File
def parse_file(df1, cnxn, cursor):
    logging.info("Parsing file...")

    # Filter for 'Status' = 4
    df1 = df1.loc[df1['Status'] == 4]

    # Create blank df to paste into
    df = pd.DataFrame()

    try:
        df['EmployeeNumber'] = ''
        df['ActivityCode'] = df1['ActivityCode']
        df['ClassStartDate'] = ''

        df['DeleteThisDate'] = pd.to_datetime(df1['CompletionDate']) # Don't forget to delete this column
        df['RegistrationDate'] = df['DeleteThisDate'].dt.strftime('%m/%d/%Y') + ' 00:00'
        df['CompletionDate'] = df['RegistrationDate']

        df['FirstLaunchDate'] = ''
        df['Score'] = ''
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
        df['EmpID'] = df1['Student Username']
        df['DataSource'] = 'TraCorp'

        # Log Number of Rows in DataFrame
        dfLength = str(len(df.index))
        logging.debug("Number of rows in DataFrame: " + dfLength)


        # Sort by columns
        df = df.sort_values(by = ['EmployeeNumber', 'ActivityCode', 'DeleteThisDate'], ascending=[True, True, True])

        # Remove 'DeleteThisDate' column
        df = df.drop(['DeleteThisDate'], axis=1)

        # Remove 'EmpID' column
        df = df.drop(['EmpID'], axis=1)

        # Remove 'DataSource' column
        df = df.drop(['DataSource'], axis=1)

    except Exception as e:
        logging.critical("Error parsing file")
        logging.critical(e)
    else:
        logging.info("File parsed successfully")

    return df



# Export CSV
def export_csv(df, file):
    logging.info("Exporting file...")
    logging.debug("File path: " + file)

    # Save copy of modified file for debugging
    # Remove File Extention
    extention = os.path.splitext(file)[1]
    logging.debug("File extention: " + extention)
    file = file.replace(extention, "")
    logging.debug("File path without extention: " + file)
    # Add _modified to file name
    
    modified_file = file + "_modified" + extention
    logging.debug("Modified file path: " + modified_file)

    try:
        df.to_csv(modified_file, sep='|', index=False)
    except Exception as e:
        logging.critical("Error exporting file")
        logging.critical(e)
    else:
        logging.info("File exported successfully")
    
    return modified_file



# Convert CSV to TXT
def convert_csv_to_txt(file):
    logging.info("Converting CSV to TXT...")
    # Get File path
    file_path = os.path.dirname(file)
    # Strip .csv from file name and replace with .txt
    # txt_file = file.replace(".csv", ".txt")
    txt_file = os.path.join(file_path, "TracorpTraining.txt")
    logging.debug("TXT file path: " + txt_file)
    try:
        with open(txt_file, "w") as my_output_file:
            with open(file, "r") as my_input_file:
                [my_output_file.write(" ".join(row)+'\n')
                 for row in csv.reader(my_input_file)]
            my_output_file.close()
    except Exception as e:
        logging.critical("Error converting CSV to TXT")
        logging.critical(e)
    else:
        logging.info("CSV converted to TXT successfully")
    return txt_file



# Main
def main(log_file):
    # SQL Server Config
    driver = config['SQLSettings']['driver']
    server = config['SQLSettings']['server']
    database = config['SQLSettings']['database']
    db_user = config['SQLSettings']['user']
    encrypt = config['SQLSettings']['encrypt']
    auth = config['SQLSettings']['auth']

    # SFTP incoming file connection (from TraCorp)
    tc_url = config['SFTPSettingsTC']['sftpTcUrl']
    tc_userName = config['SFTPSettingsTC']['userName']
    tc_key = config['SFTPSettingsTC']['key']
    tc_file = config['SFTPSettingsTC']['file']

    # SFTP incoming report connection (from SumTotal) - to compare & remove duplicates
    st_report = config['SFTPSettingsST']['report']

    # SFTP outgoing file connectino (to SumTotal)
    st_url = config['SFTPSettingsST']['sftpStUrl']
    st_userName = config['SFTPSettingsST']['userName']
    st_key = config['SFTPSettingsST']['key']
    st_file = config['SFTPSettingsST']['file']

    # Email Config
    email_from = config['EmailSettings']['from']
    email_to = config['EmailSettings']['to']
    email_server = config['EmailSettings']['smtpserver']
    email_port = config['EmailSettings']['port']

    logging.debug("Email From: " + email_from)
    logging.debug("Email To: " + email_to)
    logging.debug("Email Server: " + email_server)
    logging.debug("Email Port: " + email_port)

    temp_path = os.path.join(args.path, 'temp') 
    logging.debug("temp path: " + temp_path)
    # Create temp directory if it doesn't exist
    if not os.path.exists(temp_path):
        logging.debug("Temp directory does not exist. Creating...")
        os.makedirs(temp_path)

    # Change working directory to temp directory
    os.chdir(temp_path)
    
    cnxn = None
    cursor = None
    # Connect to SQL Server
    if(args.no_sql):
        logging.info("Skipping SQL Server connection")
    else:
        logging.info("Connecting to SQL Server...")
        try: 
            #cnxn, cursor = connect_to_sql_server(driver, server, database, encrypt, auth)
            cnxn, cursor = connect_to_sql_server(driver, server, database, encrypt, auth, db_user)
        except Exception as e:
            logging.critical("Error connecting to SQL Server")
            logging.critical(e)
        else:
            logging.info("Connected to SQL Server successfully")

    # Get file from SFTP (TraCorp)
    tracorp_file = download_file(tc_url, tc_userName, tc_file, tc_key, temp_path)

    # Download the report file from SFTP (TraCorp)
    report_file = download_file(st_url, st_userName, st_report, st_key, temp_path)

    # Import file
    df1 = import_file(tracorp_file)

    # Parse file
    df = parse_file(df1, cnxn, cursor)


    # Export CSV
    modified_file = export_csv(df, tracorp_file)

    # Convert CSV to TXT
    sumtotal_file = convert_csv_to_txt(modified_file)

    # Upload file to SFTP (SumTotal)
    upload_file(st_url, st_userName, st_file, st_key, sumtotal_file)

    # Archive files
    archive_files(tracorp_file, sumtotal_file, modified_file, report_file)

    # Send Email
    email_log_and_files(email_from, email_to, email_server, email_port,
                        log_file, tracorp_file, sumtotal_file, modified_file)
    
    # Clear temp directory
    # Delete all files in temp directory
    
    for filename in os.listdir(temp_path):
        file_path = os.path.join(temp_path, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            logging.critical('Failed to delete %s. Reason: %s' % (file_path, e))
        else:
            logging.debug("Deleted " + file_path)  
    
    cursor.close()
    cnxn.close()


# Run main
if __name__ == "__main__":
    # Parse arguments
    args = parse_args()

    # Setup logging
    log_file = setup_logging(args.path, args.debug)

    # Log arguments
    logging.debug("Working directory: " + args.path)
    logging.debug("Debug mode: " + str(args.debug))
    logging.debug("No SQL mode: " + str(args.no_sql))
    logging.debug("Configuration file path: " + args.config)


    # Read configuration file
    config = read_config(args.config)

    # Run main
    main(log_file)