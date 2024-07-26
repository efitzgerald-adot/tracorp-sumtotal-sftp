import glob
import os.path
import csv
import pandas as pd
from datetime import date, datetime
import pyodbc
import pysftp
import logging
import argparse
import configparser
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
import shutil

# Parse Arguments


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="Configuration file path")
    parser.add_argument("-p", "--path", help="Working directory")
    parser.add_argument("-n", "--no-sql", help="Execute without connecting to SQL Server", action="store_true")
    parser.add_argument("-d", "--debug", help="Debug mode",
                        action="store_true")
    parser.add_argument("-v", "--verbose", help="Enable verbose console", action="store_true")
    args = parser.parse_args()
    return args

# Setup logging


def setup_logging(path, debug):
    # Get current datetime in YYYYMMDDHHMM format
    now = datetime.now().strftime("%Y%m%d%H%M")
    log_path = os.path.join(path, "logs")
    log_file = os.path.join(log_path, "reformatTracorp_" + now + ".log")
    # Create log directory if it doesn't exist
    if not os.path.exists(log_path):
        logging.debug("Log directory does not exist. Creating...")
        os.makedirs(log_path)

    if debug:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO
    logging.basicConfig(filename=log_file, level=log_level,
                        format='%(asctime)s %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    console = logging.StreamHandler()
    if args.verbose:
        console.setLevel(logging.DEBUG)
    else:
        console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)
    logging.info("Logging started")
    logging.debug("Log file: " + log_file)
    
    return log_file

# Read configuration file


def read_config(configFilePath):
    logging.info("Reading configuration file...")
    logging.debug("Configuration file path: " + configFilePath)

    try:
        config = configparser.ConfigParser()
        config.read(configFilePath)
        logging.info("Configuration file read successfully")
        if(args.debug):
            for section in config.sections():
                logging.debug("Section: " + section)
                for key in config[section]:
                    logging.debug(key + " = " + config[section][key])
        return config
    except Exception as e:
        logging.critical("Error reading configuration file")
        logging.critical(e)


# Download file from SFTP


def download_file(host_url, host_username, file_path, key_path, temp_path):
    logging.info("Downloading file from TraCorp SFTP...")
    logging.debug("SFTP URL: " + host_url)
    logging.debug("SFTP Username: " + host_username)
    logging.debug("SFTP File Path: " + file_path)
    logging.debug("SFTP Key Path: " + key_path)

    try:
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        with pysftp.Connection(host=host_url, username=host_username, private_key=key_path, cnopts=cnopts) as sftp:
            sftp.get(file_path)
            sftp.close()
    except Exception as e:
        logging.critical("Error downloading file from SFTP")
        logging.critical(e)
    else:
        file_name = os.path.basename(file_path)
        file = os.path.join(temp_path, file_name)
        logging.info("File downloaded successfully.")
        logging.debug("File path: " + file)
        return file

# Upload file to SFTP


def upload_file(host_url, host_username, file_path, key_path, file):
    logging.info("Uploading file to SumTotal SFTP...")
    logging.debug("SFTP URL: " + host_url)
    logging.debug("SFTP Username: " + host_username)
    logging.debug("SFTP File Path: " + file_path)
    logging.debug("SFTP Key Path: " + key_path)
    logging.debug("File path: " + file)

    try:
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        with pysftp.Connection(host=host_url, username=host_username, private_key=key_path, cnopts=cnopts) as sftp:
            sftp.chdir(os.path.dirname(file_path))
            sftp.put(file)
            sftp.close()
    except Exception as e:
        logging.critical("Error uploading file to SFTP")
        logging.critical(e)
    
    else:
        logging.info("File uploaded successfully.")


# Connect to SQL Server
# Encrypt authentication using pyodbc: https://learn.microsoft.com/en-us/sql/connect/python/pyodbc/step-3-proof-of-concept-connecting-to-sql-using-pyodbc?view=sql-server-ver16
# Per Muthu


def connect_to_sql_server(driver, server, database, encrypt, auth, user):
    logging.info("Connecting to SQL Server...")
    logging.debug("SQL Server Driver: " + driver)
    logging.debug("SQL Server Server: " + server)
    logging.debug("SQL Server Database: " + database)
    logging.debug("SQL Server Encrypt: " + encrypt)
    logging.debug("SQL Server Authentication: " + auth)
    #logging.debug("SQL Server Connection String: " + 'DRIVER='+driver+';SERVER='+server+';DATABASE=' + database+';ENCRYPT='+encrypt+';AUTHENTICATION=' + auth)
    logging.debug("SQL Server Connection String: " + 'DRIVER='+driver+';SERVER='+server+';DATABASE=' + database+';UID='+user+';Trusted_Connection=yes;')
    logging.debug("Current User: " + os.getlogin())

    cnxn = None
    cursor = None
    # ENCRYPT defaults to yes starting in ODBC Driver 18. It's good to always specify ENCRYPT=yes on the client side to avoid MITM attacks.
    try:
        #cnxn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';DATABASE=' +
        #                      database+';ENCRYPT='+encrypt+';AUTHENTICATION=' + auth)
        cnxn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';DATABASE=' +
                              database+';UID='+user+';Trusted_Connection=yes;')
        cursor = cnxn.cursor()
        logging.info("Connected to SQL Server successfully")
    except Exception as e:
        logging.critical("Error connecting to SQL Server")
        logging.critical(e)

    return cnxn, cursor

# Import file


def import_file(file):
    logging.info("Importing file...")
    logging.debug("File path: " + file)
    df = None
    try:
        import_file = pd.read_csv(file)
        df = pd.DataFrame(import_file)
        logging.info("File imported successfully")

        # Log Number of Rows in DataFrame
        dfLength = str(len(df.index))
        logging.debug("Number of rows in DataFrame: " + dfLength)

    except Exception as e:
        logging.critical("Error importing file")
        logging.critical(e)

    return df


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

        df['DeleteThisDate'] = pd.to_datetime(df1['CompletionDate'], format='%m/%d/%Y %H:%M')
        df['RegistrationDate'] = df['DeleteThisDate']
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


        # Loop through each row in the DataFrame to query 'Employee' db for correct email address based on EIN
        if(args.no_sql):
            logging.info("Skipping SQL Server query...")
        else:
            # Write to log every 5% of rows
            logInt = int(dfLength) / 20
            logInt = round(logInt)
            logging.debug("Log interval: " + str(logInt))

            counter = 0

            for index, row in df.iterrows():
                if counter % logInt == 0:
                    logging.debug("Parsing file... " + str(counter) + "/" + dfLength)

                # Get the UserId from column 2
                user_id = row['EmpID']

                # Query ADOT Master db to get the email address associated with that UserId

                # Pad EIN with leading zeros
                user_id = str(user_id).zfill(9)

                query = f"SELECT TOP (1) EmployeeEmailAddress as Email FROM VW_EmployeeRoster_With_SensitiveInfo WHERE EIN ='{user_id}'"
                #logging.debug("Query: " + query)
                
                # Execute query
                cursor = cnxn.cursor()
                cursor.execute(query)
                
                # Get the email address from the query
                email = cursor.fetchone()
                

                if email is None:
                    email = row['EmployeeNumber']
                else: 
                    email = str(email[0])

                cursor.close()

                # Update the email address in column 1 for that row
                df.at[index, 'EmployeeNumber'] = email
                counter += 1


        # Sort by columns
        df = df.sort_values(by = ['EmployeeNumber', 'ActivityCode', 'DeleteThisDate'], ascending=[True, True, True])

        # Remove 'DeleteThisDate' column
        df = df.drop(['DeleteThisDate'], axis=1)

        # Delete duplicate rows
        df = df.drop_duplicates(subset=['EmpID', 'ActivityCode', 'CompletionDate'], keep='first')

        # Remove 'EmpID' column
        df = df.drop(['EmpID'], axis=1)

    except Exception as e:
        logging.critical("Error parsing file")
        logging.critical(e)
    else:
        logging.info("File parsed successfully")

    return df



# Parse report
def parse_report(df_input):

    import_report = pd.read_csv(df_input)
    df_report = pd.DataFrame(import_report)
    # Create blank df to paste into
    df = pd.DataFrame()

    try:
        # Rename report columns
        df_report.columns.values[1] = "EmployeeNumber"
        df_report.columns.values[2] = "ActivityCode"
        df_report.columns.values[8] = "CompletionDate"


        df['EmployeeNumber'] = df_report['EmployeeNumber'].str.lower()
        df['ActivityCode'] = df_report['ActivityCode']
        df['DeleteThisDate'] = df_report['CompletionDate']
        df['RegistrationDate'] = pd.to_datetime(df['DeleteThisDate'])
        df['CompletionDate'] = df['RegistrationDate']

        # Remove 'DeleteThisDate' column
        df = df.drop(['DeleteThisDate'], axis=1)

    except Exception as e:
        logging.critical("Error parsing Report")
        logging.critical(e)
    else:
        logging.info("Report parsed successfully")

    return df



# Merge dfs & remove duplicates
def merge_dfs(df_left, df_right):
    df = df_left.merge(df_right, how='outer')

    # Convert subsets to same data types
    df['ActivityCode'] = df['ActivityCode'].str.replace(' ', '')

    # Remove trailing whitespaces
    df['EmployeeNumber'] = df['EmployeeNumber'].str.strip()
    df['ActivityCode'] = df['ActivityCode'].str.strip()


    df = df.drop_duplicates(subset=['EmployeeNumber', 'ActivityCode', 'CompletionDate'], keep=False)

    df = df.loc[df['DataSource'] == 'TraCorp']

    # Remove old data (before 6 months ago)
    cutoff_date = pd.Timestamp.now() - pd.DateOffset(months=6)
    df = df[df['CompletionDate'] >= cutoff_date]

    # Reset index
    df = df.reset_index(drop=True)

    # Return dates to regular format
    df['CompletionDate'] = df['CompletionDate'].dt.strftime('%m/%d/%Y') + ' 00:00'
    df['RegistrationDate'] = df['CompletionDate']

    # Drop DataSource column
    df = df.drop(['DataSource'], axis=1)

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



# Email Log and Files

def email_log_and_files(email_from, email_to, email_server, email_port, log_file, tracorp_file, sumtotal_file, modified_file):
    # Email log file
    logging.info("Emailing log and files...")

    try:
        msg = MIMEMultipart()
        msg['From'] = email_from
        msg['To'] = email_to
        msg['Subject'] = "SumTotalLMS_TraCorp Transform and Upload Log"
        email_body = "Please see attached log file and files for SumTotalLMS_TraCorp Transform and Upload."
        

        # Attach log file
        # Test if log file exists
        if os.path.exists(log_file): 
            attachment = open(log_file, "rb")
            part = MIMEBase('application', 'octet-stream')
            part.set_payload((attachment).read())
            attachment.close()
            part.add_header(
                'Content-Disposition', "attachment; filename= %s" % log_file)
            msg.attach(part)
        else: 
            email_body = email_body + "\n\nLog file not found."

        # Attach tracorp file
        # Test if tracorp file exists
        if os.path.exists(tracorp_file):
            attachment = open(tracorp_file, "rb")
            part = MIMEBase('application', 'octet-stream')
            part.set_payload((attachment).read())
            attachment.close()
            part.add_header('Content-Disposition',
                            "attachment; filename= %s" % tracorp_file)
            msg.attach(part)
        else:
            email_body = email_body + "\n\nTraCorp file not found."

        # Attach sumtotal file
        # Test if sumtotal file exists
        if os.path.exists(sumtotal_file):
            attachment = open(sumtotal_file, "rb")
            part = MIMEBase('application', 'octet-stream')
            part.set_payload((attachment).read())
            attachment.close()
            part.add_header('Content-Disposition',
                            "attachment; filename= %s" % sumtotal_file)
            msg.attach(part)
        else:
            email_body = email_body + "\n\nSumTotal file not found."

        # Attach modified file
        # Test if modified file exists
        if os.path.exists(modified_file):
            attachment = open(modified_file, "rb")
            part = MIMEBase('application', 'octet-stream')
            part.set_payload((attachment).read())
            attachment.close()
            part.add_header('Content-Disposition',
                            "attachment; filename= %s" % modified_file)
            msg.attach(part)
        else:
            email_body = email_body + "\n\nModified file not found."

        # Attach email body
        msg.attach(MIMEText(email_body, 'plain'))

        # Send email
        server = smtplib.SMTP(email_server, email_port)
        server.starttls()
        text = msg.as_string()
        server.sendmail(email_from, email_to, text)
        server.quit()
    except Exception as e:
        logging.critical("Error emailing log and files")
        logging.critical(e)
        
    else:
        logging.info("Log and files emailed successfully")



# Archive Files

def archive_files(tracorp_file, sumtotal_file, modified_file, report_file):
    logging.info("Archiving files...")
    # Setup File Archive Paths
    archive_root = os.path.join(args.path, 'archive')
    archive_date = datetime.now().strftime("%Y%m%d%H%M")
    archive_path = os.path.join(archive_root, archive_date)
    logging.debug("Archive path: " + archive_path)

    # Create Archive Root Directory if it doesn't exist
    if not os.path.exists(archive_root):
        logging.debug("Archive root directory does not exist. Creating...")
        os.makedirs(archive_root)

    # Create archive directory if it doesn't exist
    if not os.path.exists(archive_path):
        logging.debug("Archive directory does not exist. Creating...")
        os.makedirs(archive_path)

    # New File Paths
    tracorp_archive = os.path.join(
        archive_path, os.path.basename(tracorp_file))
    sumtotal_archive = os.path.join(
        archive_path, os.path.basename(sumtotal_file))
    modified_archive = os.path.join(
        archive_path, os.path.basename(modified_file))
    report_archive = os.path.join(
        archive_path, os.path.basename(report_file))

    logging.debug("TraCorp archive path: " + tracorp_archive)
    logging.debug("SumTotal archive path: " + sumtotal_archive)
    logging.debug("Modified archive path: " + modified_archive)
    logging.debug("Report archive path: " + report_archive)

    # Copy files to archive directory
    logging.debug("Copying files to archive directory...")

    try:
        logging.debug("Copying " + tracorp_file + " to " + tracorp_archive)
        shutil.copy2(tracorp_file, tracorp_archive)
    except Exception as e:
        logging.critical("Error copying " + tracorp_file + " to " + tracorp_archive)
        logging.critical(e)
    else:
        logging.debug("TraCorp file copied successfully")
    try:
        logging.debug("Copying " + sumtotal_file + " to " + sumtotal_archive)
        shutil.copy2(sumtotal_file, sumtotal_archive)
    except Exception as e:
        logging.critical("Error copying " + sumtotal_file + " to " + sumtotal_archive)
        logging.critical(e)
    else:
        logging.debug("SumTotal file copied successfully")
    try:
        logging.debug("Copying " + modified_file + " to " + modified_archive)
        shutil.copy2(modified_file, modified_archive)
    except Exception as e:
        logging.critical("Error copying " + modified_file + " to " + modified_archive)
        logging.critical(e)
    else:
        logging.debug("Modified file copied successfully")
    try:
        logging.debug("Copying " + report_file + " to " + report_archive)
        shutil.copy2(report_file, report_archive)
    except Exception as e:
        logging.critical("Error copying " + report_file + " to " + report_archive)
        logging.critical(e)
    else:
        logging.debug("Report file copied successfully")



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

    # Parse report
    df_inputReport = parse_report(report_file)

    # Merge dfs & remove duplicates
    df_merged = merge_dfs(df, df_inputReport)

    # Export CSV
    modified_file = export_csv(df_merged, tracorp_file)

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