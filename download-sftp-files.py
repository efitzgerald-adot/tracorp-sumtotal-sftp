import glob
import os.path
from datetime import date, datetime
import pysftp
import logging
import argparse
import configparser
import shutil




# Parse Arguments
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="Configuration file path")
    parser.add_argument("-p", "--path", help="Working directory")
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



# Main

def main(log_file):

    # SFTP incoming file connection (from TraCorp)
    tc_url = config['SFTP_tracorp']['url']
    tc_userName = config['SFTP_tracorp']['userName']
    tc_key_file = config['SFTP_tracorp']['key']
    tc_key = os.path.join(args.path, tc_key_file)
    tc_file = config['SFTP_tracorp']['file']

    # SFTP incoming report connection (from SumTotal) - to compare & remove duplicates
    st_url = config['SFTP_sumtotal']['url']
    st_userName = config['SFTP_sumtotal']['userName']
    st_key_file = config['SFTP_sumtotal']['key']
    st_key = os.path.join(args.path, st_key_file)
    st_file = config['SFTP_sumtotal']['file']




    temp_path = os.path.join(args.path, 'temp') 
    logging.debug("temp path: " + temp_path)
    # Create temp directory if it doesn't exist
    if not os.path.exists(temp_path):
        logging.debug("Temp directory does not exist. Creating...")
        os.makedirs(temp_path)


    # Clear temp directory   
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


    # Change working directory to temp directory
    os.chdir(temp_path)
    

    # Get file from SFTP (TraCorp)
    tracorp_file = download_file(tc_url, tc_userName, tc_file, tc_key, temp_path)

    # Download the report file from SFTP (TraCorp)
    sumtotal_file = download_file(st_url, st_userName, st_file, st_key, temp_path)

    
    



# Run main
if __name__ == "__main__":
    # Parse arguments
    args = parse_args()

    # Setup logging
    log_file = setup_logging(args.path, args.debug)

    # Log arguments
    logging.debug("Working directory: " + args.path)
    logging.debug("Debug mode: " + str(args.debug))
    logging.debug("Configuration file path: " + args.config)


    # Read configuration file
    config = read_config(args.config)

    # Run main
    main(log_file)