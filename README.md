# ldc-tracorp-data-transform-script
A component of the process to migrate daily training records from TraCorp to SumTotal (aka the TraCorp SFTP Project). This script reformats the .csv from TraCorp into a .txt to SumTotal

# 
### Converting The Data - The Purpose and Contents of this Repo
The SumTotal data import process requires tabular data in the form of a .txt file. The data is pipe-delimited and requires columns to be in a specific order and data type.
The file type received from TraCorp is .csv. The purpose of this repo is to convert this file into one that is compatible for the SumTotal import. In addition to converting the file type, scripts in this repo also delete unneeded columns, add required columns, convert dates to proper date/time format, filter rows, sort, etc. 

# 
### Getting Started
The script needs 3 connections: 2 SFTP servers, and 1 SQL server. Connection details and credentials are stored in config.ini. To update this file, edit and then execute configCreator.py. configReader.py contains a function that reads data from config.ini. configReader.py is imported into the main reformatTracorp.py file where the read function is used to connect with the proper paramaters. 

#
### Packaging
To package the python script into a single executable for deployment first install the required packages.

```pip download -r .\requirements.txt -d \\infscrp98001\e\Python\packages```

```pip install --no-index --find-link \\infscrp98001\e\Python\packages -r .\requirements.txt```

Then execute pyinstaller.

```pyinstaller --onefile .\reformatTracorp.py```

#
### Command line arguments
Argument | Alias | Required | Description
--- | --- | --- | ---
--config | -c | YES | Path to the config.ini file
--path | -p | YES | Path to the working directory to be used for script output
--debug | -d | NO | Enable debug logging level 
--no-sql | -n | NO | Do not connect to SQL Server
--verbose | -v | NO | Enable Verbose logging to the console

Example: 

```.\reformatTracorp.exe --config C:\scripts\tracorp\config.ini --path c:\scripts\tracorp\```


# 
### Resources
For more project details, [visit the repo wiki](https://github.com/azdot/ldc-tracorp-data-transform-script/wiki)
<br>
For next steps, [visit the Tasks page of the wiki](https://github.com/azdot/ldc-tracorp-data-transform-script/wiki/Tasks)
<br>
[Project SOW - Google Doc](https://docs.google.com/document/d/1jWujbzeZKMM7hdYMTE_dLJWg2ExKc8x8Mv5fa3mihiI/edit?usp=sharing)
<br>
[SumTotal File Import & Export Schedule - Google Sheet](https://docs.google.com/spreadsheets/d/1hQNLt0eK96qxAOj1bk1bC4HoPijUD-rSWl5BwC3ClfQ/edit#gid=0&range=A17:G17)
