#
### Description
This script downloads two files from external SFTP servers:

File | SFTP URL | Destination
--- | --- | ---
sumTotal.csv | sftp.azdot.gov | .\temp
Successful_TraCorp_Completions.xlsx | sftp24-pci.sumtotalsystems.com | .\temp





#
### Command line arguments
Argument | Alias | Required | Description
--- | --- | --- | ---
--config | -c | YES | Path to the config.ini file
--path | -p | YES | Path to the working directory to be used for script output
--debug | -d | NO | Enable debug logging level 
--verbose | -v | NO | Enable Verbose logging to the console


# 
### Example: 

```.\download-sftp-files.exe --config C:\scripts\tracorp\config.ini --path C:\scripts\tracorp\```


#
### Packaging
To package the python script into a single executable for deployment first install the required packages.

```pip download -r .\requirements.txt -d \\infscrp98001\e\Python\packages```

```pip install --no-index --find-link \\infscrp98001\e\Python\packages -r .\requirements.txt```

Then execute pyinstaller.

```pyinstaller --onefile .\download-sftp-files.py```
