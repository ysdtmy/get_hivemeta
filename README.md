# gethivetblmeta
python scripts for getting table meta data from HIVE.
Metadata of all tables in the specified database is outputted to the csv/tsv file.
 
# Requirement
- Hortonworks Hive ODBC Driver
- pyodbc
- pandas

# Usage

using DNS
``` shell:
python get-tblcol.py -n [DNSname] -d [databasename] [outputfilepath]
```
using Driver
``` shell:
python get-tblcol.py -a [IPaddress] -po [port] -u [user] -p[password] [outputfilepath]
```

options

```-t [tablename]``` specify table to get metadata

```--tab```  outputs as tsv
