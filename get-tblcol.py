#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import sys
import pyodbc
import pandas as pd
import datetime
import argparse


def execdesc(cur, db,tbl):
    print(tbl)
    cur.execute("DESC %s.%s" % (db, tbl))
    meta = cur.fetchall()
    return meta


def gethivemain(outputdir, target, db, dns, address, port, password, user, tab):
    now = datetime.datetime.now()
    runtime = now.strftime('%Y%m%d%H%M%S')

    if tab:
        sep = '\t'
        extension = '.tsv'
    else:
        sep = ','
        extension = '.csv'

    outputfilename = 'hmeta_' + db + '_' + runtime + extension

    dnsmode = True
    if dns is None:
        dnsmode = False

    if dnsmode:
        connectinfo = "DSN={dns};".format(dns=dns)

    else:
        connectinfo = "Driver=Hortonworks Hive ODBC Driver;HOST={address};Port={port};Uid={user};Pwd={password};".format(address=address, port=port, user=user, password=password)

    print('*********Connectinfo********')
    print(connectinfo)
    con = pyodbc.connect(connectinfo, autocommit=True)
    cur = con.cursor()
    cur.execute("SHOW TABLES FROM {db}".format(db=db))
    tbl_lis = cur.fetchall()

    meta_lis = []

    print('***********Database*********')
    print(db)
    print('***********Tables***********')
    if target is None:

        for tbl in tbl_lis:
            meta = execdesc(cur, db, tbl[0])
            for m in meta:
                lis = list(m)

                lis.insert(0, tbl[0])

                meta_lis.append(lis)

    else:
        meta = execdesc(cur, db, target)
        for m in meta:
            lis = list(m)
            lis.insert(0, target)
            meta_lis.append(lis)

    df = pd.DataFrame(meta_lis, columns=['TableName','ColumnName','DataType', ''])

    df.to_csv(os.path.join(outputdir,outputfilename), sep=sep, index=None)

    return True



if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("outputdir", help="directory for output file")

    parser.add_argument('-n','--dns', help="DNS name")
    parser.add_argument('-u','--user', help="user name")
    parser.add_argument('-po', '--port', help="port", default='10000')
    parser.add_argument('-p','--password', help="password")
    parser.add_argument('-a', '--address', help="host IP address")
    parser.add_argument('-d', '--db', help="Database name, defalut is 'defalut db'", default='defalut')
    parser.add_argument('-t', '--table', help="get metadata only one specific table")
    parser.add_argument('--tab', help="Output file separatior is tab. default comma",action="store_true", default=False)

    ag = parser.parse_args()
    outputdir = ag.outputdir
    target = ag.table
    db = ag.db
    dns = ag.dns
    address = ag.address
    port = ag.port
    password = ag.password
    user = ag.user
    tab = ag.tab

    gethivemain(outputdir, target, db,  dns, address, port, password, user, tab)
