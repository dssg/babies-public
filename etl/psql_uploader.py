import psycopg2
from sqlalchemy import create_engine 
import json
import pandas as pd 
import subprocess, os
import re 
import argparse 
from argparse import ArgumentParser

argparse_desc = ('PostgreSQL uploader for CSV files\n'
                 'usage: python psql_uploader.py [options] -i filename '
                 '-t table')

parser = ArgumentParser(description=argparse_desc)
parser.add_argument('-i', '--input', type=str, help='path to csv file')
parser.add_argument('-t', '--table', type=str, help='name of SQL table')
parser.add_argument('-s', '--schema', type=str, 
                    help='name of schema if not using public')
parser.add_argument('-o', '--overwrite', dest='overwrite', 
                    action='store_true', 
                    help='overwrite existing table')
parser.set_defaults(overwrite=False, schema='public')
args = parser.parse_args()

# connect to db
params = json.load(open('psql_psycopg2.password'))
try:
    conn = psycopg2.connect(**params)
except:
    print('Unable to connect to database')

conn.autocommit = True 
cur = conn.cursor() 

# engine = create_engine(open('psql_engine.password', 'r').read().strip())

cq = ('SELECT EXISTS(SELECT * FROM information_schema.tables '
      'WHERE table_name=%s)')
cur.execute(cq, (args.table,))

if cur.fetchone()[0]:
    print(args.table+' exists...\n')
    if args.overwrite:
        print('Overwrite argument specified!')
        print('Deleting '+args.table+'...\n')
        del_table = 'DROP TABLE ' + args.table 
        cur.execute(del_table)
    else:
        print('Appending to '+args.table+'...\n')
else:
    print(args.table+' does not exist...\n')
    print('Creating '+args.table+'...\n')

    col_names = pd.read_csv(args.input, nrows=1).columns.values
    make_table = 'CREATE TABLE ' + args.table + '('
    for i in col_names:
        i_new = '"'+i+'"'
        if bool(re.search(r'(?i)_I$|_I,|_C$|_C,', i)):
            make_table += i_new+' text,'  
        elif bool(re.search(r'(?i)_N$|_N,', i)):
            make_table += i_new+' decimal,'
        elif bool(re.search(r'(?i)_D$|_D,', i)):
            make_table += i_new+' date,'
        else:
            make_table += i_new+' text,'

    make_table = make_table[:-1]
    make_table += ');'

    cur.execute(make_table)

env = os.environ.copy()
env['PGPASSWORD'] = params['password']
bash_call = 'psql -h '+params['host']+' -U '+params['user']+' -d '+\
            params['database']+' -c "\COPY '+args.schema+'.'+args.table+\
            ' FROM '+args.input+' WITH CSV HEADER;"'
subprocess.call(bash_call, env=env, shell=True)

conn.close()

## this is for uploading in chunks from csv directly
## issues with to_sql type inferring because we had numeric codes

# nlines = subprocess.check_output('wc -l %s' % args.input, shell=True)
# nlines = int(nlines.split()[0])

# if args.append_exist:
#     if not cur.fetchone()[0]:
#         raise Exception('The specified table does not exist')
#     else:
#         if nlines <= args.chunk:
#             df = pd.read_csv(args.input)
#             df.to_sql(args.table, engine, index=False, if_exists='append',
#                       dtype=type_dict)
#         else:
#             for i in range(0, nlines, args.chunk):
#                 df = pd.read_csv(args.input, nrows=chunk, skiprows=i)
#                 df.columns = col_names 
#                 df.to_sql(args.table, engine, if_exists='append', index=False,
#                           dtype=type_dict)
# else:
#     if cur.fetchone()[0]:
#         raise Exception('This table already exists in your database')
#     else:
#         if nlines <= args.chunk:
#             df = pd.read_csv(args.input)
#             df.to_sql(args.table, engine, index=False, dtype=type_dict)
#         else:
#             for i in range(0, nlines, args.chunk):
#                 df = pd.read_csv(args.input, nrows=chunk, skiprows=i)
#                 df.columns = col_names
#                 df.to_sql(args.table, engine, if_exists='append', index=False,
#                           dtype=type_dict)





