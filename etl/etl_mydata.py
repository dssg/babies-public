import pandas as pd 
import numpy as np 
import psycopg2 
import json 
from sqlalchemy import create_engine 
import subprocess, os
from multiprocessing import Process, Pool
import time 
import urllib2 
import requests 

"""
This is the ETL (extract-transform-load) script. It's probably broken, but 
you can debug it.

^_^

Written by: Ian Pan, Rashida Brown

Date created: 06/17/2015
Date updated: 08/24/2015
"""

def print_progress(progress):
    print(progress+' ...\n'+str(datetime.datetime.now()).split('.')[0])

def geocoder(table_name, address_table, conn, engine,
             address_col='add_geo2',batch_size=1000, chunk_size=10**5):
    
    if not os.path.exists('geocode.sh'):
        with open('geocode.sh', 'w') as f:
            f.write('#!/bin/bash\n\n')
            f.write('curl -o temp_coordinates.json -d @temp_addr.out '+
                    'http://54.188.66.178/')

    cur = conn.cursor()
    cur.execute('DROP TABLE IF EXISTS '+table_name)

    address_sql = pd.read_sql('SELECT * FROM '+address_table, conn)
    address = list(set(address_sql[address_col]))
    addr_len = len(address)
    geocoded = pd.DataFrame()

    if addr_len <= batch_size:
        json.dump(address, open('temp_addr.out', 'w'))
        subprocess.call(['./geocode.sh'])
        json_data = pd.read_json('temp_coordinates.json').T 
        json_data = json_data.reset_index() 
        geocoded = json_data.rename(columns = {'index': 'add_geo2'})
    else:
        for i in range(0, addr_len, batch_size):
            temp_addr = address[i:(i+batch_size)]
            json.dump(temp_addr, open('temp_addr.out', 'w'))
            subprocess.call(['./geocode.sh'])
            json_data = pd.read_json('temp_coordinates.json').T
            json_data = json_data.reset_index()
            json_data = json_data.rename(columns = {'index':'add_geo2'})
            geocoded = geocoded.append(json_data, ignore_index=True)     

    add_geo = address_sql.merge(geocoded, how='left', on='add_geo2')

    nlines = len(add_geo)

    if nlines <= chunk_size:
        df.to_sql(table_name, engine, index=False)
    else: 
        for i in range(0, nlines, chunk_size):
            df = add_geo.ix[i:(i+chunk_size),:]
            df.to_sql(table_name, engine, if_exists='append', index=False)


def millis():
    return int(round(time.time()*1000))

def coord2tract(table_name, geo_table, conn, engine, num_procs=50, timeout=20):

    def http_get(url):
        start_time = millis()
        result = {'url': url, 
                  'data': urllib2.urlopen(url, timeout=timeout).read()[:100]}
        print url+' took '+str(millis()-start_time)+' ms'
        return result 

    cur = conn.cursor()
    query = 'SELECT "UNI_PART_ID_I", latitude, longitude FROM ' + geo_table + \
            ' WHERE latitude is NOT NULL'

    add_sql = pd.read_sql(query, conn)
    add_sql = add_sql.drop_duplicates()

    api_link = ('http://data.fcc.gov/api/block/find?format=json&'
                'latitude={}&longitude={}')

    urls = [api_link.format(str(add_sql.latitude.ix[i]), 
                            str(add_sql.longitude.ix[i]))
            for i in add_sql.index]

    pool = Pool(processes=num_procs)

    start_time = millis()
    results = pool.map(requests.get, urls)

    out_json = [] 

    for result in results:
        r = result.json()
        out_json.append((str(r['Block']['FIPS'])))

    out_json = pd.DataFrame(out_json)
    add_sql['FIPS_BLK_GEO'] = ''

    add_sql['FIPS_BLK_GEO'] = out_json[0]
    add_sql.to_csv(table_name+'.csv')
    cur.execute('DROP TABLE IF EXISTS '+table_name)
    add_sql.to_sql(table_name, engine, index=False)

    print('\nTotal took {} ms\n'.format(str(millis()-start_time)))

def main():

    # script start time
    abs_begin_time = datetime.datetime.now() 

    # set up postgres connections
    params = json.load(open('psql_psycopg2.password'))

    try: 
        conn = psycopg2.connect(**params)

    except:
        print('Unable to connect to database')

    conn.autocommit = True 
    cur = conn.cursor()

    engine = create_engine(open('psql_engine.password', 'r').read().strip())

    print_progress('Established connection to database')

    #############################
    # CREATE INDIVIDUAL DATASET #
    #############################

    cur.execute('DROP TABLE IF EXISTS core_birth_info, core_birth_info_rc')
    print_progress('Creating merged individual-level dataset')
    cur.execute(open('sql-scripts/merge_data.sql', 'r').read())

    # prepare addresses
    cur.execute(open('sql-scripts/indiv_address.sql', 'r').read())

    # geocode addresses
    print_progress('Geocoding participant addresses')
    geocoder(table_name='add_geocode_fin', address_table='add_geocode_prep',
             conn=conn, engine=engine)

    # merge geocode info with the rest of the variables
    cur.execute(open('sql-scripts/merge_indiv_geocode.sql', 'r').read())

    # recode data based on our set of rules
    print_progress('Recoding data')
    cur.execute(open('sql-scripts/recode_data.sql', 'r').read())

    # merge birth outcomes with assessment questions
    print_progress('Merging birth outcomes with 711 assessment')
    cur.execute('DROP TABLE IF EXISTS assess711_births')
    cur.execute(open('sql-scripts/build_711_dataset.sql').read())

    print_progress('Merging birth outcomes with 707G assessment')   
    cur.execute('DROP TABLE IF EXISTS assess707g_births')
    cur.execute(open('sql-scripts/build_707g_dataset.sql').read())

    # pivot assessment question tables 
    print_progress('Creating 711 assessment question matrix')
    cur.execute('DROP TABLE IF EXISTS assess711_qmat')
    read_711_query = """ 
    SELECT "UNI_PART_ID_I" AS unique_index_711, 
        "PART_ID_I", "LMP_D", "ACT_DLV_D", 
        "FIRST_ASSESS_D" AS "711_FT_D", 
        "LAST_ASSESS_D" AS "711_LT_D",
        "MOST_ASSESS_D" AS "711_MT_D",
        "QUESTION_N", "QTS_RSLT_T"
        FROM assess711_births 
    """
    assess711 = pd.read_sql(read_711_query, conn)

    assess711_pvt = assess711.pivot(index='unique_index_711', 
                                    columns='QUESTION_N', 
                                    values='QTS_RSLT_T')

    assess711_pvt.replace(['', 'Y', 'N', 'U', 'ATRISK', 'NTATRISK'],
                          [np.nan, 1, 0, 1, 1, 0], inplace=True)

    assess711_pvt.columns = ['711_' + str(i) + '_Q'
                             for i in assess711_pvt.columns.values]

    date_cols = ['unique_index_711', '711_FT_D', '711_LT_D', '711_MT_D'] 
    assess711_dates = assess711[date_cols].drop_duplicates().\
                                           set_index('unique_index_711')
    assess711_pvt = assess711_dates.join(assess711_pvt)
    assess711_pvt.to_sql('assess711_qmat', engine, conn)

    print_progress('Creating 707G assessment question matrix')
    cur.execute('DROP TABLE IF EXISTS assess707g_qmat')
    read_707g_query = """ 
    SELECT "UNI_PART_ID_I" AS unique_index_707g, 
        "PART_ID_I", "LMP_D", "ACT_DLV_D", 
        "FIRST_ASSESS_D" AS "707G_FT_D", 
        "LAST_ASSESS_D" AS "707G_LT_D",
        "MOST_ASSESS_D" AS "707G_MT_D",
        "QUESTION_N", "QTS_RSLT_T"
        FROM assess707g_births
    """ 

    assess707g = pd.read_sql(read_707g_query, conn)

    assess707g_pvt = assess707g.pivot(index='unique_index_707g',
                                      columns='QUESTION_N',
                                      values='QTS_RSLT_T')

    assess707g_pvt.replace(['', 'Y', 'N', 'U'],
                          [np.nan, 1, 0, 1], inplace=True)

    assess707g_pvt.columns = ['707G_' + str(i) + '_Q' 
                              for i in assess707g_pvt.columns.values]

    date_cols = ['unique_index_707g', '707G_FT_D', '707G_LT_D', '707G_MT_D'] 
    assess707g_dates = assess707g[date_cols].drop_duplicates().\
                                             set_index('unique_index_707g')
    assess707g_pvt = assess707g_dates.join(assess707g_pvt)
    assess707g_pvt.to_sql('assess707g_qmat', engine, conn)

    # merge question responses to core dataset
    print_progress('Merging question responses')
    cur.execute(open('sql-scripts/merge_assess_qts.sql', 'r').read())

    # get individual-level census tracts from latlongs
    print_progress('Getting census tracts for individuals')
    coord2tract(table_name='indiv_census_tracts', geo_table='core_birth_info_rc2',
                conn=conn, engine=engine)
    cur.execute(open('sql-scripts/indiv_census_tracts.sql', 'r').read())

    ##############################
    # CREATE GEOGRAPHIC DATASETS #
    ##############################

    print_progress('Creating geographic-level datasets')

    # prepare clinic addresses for geocoding
    cur.execute(open('sql-scripts/clinic_address.sql', 'r').read())

    # geocoding clinic addresses
    print_progress('Geocoding WIC clinics')
    geocoder(table_name='wic_geocode_fin', address_table='wic_add_full_fin',
             conn=conn, engine=engine)
    print_progress('Geocoding FCM clinics')
    geocoder(table_name='fcm_geocode_fin', address_table='fcm_add_full_fin',
             conn=conn, engine=engine)
    print_progress('Geocoding BBO clinics')
    geocoder(table_name='bbo_clinic_geocode_fin', 
             address_table='bbo_clin_add_full_fin',
             conn=conn, engine=engine)

    # get census tracts from latlongs
    print_progress('Getting census tracts for clinics')
    coord2tract(table_name='wic_census_tracts', geo_table='wic_geocode_fin',
                conn=conn, engine=engine)
    coord2tract(table_name='fcm_census_tracts', geo_table='fcm_geocode_fin',
                conn=conn, engine=engine)
    coord2tract(table_name='bbo_census_tracts', 
                geo_table='bbo_clinic_geocode_fin', conn=conn, engine=engine)
    cur.execute(open('sql-scripts/clinic_census_tracts.sql', 'r').read())

    # clean up the census data
    # assumes you have the acs data dump in postgres
    print_progress('Cleaning census data')
    cur.execute(open('sql-scripts/census_cleaning.sql', 'r').read())

    # merge census tract data to 
    # get aggregate birth outcomes
    print_progress('Aggregating birth outcomes')
    cur.execute(open('sql-scripts/county_birth_otc.sql', 'r').read())

    ###############################################
    # MERGE TRACT VARIABLES TO INDIVIDUAL DATASET #
    ###############################################
    print_progress('Merging tract data to individual-level dataset')
    cur.execute(open('sql-scripts/merge_tractvars.sql', 'r').read())

    # we're done!
    conn.close()
    print("DONE \n"+
          str(datetime.datetime.now()).split('.')[0])
    print("RUNTIME: "+str(datetime.datetime.now()-start_time))

