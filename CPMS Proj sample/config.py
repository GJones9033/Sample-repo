
__author__ = 'FCR3P0'

import os
from configparser import ConfigParser
from ast import literal_eval
import itertools

class LogFile(object):
    def __init__(self, folder='logs', logfile_name='logs.log'):
        self.folder_full = os.path.join(os.path.dirname(os.path.abspath(__file__)), folder)
        self.filename_full = os.path.join(self.folder_full, logfile_name)
        self.ensure_folder(self.folder_full)

    def open(self):
        self.f = open(self.filename_full, 'a')

    def ensure_folder(self, full_path):
        if not os.path.exists(self.folder_full):
            os.makedirs(full_path)

    def write(self, text):
        self.f.write(text)

    def close(self):
        self.f.close()

    def prune_file(self, to):

        #remove lines from the beginning of the file if there are more than 'max_lines'
        f = open(self.filename_full, 'r')
        content = f.readlines()
        f.close()

        f = open(self.filename_full, 'w')
        f.writelines(content[-to:])  # will always take the last "max_lines" and write them back to the file
        f.close()

log_file = LogFile(logfile_name='CPMS_UPDATE.log')

headers = ['AREA_NAME', 'DISTRICT_NAME', 'BOX_NBR', 'DAY_OF_WEEK_SERVICE', 'BOX_ADDRESS', 'CITY', 'STATE', 'ZIP5', 'LABEL_MILITARY_TIME', 'ROUTE_ID', 'SEQUENCE_NBR',\
         'ROUTE_START_TIME', 'LEAVE_STATION_TIME', 'ROUTE_END_TIME', 'BOX_STATUS', 'BOX_TYPE', 'DATA_EXTRACT_DATE', 'LONGITUDE', 'LATITUDE', 'ROUTE', 'LAST_TIME_MULTIPLE_IND',\
        'TRANSACTION_CODE', 'LOCALE_KEY', 'FACILITY', 'FINANCE_NUMBER', 'BOX_LOCATION_DESCRIPTION', 'SERVICE_CLASS', 'LOCATION_TYPE', 'BOX_INSTALL_DATE',\
        'BUFFER_FT', 'XY_TYPE', 'AREA_ID', 'DISTRICT_ID']

col_headers = ['OBJECTID', 'AREA_NAME', 'DISTRICT_NAME', 'BOX_NBR', 'DAY_OF_WEEK_SERVICE', 'BOX_ADDRESS', 'CITY', 'STATE', 'ZIP5', 'LABEL_MILITARY_TIME', 'ROUTE_ID', 'SEQUENCE_NBR',\
                          'ROUTE_START_TIME', 'LEAVE_STATION_TIME', 'ROUTE_END_TIME', 'BOX_STATUS', 'BOX_TYPE', 'DATA_EXTRACT_DATE', 'LONGITUDE', 'LATITUDE', 'ROUTE', 'LAST_TIME_MULTIPLE_IND',\
                          'TRANSACTION_CODE', 'LOCALE_KEY', 'FACILITY', 'FINANCE_NUMBER', 'BOX_LOCATION_DESCRIPTION', 'SERVICE_CLASS', 'LOCATION_TYPE', 'BOX_INSTALL_DATE',\
                          'BUFFER_FT', 'XY_TYPE', 'AREA_ID', 'DISTRICT_ID', 'SHAPE']

insert_proc = '''
            INSERT INTO GIS_ADM.USPS_CPMS_FC (OBJECTID, AREA_NAME, DISTRICT_NAME, BOX_NBR, DAY_OF_WEEK_SERVICE,\
            BOX_ADDRESS, CITY, STATE, ZIP5, LABEL_MILITARY_TIME, ROUTE_ID, SEQUENCE_NBR, ROUTE_START_TIME,\
            LEAVE_STATION_TIME, ROUTE_END_TIME, BOX_STATUS, BOX_TYPE, DATA_EXTRACT_DATE, LONGITUDE, LATITUDE,\
            ROUTE, LAST_TIME_MULTIPLE_IND, TRANSACTION_CODE, LOCALE_KEY, FACILITY, FINANCE_NUMBER, BOX_LOCATION_DESCRIPTION,\
            SERVICE_CLASS, LOCATION_TYPE, BOX_INSTALL_DATE, BUFFER_FT, XY_TYPE, AREA_ID, DISTRICT_ID, SHAPE)
            values(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, \
            TO_TIMESTAMP(:18,'YYYY-MM-DD HH24:MI:SS'), :19, :20, :21, :22, :23, :24, :25, :26, :27, :28, :29, :30,\
             :31, :32, :33, :34, SDE.ST_GEOMETRY(:35, 4326))                
            '''
trun_tbl = '''
            TRUNCATE TABLE GIS_ADM.USPS_CPMS_FC
          '''

drop_ind = '''drop index R39096_SDE_ROWID_UK'''
force_drop_ind = '''drop index A659_IX1 force'''

##CAT Drop index
cat_drop_ind = '''drop index R17988_SDE_ROWID_UK'''
cat_force_ind= '''drop index A146_IX1 force'''

##PROD DROP_index
prod_drop_ind = '''drop index R369157_SDE_ROWID_UK'''
prod_force_ind = '''drop index A1491_IX1 force'''


create_in = '''
            CREATE INDEX sa_idx
             ON sensitive_areas(zone)
            INDEXTYPE IS sde.st_spatial_index
            PARAMETERS('st_grids=1,3,0 st_srid=4326');
            '''

## detect environment
def detect_env():
    # path = r'/gisp/dgisp/a00shared/myapp/mymodule.py'
    path = __file__

    if path[0] == '/':                    # linux
        result = path.split('/')[2][0]
    else:                                 #windows
        result = 'w'

    if result.lower() in ('d','t','q','p','w'):
        return result.lower()
    else:
        raise Exception('Unknown environment: {}'.format(result))

parser = ConfigParser()
parser.read('config.ini')

## SERVER CYLCE
dev_servers =literal_eval(parser['servers']['d'])
sit_servers = literal_eval(parser['servers']['t'])
cat_servers = literal_eval(parser['servers']['q'])
prod_servers = literal_eval(parser['servers']['p'])

conn = 'http://proxy.usps.gov:8080'

d = {
    'w': {'env_name': 'DEV',
          'un': "FCR3P0[GIS_ADM]/XXXXX@TXXX0.XXXXXXGOV",
          'trunc_tbl': trun_tbl,
          'drop_ind': drop_ind,
          'force_ind': force_drop_ind,
          'insert_proc': insert_proc,
          'transac_file': r'E:\CPMS\sit_data\cpmsschd.txt',
          'url': parser['web_vars']['url'],
          'fold': '/USPS/USPS_CPMS.MapServer/',
          'headers': headers,
          'web_un': parser['web_vars']['low_un'],
          'web_pw': parser['web_vars']['low_pw'],
          'col_headers': col_headers,
          'servers': sit_servers,
          'sem' : 200,
          'log_file': log_file,
          'proxy': conn},

    'd': {'env_name': 'DEV',
          'un': parser['sql']['UN'],
          'headers': headers,
          'col_headers': col_headers,
          'drop_ind': drop_ind,
          'force_ind': force_drop_ind,
          'trunc_tbl': trun_tbl,
          'url': parser['web_vars']['url'],
          'fold': '/USPS/USPS_CPMS.MapServer/',
          'insert_proc': insert_proc,
          'web_un': parser['web_vars']['low_un'],
          'web_pw': parser['web_vars']['low_pw'],
          'transac_file': r'/gisp/dgisp/a00shared/ams/cpmsschd.txt',
          'servers': dev_servers,
          'log_file': log_file,
          'sem' : 200,
          'proxy': conn},

    't': {'env_name': 'SIT',
          'un': parser['sql']['UN'],
          'url': parser['web_vars']['url'],
          'headers': headers,
          'col_headers': col_headers,
          'fold': '/USPS/USPS_CPMS.MapServer/',
          'drop_ind': drop_ind,
          'force_ind': force_drop_ind,
          'trunc_tbl': trun_tbl,
          'insert_proc': insert_proc,
          'servers': sit_servers,
          'web_un': parser['web_vars']['low_un'],
          'web_pw': parser['web_vars']['low_pw'],
          'log_file': log_file,
          'transac_file': r'/gisp/tgisp/a00shared/ams/cpmsschd.txt',
           },

    'q': {'env_name': 'CAT',
          'un': parser['sql']['UN'],
          'url': parser['web_vars']['url'],
          'headers': headers,
          'col_headers': col_headers,
          'fold': '/USPS/USPS_CPMS.MapServer/',
          'trun_tbl': trun_tbl,
          'drop_ind': cat_drop_ind,
          'force_ind': cat_force_ind,
          'insert_proc': insert_proc,
          'servers': cat_servers,
          'web_un': parser['web_vars']['up_un'],
          'web_pw': parser['web_vars']['up_pw'],
          'log_file': log_file,
          'transac_file': r'/gisp/qgisp/a00shared/ams/cpmsschd.txt',
          'proxy': conn},

    'p': {'env_name': 'PROD',
          'un': parser['sql']['UN'],
          'url': parser['web_vars']['url'],
          'headers': headers,
          'col_headers': col_headers,
          'fold': '/USPS/USPS_CPMS.MapServer/',
          'drop_ind': prod_drop_ind,
          'force_ind': prod_force_ind,
          'trunc_tbl': trun_tbl,
          'insert_proc': insert_proc,
          'servers': prod_servers,
          'web_un': parser['web_vars']['up_un'],
          'web_pw': parser['web_vars']['up_pw'],
          'log_file': log_file,
          'transac_file': r'/gisp/pgisp/a00shared/ams/cpmsschd.txt',
          'proxy': conn}}



conf = d.get(detect_env())

log = LogFile()
