import pandas as pd
import cx_Oracle
from config import conf
import logging, sys
from geopandas import GeoDataFrame
import geopandas as gp
from itertools import zip_longest
from shapely import wkt
from Tokenizer import Token



logging.basicConfig(**{
    'filename': conf.get('log_file').filename_full,
    'filemode': 'w',
    'format': '%(asctime)s; %(levelname)s: %(message)s',
    'level': logging.INFO,
})
log = logging.getLogger(__name__)
log.info('')

def grouper(n, iterable, fillvalue=None):

    "grouper(3, 'ABCDEFG', 'x')"
    args = [iter(iterable)] * n
    return zip_longest(fillvalue=fillvalue, *args)


class sql_Opener(object):

    '''Object to open CX_oracle db connection & execute SQL statements'''
    def __init__(self, user):

        conn = cx_Oracle.connect(user, threaded=True)
        self.conn = conn
        self.cur  = self.conn.cursor()

    def execute_sql(self, sql):     # handling delete, update, insert and select statements

        try:
            self.cur.prepare(sql)
            data = [rec for rec in self.cur.execute(sql)]
            # self.cur.execute(sql).fetchall()
            return data
        except Exception as e:
            # logging.error(e)
            logging.info("Error on insert: {}".format(e))
            print(e)

    def trunc_sql(self, sql):
        try:
            self.cur.execute(sql)
        except Exception as e:
            # logging.error(e)
            logging.info("Error on truncate: {}".format(e))
            print(e)

    def insert_sql(self, statement, d_list):

        try:
            
            # self.cur.setinputsizes(cx_Oracle.DB_TYPE_JSON)
            self.cur.prepare(statement)
            self.cur.executemany(statement, d_list, batcherrors=True)
            for error in self.cur.getbatcherrors():
                logging.info("Error", error.message, "at row offset", error.offset)
            self.conn.commit()

        except Exception as e:

            print(e)
            logging.info("Uh Ohhh...something happened with the insert: {}".format(e))
            # sys.exit()

    def proc_sql(self, sql, *args):

        try:
            self.cur.callproc(sql, args)

        except Exception as e:
            print(e)
            logging.info("Procedure error {} ".format(e))
            logging.info("Procedure {}".format(sql))
            sys.exit()
    
    def __del__(self):
        self.cur.close()


def get_data(file, cursor):

    data_list = []

    file_data = pd.read_csv(file, sep='|',  dtype={'DISTRICT_ID': str, 'ZIP5': str,
                                                   'SEQUENCE_NBR': str, 'FINANCE_NUMBER': str},
                           skipinitialspace=True, encoding="latin", names=conf.get('headers'), low_memory=False)
   
    file_data = file_data[file_data.LONGITUDE.notnull()]
    file_data['OBJECTID'] = file_data.index + 1

    gdf = GeoDataFrame(
        file_data, geometry=gp.points_from_xy(file_data.LONGITUDE, file_data.LATITUDE))
    gdf = gdf.dropna(subset=['geometry'])


    gdf['SHAPE'] = gdf['geometry']

    gdf.fillna('', inplace=True)

    del gdf['geometry']
    gdf_dict = gdf.to_dict(orient='records')

    for rec in gdf_dict:

        rec['SHAPE'] = rec['SHAPE'].wkt

        argue = [rec[column] for column in conf.get('col_headers')]
        data_list.append(argue)

    grp_list = grouper(1000, data_list)

    for in_data in grp_list:
        cursor.insert_sql(conf.get('insert_proc'), list(in_data))

if __name__ == '__main__':

    sql_sel = sql_Opener(conf.get('un'))

    logging.info("Stopping service...")

    for serv in conf.get('servers'):

        print("Stopping service on {}".format(serv))
        
        logging.info("Stopping service on {}".format(serv))
        tok = Token(conf.get('web_un'), conf.get('web_pw'), conf.get('url').format(serv))
        tok.stop_serv(conf.get('fold'))

    logging.info("Dropping Index....")
    print("dropping Index....")
    
    sql_sel.trunc_sql(conf.get('drop_ind'))
    sql_sel.trunc_sql(conf.get('force_ind'))
    logging.info("Truncating table....")
    print("truncating table....")
    sql_sel.trunc_sql(conf.get('trunc_tbl'))

    print("Processing data ... ")
    get_data(conf.get('transac_file'), sql_sel)

    logging.info("Starting service....")
    print("Staritng service")

    for serv in conf.get('servers'):

        print("Starting service on {}".format(serv))
        logging.info("Starting service on {}".format(serv))
        tok = Token(conf.get('web_un'), conf.get('web_pw'), conf.get('url').format(serv))
        tok.start_serv(conf.get('fold'))





