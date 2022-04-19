import sys, os, json
import aiohttp
import asyncio
from config import conf, log_file 
import pandas as pd
import logging
import cx_Oracle
from itertools import zip_longest
import time
from contextlib import suppress

##Logging
logging.basicConfig(**{
    'filename': conf.get('log_file').filename_full,
    'filemode':  'w',
    'format': '%(asctime)s; %(levelname)s: %(message)s',
    'level': logging.INFO,
})
log = logging.getLogger(__name__)
log.info('')

'''Function to group data to throttle inserts'''
def grouper(n, iterable, fillvalue=None):
    args = [iter(iterable)] * n
    return zip_longest(fillvalue=fillvalue, *args)

class SqlOpener(object):
    '''Object to open CX_oracle db connection & execute SQL statements'''
    def __init__(self, user):

        conn = cx_Oracle.connect(user, threaded=True, encoding="UTF-8", nencoding="UTF-8")
        self.conn = conn
        self.cur  = self.conn.cursor()

    def execute_sql(self, sql, size=None):     # handling delete, update, insert and select statements

        try:
            if 'SELECT' in sql.upper():
                data = pd.read_sql_query(sql, self.conn, chunksize=size)
            else:
                data = self.cur.execute(sql)
                self.conn.commit()
            return data
        except Exception as e:
            # logging.error(e)
            print(e)

    def insert_pandas(self, table):
        pd_table = pd.DataFrame.to_sql(table, self.conn, 'oracle', if_exists='append')
        return pd_table

    def insert_sql(self, statement, list):
        try:
            self.cur.executemany(statement, list)
            self.conn.commit()
        except Exception as e:
            print(e)
            logging.info("Uh Ohhh...something happened with the insert: {}".format(e))
            pass

    def proc_sql(self, sql, *args):
        try:
            self.cur.callproc(sql, (args))
        except Exception as e:
            print(e)
            logging.info("Procedure error {} ".format(e))
            logging.info("Thie procedure failed {}".format(sql))
            sys.exit()
    def __del__(self):
        self.cur.close()

class GeocodeUrl(object):
    '''class for instantiating url'''
    def __init__ (self, url):
        self.url = url
        self.conn = aiohttp.TCPConnector(ssl=False)
        self.session = aiohttp.ClientSession(connector=self.conn)

    def parse_json(self, resp):
        self.json_resp = json.loads(resp)
        return self.json_resp

    async def fetch(self, url, data, session):
        async with session.post(url, data=data, timeout=6000) as resp:
            with suppress(asyncio.TimeoutError):
                resp_txt = await resp.text()

                json_resp = json.loads(resp_txt)
                await asyncio.sleep(1)
                return json_resp

    async def bounder(self, url, sem, data, session):
        async with sem:
            await asyncio.sleep(1)
            return await self.fetch(url, data, session)

    async def get_data(self, data, con_req):
        # object_id = 0

        tasks = []
        sem = asyncio.Semaphore(con_req)
        async with self.session as c_session:
            records = []
            for index, row in data.iterrows():

                address = row['ADDRESS']

                city = row['CITY']
                if city is None:
                    city = ""

                state = row['STATE']
                if state is None:
                    state = ""

                zipcode = row['ZIP_CODE']
                object_id = row['OBJECTID']


                if row.get('OBJECTID') is not None:
                    object_id = row['OBJECTID']
                else:
                    object_id = object_id

                rec_dict = {
                    "attributes": {
                        "OBJECTID": object_id,
                        # "SingleLine": address.rstrip()+", "+city.rstrip()+", "+state.rstrip()+" "+str(zipcode),
                        "SingleLine": address.rstrip()+", "+city+", "+state+" "+str(zipcode),

                    }
                }

            # "Street": address.rstrip(),
            # "City": city.rstrip(),
            # "State": state,
            # "ZIP": zipcode

                logging.info(rec_dict)
                records.append(rec_dict)
            #Group address records by max amount(1000)
            for greg in grouper(1000, records):
                prop = {'records': list(greg)}
                json_dict = {'addresses': json.dumps(prop),
                            'sourceCountry': 'USA',
                            'f': 'pjson'
                            }
                task = asyncio.ensure_future(self.bounder(self.url.format(next(conf.get('servers'))), sem, json_dict, c_session))
                #print(task)
                tasks.append(task)
            response = await asyncio.gather(*tasks)

            return response


def json_parse(result, sheet, cursor):


    insert_vals = []
    Object_ID = []
    add_lst = []
    lat_lst = []
    lon_lst = []
    score_lst = []
    loc_lst = []
    dispX_lst = []
    dispY_lst = []

    # Get X,Y, Locator & score from response
    for item in result:
        # print(item)
        try:
            if item['locations']:
                for val in item['locations']:
                    # print("these are the val {}".format(val))
                    ##TODO: Zero out the lat/lon for oracle insert
                    lat = val['location']['y']
                    if lat == 'NaN':
                        lat = 0

                    lon = val['location']['x']
                    if lon == 'NaN':
                        lon = 0
                    score = val['score']
                    # locator = val['attributes']['Loc_name']

                    disp_X = val['locations'][0]['attributes'].get('DisplayX', "NaN")
                    if disp_X == 'Nan':
                        disp_X = 0

                    disp_Y = val['locations'][0]['attributes'].get('DisplayY', "NaN")
                    if disp_Y == 'Nan':
                        disp_Y = 0

                    locator = val['attributes']['Addr_type']
                    # addr = val['address']
                    results_id = val['attributes']['ResultID']

                    lat_lst.append(lat)
                    lon_lst.append(lon)
                    score_lst.append(score)
                    loc_lst.append(locator)
                    Object_ID.append(results_id)
                    dispX_lst.append(disp_X)
                    dispY_lst.append(disp_Y)

        except KeyError:
            logging.info("There's no coordinates here")
            print("There's no coordinates here")


    print("This is the lon list {}".format(len(lon_lst)))
    print("This is the lat list {}".format(len(lat_lst)))
    print("This is the score list {}".format(len(score_lst)))
    print("This is the loc list {}".format(len(loc_lst)))

    # print(lon_lst)
    # TODO: use fillna to replace 'nan' for Oracle insert
    
    sheet['LOAD_FREQUENCY'] = conf.get('qrtr')

    ##Fill missing data
    proc_dict = {'LON': pd.Series(lon_lst).fillna(0),
                 'LAT': pd.Series(lat_lst).fillna(0),\
                 'SCORE': pd.Series(score_lst).fillna(0),\
                 'ROOFTOP_LAT': pd.Series(dispY_lst).fillna(0),\
                 'ROOFTOP_LON': pd.Series(dispX_lst).fillna(0),\
                 'LOCATOR': pd.Series(loc_lst).fillna('None'),\
                 'OBJECTID': pd.Series(Object_ID).fillna('None')}

    proc_df = pd.DataFrame(proc_dict)

    proc_df['LON'] = proc_df['LON'].fillna(0)
    proc_df['LAT'] = proc_df['LAT'].fillna(0)
    proc_df['ROOFTOP_LON'] = proc_df['LON'].fillna(0)
    proc_df['ROOFTOP_LAT'] = proc_df['LAT'].fillna(0)
    proc_df['SCORE'] = proc_df['SCORE'].fillna(0)
    proc_df['LOCATOR'] = proc_df['LOCATOR'].fillna('None')

    ##Merge data frames on Object ID
    real_merg = pd.merge(sheet, proc_df, how='left', on=['OBJECTID'])

    real_merg['LON'] = real_merg['LON'].fillna(0)
    real_merg['LAT'] = real_merg['LAT'].fillna(0)
    real_merg['ROOFTOP_LON'] = real_merg['ROOFTOP_LON'].fillna(0)
    real_merg['ROOFTOP_LAT'] = real_merg['ROOFTOP_LAT'].fillna(0)
    real_merg['SCORE'] = real_merg['SCORE'].fillna(0)
    real_merg['LOCATOR'] = real_merg['LOCATOR'].fillna('None')

    resp = real_merg.to_dict(orient='records')

    ##TODO: Move to __main__; return 'argue' list
    '''
    Create list for Oracle Insert
    '''
    for row in resp:
        values = {key: row.get(key) for key in conf.get('cols')}
        argue = [values[column] for column in conf.get('cols')]
        insert_vals.append(argue)

    return insert_vals

def insert_deez(insert_stuff, cursor):
    for ins in insert_stuff:
        print(ins)
        # logging.info(ins)
        cursor.insert_sql(conf.get('insert_proc'), list(ins))

def main(input, cursor):

    org_Inst = GeocodeUrl(conf.get('orig_url'))

    #TODO data from dataframe - done through input param

    ##Generate loop
    loop = asyncio.get_event_loop()
    # loop = asyncio.new_event_loop()
    asyncio.set_event_loop(asyncio.new_event_loop())

    results = loop.run_until_complete(org_Inst.get_data(input, con_req=conf.get('con_req')))

    in_data = json_parse(results, input, cursor)
    insert_grp = grouper(2000, in_data)

    insert_deez(insert_grp, cursor)

    loop.close()

if __name__ == '__main__':

    start_time = time.time()
    obj_cursor = SqlOpener(conf.get('core_un'))

    '''Truncate:
     core_adm.stage_geocoded_dpfs 
     core_adm.stage_ungeocoded_dpfs '''

    obj_cursor.execute_sql(conf.get('trunc_ungeocoded'))
    obj_cursor.execute_sql(conf.get('trunc_geocoded'))

    #TODO  uncomment to iterate through larger database chunksize

    '''
    Select query from invalid_dpfs_vw with chunk size 
    '''
    data_in = obj_cursor.execute_sql(conf.get('inval_qrt'), conf.get('chunksize'))

    for data in data_in:
        main(data, obj_cursor)
    '''
    Run post-processing procedures to:
    1) cleanup ungeocoded table 2) validate w/zip bound 3) export data
     '''

    obj_cursor.proc_sql(conf.get('clean_up_ungeo'))
    # obj_cursor.proc_sql(conf.get('inval_dpf_chk'), conf.get('qtr'))

    obj_cursor.proc_sql(conf.get('export_data'), conf.get('qrtr'))
    del(obj_cursor)

    ##TODO: Add delete cursor
    end_time = time.time()
    logging.info("Process time {}".format(int(end_time - start_time)))
    print("Process time {}".format(int(end_time - start_time)))

    #Prune log file
    log_file.prune_file(to=50000)
