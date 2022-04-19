import os
import cx_Oracle
import datetime
from base64 import b64decode as b64dec


class LogFile(object):
    def __init__(self, folder='logs', logfile_name='vrp_controller.log'):
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


class Db(object):

    def __init__(self, un=None, pw=None, db=None):
        print ('Instantiating database object')
        self.un = un
        self.pw = pw
        self.db = db

        try:
            if pw and db:
                conn = cx_Oracle.connect(user=self.un, password=self.pw, dsn=self.db, threaded=True)
            else:
                conn = cx_Oracle.connect(user=self.un, threaded=True)
        except:
            raise Exception('Unable to connect to the database, invalid credentials.')

        else:
            self.conn = conn

    def get_cursor(self):
        return self.conn.cursor()

    def execute_sql(self, sql):     # handling delete, update, insert and select statements

        cursor = self.get_cursor()

        if 'SELECT' in sql.upper():
            data = [rec for rec in cursor.execute(sql)]

        else:
            data = cursor.execute(sql)
            self.conn.commit()
        cursor.close()
        return data

    def __del__(self):
        print ('Deleting database object')
        self.conn.close()


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


env = detect_env()
print('Environment: {}'.format(env))

_db_conf = {
    'w': {
        'env_name': 'LOCAL',
        # 'un': '/@DDR_ADM',
        'oracle_home': r'c:\Oracle32\product\11.2.0\client_1',
        'tns_admin': r'c:\Users\dhdyk0\PycharmProjects2\VRP\wallets\LOCAL',
        'un': 'DRT_dhdyk0[GSRT_ADM]',
        'pw': b64dec('ZGRydF90ZW1wOA=='),
        'db':'DDR_ADM',
    },
    'd': {
        'env_name': 'DEV',
        'un': '/@DDR_ADM',
        'oracle_home': '/itcommon/oraclient/u00/oracle/product/client/12102',
        'tns_admin': '/gisp/{}gisp/a00shared/apps/vrpController2/wallets/DEV'.format(env)
    },
    't': {
        'env_name': 'SIT',
        'un': '/@DDR_ADM',
        'oracle_home': '/itcommon/oraclient/u00/oracle/product/client/12102',
        'tns_admin': '/gisp/{}gisp/a00shared/apps/vrpController2/wallets/SIT'.format(env)
    },
    'q': {
        'env_name': 'CAT',
        'un': '/@DDR_ADM',
        'oracle_home': '/itcommon/oraclient/u00/oracle/product/client/12102',
        'tns_admin': '/gisp/{}gisp/a00shared/apps/vrpController2/wallets/CAT'.format(env)
    },
    'p': {
        'env_name': 'PROD',
        'un': '/@DDR_ADM',
        'oracle_home': '/itcommon/oraclient/u00/oracle/product/client/12102',
        'tns_admin': '/gisp/{}gisp/a00shared/apps/vrpController2/wallets/PROD'.format(env)
    }
}

#TODO: Check with Unix/Oracle team for wallet instances: may not be necessary 
db_conf = _db_conf.get(env)
os.environ['ORACLE_HOME'] = db_conf.get('oracle_home')
os.environ['TNS_ADMIN']= db_conf.get('tns_admin')


#TODO: Change to something more elegant - May put class in main script
db_instance = Db(**{
        'un': db_conf.get('un'),
        'pw': db_conf.get('pw', None),
        'db': db_conf.get('db', None)
})

log_file = LogFile(logfile_name='VRP_controller_{}.log'.format(datetime.datetime.now().date()))

export = {
    'env_name': _db_conf.get(env, {}).get('env_name'),
    'db': db_instance,
    'log_file': log_file,
    'VRP_gp_url': 'http://{}:6080/arcgis/rest/services/SolveVehicleRoutingProblem/GPServer/Solve%20Vehicle%20Routing%20Problem/submitJob',
    'AM_gp_url': 'http://{}:6080/arcgis/rest/services/DRT/DRT_AddPackageRoute/GPServer/AddPackageReroute/submitJob'
}
