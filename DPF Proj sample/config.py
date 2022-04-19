import os, sys
import itertools, servers

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
        # remove lines from the beginning of the file if there are more than 'max_lines'
        f = open(self.filename_full, 'r')
        content = f.readlines()
        f.close()

        f = open(self.filename_full, 'w')
        f.writelines(content[-to:])  # will always take the last "max_lines" and write them back to the file
        f.close()


log_file = LogFile(logfile_name='DPF_Cleanse_log')



## detect environment
def detect_env():
    # path = r'/gisp/dgisp/a00shared/myapp/mymodule.py'
    path = __file__

    if path[0] == '/':  # linux
        result = path.split('/')[2][0]
    else:  # windows
        result = 'w'

    if result.lower() in ('d', 't', 'q', 'p', 'w'):
        return result.lower()
    else:
        raise Exception('Unknown environment: {}'.format(result))



## SERVER CYLCE
dev_servers = itertools.cycle(servers.DEV)
sit_servers = itertools.cycle(servers.SIT)
cat_servers = itertools.cycle(servers.CAT)
prod_servers = itertools.cycle(servers.PROD)


orig_url = r'https://{}:6443/arcgis/rest/services/Tools/ESRI_GEOCODER/GeocodeServer/geocodeAddresses'
# redo_url  = r'https://{}:6443/arcgis/rest/services/Tools/CLEANSER_GEOCODER_2/GeocodeServer/geocodeAddresses'


cols = ['DEL_SYS_ID', 'ADDRESS', 'CITY', 'STATE', 'ZIP_CODE', 'LAT', 'LON', 'ROOFTOP_LAT', 'ROOFTOP_LON', 'LOCATOR', 'SCORE', 'LOAD_FREQUENCY']
index_cols = ['ADDRESS', 'CITY', 'STATE', 'ZIP_CODE', 'DEL_SYS_ID', 'CRID_ID']

##Select query to pull address information
weekly_select = """
                SELECT DISTINCT DEL_SYS_ID, OBJECTID, ADDRESS, CITY, STATE, ZIP_CODE, CRID_ID
                from CORE_ADM.DPFS_GEOCODE_WEEKLY_VW
                order by zip_code
                """

big_select = '''
               SELECT DISTINCT DEL_SYS_ID, OBJECTID, ADDRESS, CITY, STATE, ZIP_CODE
               FROM CORE_ADM.ALL_DPFS_GEOCODE_VW
               where rownum <=20000          
               order by zip_code
               '''
               
##KEEP FOR CLIENTS REQUEST               
## --where del_sys_id not LIKE '%1999%' and state != 'PR' and state != 'VI' and state is not NULL and rownum < 250000
## where state in ('NY', 'MA', 'PA', 'RI', 'NJ', 'VA', 'NC', 'ME') and state is not NULL
#--where state != 'PR' and state != 'VI' and ADDRESS is not NULL and state is not NULL
##For local development

insert_sql= '''
            BEGIN
            CORE_ADM.INVALID_DPF_UTILS.STAGE_GEOCODED_DPFS(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12);
            END;                
            '''


trunc_geocoded  = '''
                TRUNCATE TABLE core_adm.stage_geocoded_dpfs
                '''

trunc_ungeocoded = '''
                TRUNCATE TABLE core_adm.stage_ungeocoded_dpfs
                '''



d = {
    'w': {'env_name': 'DEV',
          'core_un': "FCR3XXXXX/XXXX@XXXXXXX.OV",
          'ssl': r'D:\usps_certs\usps_ca_bundle.cer',
          'log_file': log_file,
          'orig_url': orig_url,
          'con_req': 10,
          'chunksize': 20000,
           'cols': cols,
          'in_cols': index_cols,
           'servers': cat_servers,
          'inval_qrt': big_select,
          'inval_wkly': weekly_select,
          'in_state': insert_sql,
          'insert_proc': insert_sql,
          'trunc_geo': trunc_geocoded,
          'trunc_ungeo': trunc_ungeocoded,
          'drop_indx': 'core_adm.invalid_dpf_utils.drop_geocoded_idxs',
          'clean_up_ungeo': 'core_adm.invalid_dpf_utils.cleanup_stage_ungeocoded_dpfs',
          # 'inval_dpf_chk': 'core_adm.invalid_dpf_utils.check_geocoded_zip_dpfs',
          'export_data': 'core_adm.invalid_dpf_utils.build_ams_export_data',
          'input_data': r'E:\TRANSCEND\DPF_Cleanse\undeliverables_05192020.csv',
          'out_data': r'E:\TRANSCEND\DPF_Cleanse\Consol_try_DEV.csv',
          'qrtr': 'QUARTERLY',
           'wkly': 'WEEKLY',
          'in_table': 'UNDELIVERABLE_DPFS',
          'proxy': proxies
          },

    'd': {'env_name': 'DEV',
          'orig_url': orig_url,
          'redo_url': redo_url,
          'core_un': '/@BCORE_USR',
          'log_file': log_file,
          'con_req': 6,
          'chunksize': 20000,
           'cols': cols,
          'in_cols': index_cols,
           'servers': dev_servers,
          'inval_wkly': weekly_select,
          'inval_qrt': big_select,
          'in_state': insert_sql,
          'clean_up_ungeo': 'core_adm.invalid_dpf_utils.cleanup_stage_ungeocoded_dpfs',
          'inval_dpf_chk': 'core_adm.invalid_dpf_utils.check_geocoded_zip_dpfs',
          'export_data': 'core_adm.invalid_dpf_utils.build_ams_export_data',
          'qrtr': 'QUARTERLY',
          'wkly': 'WEEKLY', 
          'ssl': '/gisp/dgisp/a00shared/python/ssl/certs/usps_ca_bundle.cer',
          'out_data': '/gisp/dgisp/a00shared/ams/DPF_Route_Seq.csv',
          'proxy': proxies},

    't': {'env_name': 'SIT',
          'core_un': '/@BCORE_USR',
          'orig_url': orig_url,
          'redo_url': redo_url,
          'log_file': log_file,
          'con_req': 20,
          'chunksize': 20000,
           'cols': cols,
          'in_cols': index_cols,
           'servers': sit_servers,
          'inval_wkly': weekly_select,
          'inval_qrt': big_select,
          'in_state': insert_sql,
          'insert_proc': insert_geo,
          'clean_up_ungeo': 'core_adm.invalid_dpf_utils.cleanup_stage_ungeocoded_dpfs',
          'inval_dpf_chk': 'core_adm.invalid_dpf_utils.check_geocoded_zip_dpfs',
          'export_data': 'core_adm.invalid_dpf_utils.build_ams_export_data',
          'qrtr': 'QUARTERLY',
          'wkly': 'WEEKLY',
          'ssl': '/gisp/tgisp/a00shared/python/ssl/certs/usps_ca_bundle.cer',
          'out_data': '/gisp/tgisp/a00shared/ams/DPF_Route_Seq.csv',
          'proxy': proxies},

    'q': {'env_name': 'CAT',
          'core_un': '/@BCORE_USR',
          'orig_url': orig_url,
          'redo_url': redo_url,
          'log_file': log_file,
          'con_req': 50,
          'chunksize': 20000,
           'cols': cols,
          'in_cols': index_cols,
           'servers': cat_servers,
          'inval_wkly': weekly_select,
          'inval_qrt': big_select,
          'in_state': insert_sql,
          'insert_proc': insert_geo,
          'clean_up_ungeo': 'core_adm.invalid_dpf_utils.cleanup_stage_ungeocoded_dpfs',
          'inval_dpf_chk': 'core_adm.invalid_dpf_utils.check_geocoded_zip_dpfs',
          'export_data': 'core_adm.invalid_dpf_utils.build_ams_export_data',
          'qrtr': 'QUARTERLY',
          'wkly': 'WEEKLY',
          'ssl': '/gisp/qgisp/a00shared/python/ssl/certs/usps_ca_bundle.cer',
          'out_data': '/gisp/qgisp/a00shared/ams/DPF_Route_Seq.csv',
          'proxy': proxies},

    'p': {'env_name': 'PROD',
          'core_un': '/@BCORE_USR',
          'orig_url': orig_url,
          'redo_url': redo_url,
          'log_file': log_file,
          'con_req': 50,
          'chunksize': 30000,
           'cols': cols,
          'in_cols': index_cols,
           'servers': prod_servers,
          'inval_wkly': weekly_select,
          'inval_qrt': big_select,
          'in_state': insert_sql,
          'insert_proc': insert_geo,
          'clean_up_ungeo': 'core_adm.invalid_dpf_utils.cleanup_stage_ungeocoded_dpfs',
          'inval_dpf_chk': 'core_adm.invalid_dpf_utils.check_geocoded_zip_dpfs',
          'export_data': 'core_adm.invalid_dpf_utils.build_ams_export_data',
          'qrtr': 'QUARTERLY',
          'wkly': 'WEEKLY',
          'ssl': '/gisp/pgisp/a00shared/python/ssl/certs/usps_ca_bundle.cer',
          'out_data': '/gisp/pgisp/a00shared/ams/DPF_Route_Seq.csv',
          'proxy': proxies}}

conf = d.get(detect_env())






