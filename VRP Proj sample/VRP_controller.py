import os
import json
import logging
import datetime
import requests
from flask import Flask, request
from VRP_controller_config import export as conf

app = Flask(__name__)

logging.basicConfig(**{
    'filename': conf.get('log_file').filename_full,
    'format': '%(asctime)s; %(name)s; %(levelname)s; %(message)s',
    'level': logging.INFO,
})

logger = logging.getLogger(__name__)
logger.info('Environment: {}'.format(conf['env_name']))
logger.info('Start processing.')

def get_server(db_instance, env):
    sql = """
                select server_name
                  from  (
                        select server_name from gsrt_adm.gis_servers_t
                    where env = '{}' and active = 1
                    ORDER BY DBMS_RANDOM.VALUE
                  )
                where rownum = 1
            """.format(env)
    try:
        random_server = db_instance.execute_sql(sql)[0][0]
    except IndexError:
        logger.error('Unable to retrieve server name.')
        raise IndexError('Unable to retrieve a server name. Env: {}'.format(env))
    return random_server


def get_request_data():
    request_data = {key: value for key, value in request.form.items()}
    logger.debug('Request data: {}'.format(request_data))

    return request_data


def submit_job(request_data, db, env):
    server_name  = get_server(db, env)

    logger.debug('Submitted to solver: {}'.format(request_data))

    # define submit url based on the AM flag
    if request_data.get('AM'):
        submitURL = conf.get('AM_gp_url').format(server_name) + '?f=json'
    else:
        submitURL = conf.get('VRP_gp_url').format(server_name) + '?f=json'

    # removes AM key from the data submitted to the solver
    request_data.pop('AM')

    response = requests.post(submitURL, data = request_data)
    logger.debug('Solver server: {} Response: {}'.format(server_name, response))
    resp_json = json.loads(response.text)

    gp_id = resp_json.get('jobId')
    gp_status = resp_json.get('jobStatus')

    job_submitted_sql = """ insert into gsrt_adm.routing_controller_t
                (gp_id, routing_server, dtm, status, status_details, adds_mods)
                values('{}', '{}', SYSDATE, '{}', '{}', {})
                """.format(gp_id, server_name, gp_status[:50], gp_status[:255], request_data.get('AM',0))

    db.execute_sql(job_submitted_sql)

    job_id_sql    =   """select job_id
                      from gsrt_adm.routing_controller_t
                      where gp_id ='{}'""".format(gp_id)

    job_id_raw = db.execute_sql(job_id_sql)

    job_id = str(job_id_raw[0][0])

    return (gp_status, job_id)


def checkStatus(jobid, db):
    serv_id_sql = """ select routing_server, gp_id, adds_mods
                        from gsrt_adm.routing_controller_t
                        where job_id = {}
                    """.format(jobid)
    sel_obj = db.execute_sql(serv_id_sql)

    server =  sel_obj[0][0]
    gpID   =  sel_obj[0][1]
    gpAdds =  sel_obj[0][2]

    if server:

        # assign url to VRP solver of Adds/Mods depending on the gpAdds value
        url = conf.get('AM_gp_url').format(server) if gpAdds else conf.get('VRP_gp_url').format(server)

        # remove 'submitJob' from the url and append jobs/gpId
        jobURL = '{}{}'.format(url.replace('submitJob', ''), 'jobs/{}'.format(gpID))

        resp = requests.post(jobURL, data={'f': 'json'})

        jsonResp = json.loads(resp.text)

        status = jsonResp['jobStatus'] if 'jobStatus' in jsonResp else 'NoResponse'
        dictResults = {}
        if status =='esriJobSucceeded':
            for resultType in ['out_unassigned_stops', 'out_stops', 'out_routes', 'out_directions']:
                urlResult = jobURL + '/results/' + resultType
                resultResp = requests.post(urlResult, data={'f': 'pjson'})
                resJson = json.loads(resultResp.text)
                dictResults[resultType] = resJson

        # update status in the db
        # TODO: update only if it has changed
        updateStatus(status, jobid, db)

    return {'status':status, 'json': dictResults} if server else 0


def updateStatus(status, jobid, db):
    update_sql = """update gsrt_adm.routing_controller_t
                    set status = '{0}',
                        status_details = '{0}'
                    where job_id = {1}""".format(status, jobid)

    db.execute_sql(update_sql)


def build_routes(rte_conf):
    routes = []
    for i in range(1, rte_conf.get('number_of_routes')+1):
        route = {'attributes':
            {
                'Name': '{}{}'.format(rte_conf.get('rte_type'),i),
                'StartDepotName': rte_conf.get('depot_name'),
                'EndDepotName': rte_conf.get('depot_name'),
                'EarliestStartTime': '',
                'LatestStartTime': '',
                'CostPerUnitTime': 1,
                'Capacities': 1000,
                'FixedCost': 1000000,
                'OvertimeStartTime': 0,
                'CostPerUnitOvertime': '35.5',
                'MaxOrderCount': 1000,
                'MaxTotalTime': int(rte_conf.get('rte_duration')) * 60  # 8 hours in minutes
            }
        }
        routes.append(route)
    return json.dumps({'features': routes})


def get_depot_name(depots_json):
    depots = json.loads(depots_json)
    return depots.get('features',[None])[0].get('attributes',{}).get('Name', 'UnknownDepot')


def format_data_for_vrp_solver(request_data):

    logger.debug('Request type: {} data: {}'.format(type(request_data),request_data))
    settings = json.loads(request_data.get('Settings'))
    logger.debug('Settings: '.format(settings))

    rte_conf = {}  # route configuration object
    rte_conf['rte_type'] = settings.get('routeType')
    rte_conf['rte_start_number'] = settings.get('startingRouteNumber')
    rte_conf['rte_start_time'] = settings.get('routeStartTime')
    rte_conf['rte_duration'] = settings.get('routeDuration')
    rte_conf['number_of_routes'] = 100
    rte_conf['depot_name'] = get_depot_name(request_data.get('Depots'))

    # routes are optional parameter - the logic below builds the routes if they are not passed
    passed_in_routes = request_data.get('Routes') or ''

    # the VRP solver expects the following keys:
    solver_input = {}
    solver_input['orders'] = request_data.get('Orders')
    solver_input['depots'] = request_data.get('Depots')

    solver_input['routes'] = build_routes(rte_conf) if len(passed_in_routes) < 3 else passed_in_routes
    solver_input['breaks'] = request_data.get('breaks', '')
    solver_input['time_units'] = request_data.get('time_units') or 'Minutes'
    solver_input['distance_units'] = request_data.get('time_units') or 'Miles'
    solver_input['default_date'] = int(datetime.datetime.now().timestamp())
    solver_input['uturn_policy'] = settings.get('uturn_policy', 'ALLOW_DEAD_ENDS_AND_INTERSECTIONS_ONLY')
    solver_input['restrictions'] = settings.get('restrictions', '')
    solver_input['populate_directions'] = 'true' if settings.get('directions') else 'false'
    solver_input['overrides'] = '{"DiagnosticLevel": 1, "OptimizeForLocalOrders": 1}'
    solver_input['AM'] = settings.get('AM')  #this key will be removed before sent to the solver
    solver_input['f'] = 'json'

    return solver_input


@app.route("/", methods=["POST"])
def main():

    db_instance = conf.get('db')
    request_data = get_request_data()
    logger.info('Process id: {}, Request data: {}'.format(os.getpid(), request_data))

    result_dict = {}
    result_dict["results"] = []

    # determine whether is an original submit or check for job status
    jobid = request_data.get('JobId')

    results = []
    if not jobid:  # submitting a new job; in the submit_job function, it determines whether is a regular or Adds/Mods job
        logger.info('Process id: {}, New job submitted.'.format(os.getpid()))
        response = submit_job(**{
            'request_data': format_data_for_vrp_solver(request_data),
            'db': db_instance,
            'env': conf.get('env_name'),
        })

        # submit_response looks like this: (gp_status, job_id)
        results.append({"paramName": "JobID2", "dataType": "GPString", "value": response[1]})
        results.append({"paramName": "Status", "dataType": "GPString", "value":response[0]})
        results.append({"paramName": "Json", "dataType": "GPString", "value": ""})

        return json.dumps({'results': results, 'messages': []})

    else:   # checking status of an existing job; if job succeeded, return results, if not, return the status
        logger.info('Process id: {}, Job status requested. Jobid: {}'.format(os.getpid(), jobid))

        # checkStatus returns a dict: {'status': 'abc', 'json': results}
        response = checkStatus(jobid, db_instance)

        results.append({"paramName": "JobID2", "dataType": "GPString", "value": jobid})
        results.append({"paramName": "Status", "dataType": "GPString", "value":response.get('status')})
        results.append({"paramName": "Json", "dataType": "GPString", "value": response.get('json')})
        return json.dumps({'results': results, 'messages': []})


@app.route("/info", methods=["GET"])
def controller_site():
    return '<h2>VRP Controller site</h2> <br/>Use POST for submitting jobs.'


if __name__ == '__main__':
    app.run(port=5678)
