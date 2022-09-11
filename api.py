#!/usr/bin/env python3
"""
API that returns raw data from a SensorThings Instance with a separate raw_data table. If a query is accessing regular
sensorthings data (all data that is not in raw data table), this API simply acts as a proxy. Otherwise, if a query is
trying to access data in raw_data table, the API accesses this data throuh a PostgresQL connector, formats in in JSON
and sends it back following the SensorThings API standard.

Some of the things to be taken into account:
    -> there are no observation ID in raw data, but a primary key composed by timestamp and datastream_id
    -> To generate a unique identifier for each data point, the following formula is used:
            observation_id = datastream_id * 1e10 + epoch_time


author: Enoc Martínez
institution: Universitat Politècnica de Catalunya (UPC)
email: enoc.martinez@upc.edu
license: MIT
created: 6/9/22
"""
import logging
from argparse import ArgumentParser
import json
import requests
from flask import Flask, request, jsonify, Response
from common import setup_log, SensorthingsDbConnector, GRN, RST, RED, YEL, CYN
import time

app = Flask(__name__)

def get_sta_request(request):
    sta_url = f"{sta_base_url}{request.full_path}"
    log.debug(f"Generic query, fetching {sta_url}")
    resp = requests.get(sta_url)
    code = resp.status_code
    text = resp.text.replace(sta_base_url, service_url)  # hide original URL
    return text, code


@app.route('/<path:path>', methods=['GET'])
def generic(path):
    text, code = get_sta_request(request)
    return Response(text, code, mimetype='application/json')


@app.route('/Observations(<int:observation_id>)', methods=['GET'])
def observations(observation_id):
    try:
        full_path = request.full_path
        opts = process_sensorthings_options(request)

        log.info(f"Received Observations GET: {full_path}")
        if observation_id < 1e10:
            text, code = get_sta_request(request)
            return Response(text, code, mimetype='application/json')
        else:
            datastream_id = int(observation_id/1e10)
            struct_time = time.localtime(observation_id - 1e10*datastream_id)
            timestamp = time.strftime('%Y-%m-%dT%H:%M:%Sz', struct_time)
            filters = f"timestamp = '{timestamp}'"
            data = db.get_raw_data(datastream_id, filters=filters, debug=True, format="list")[0]
            observation = data_point_to_sensorthings(data, observation_id, datastream_id, opts)
            return Response(json.dumps(observation), 200, mimetype='application/json')
    except SyntaxError as e:
        error_message = {
            "code": 400,
            "type": "error",
            "message": str(e)
        }
        return Response(json.dumps(error_message), 400, mimetype='application/json')



def observations_from_raw_dataframe(df, datastream: dict):
    df["resultTime"] = df.index.strftime("%Y-%m-%dT%H:%M:%Sz")
    df = df[["resultTime", "value", "qc_flag"]]

    df.values.tolist()


def data_point_to_sensorthings(data_point: list, observation_id: int, datastream_id: int, opts):
    base_url = args.url
    foi_id = db.datastream_fois[datastream_id]
    timestamp, value, qc_flag = data_point
    t = timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
    observation_id = int(1e10*datastream_id + int(timestamp.strftime("%s")))
    observation = {
        "@iot.id": observation_id,
        "phenomenonTime": t,
        "result": value,
        "resultTime": t,
        "resultQuality": {
            "qc_flag": qc_flag
        },
        "@iot.selfLink": f"{base_url}/Observations({observation_id})",
        "Datastream@iot.navigationLink": f"{base_url}/Datastreams({int(datastream_id)})",
        "FeatureOfInterest@iot.navigationLink": f"{base_url}/FeaturesOfInterest({int(foi_id)})"
    }
    if "select" in opts.keys():
        for key in observation.copy().keys():
            if key not in opts["select"]:
                del observation[key]
    return observation


def data_list_to_sensorthings(data_list, foi_id: int, datastream_id: int, opts: dict):
    base_url = args.url
    n = len(data_list)
    data = {
           "@iot.nextLink": f"{base_url}{opts['full_path']}",
            "value": []
    }

    if "$skip" in data["@iot.nextLink"]:
        data["@iot.nextLink"] = data["@iot.nextLink"].replace(f"$skip={opts['skip']}", f"$skip={opts['skip'] + opts['top']}")
    else:
        data["@iot.nextLink"] = data["@iot.nextLink"].replace(f"?", f"?$skip={opts['top']}&")

    if n < opts["top"]:
        del data["@iot.nextLink"]

    p = time.time()
    for data_point in data_list:
        data["value"].append(data_point_to_sensorthings(data_point, foi_id, datastream_id, opts))
    log.debug(f"{CYN}format db data took {time.time() - p:.03} seconds{RST}")
    return data


def is_date(s):
    """
    checks if string is a date in the format YYYY-mm-ddTHH:MM:SS
    :param s: input string
    :return: True/False
    """
    if len(s) > 19 and s[4] == "-" and s[7] == "-" and s[10] == "T" and s[13] == ":" and s[16] == ":":
        return True
    return False


def sta_option_to_posgresql(filter_string):
    """
    Takes a SensorThings API filter expression and converts it to a PostgresQL filter expression
    :param filter_string: sensorthings filter expression e.g. filter=phenomenonTime lt 2020-01-01T00:00:00z
    :return: string with postgres filter expression
    """
    # try to identify dates
    elements = filter_string.split(" ")
    sta_to_pg_conversions = {
        # map SensorThings' operators with PostgresQL's operators
        "eq": "=", "ne": "!=", "gt": ">", "ge": ">=", "lt": "<", "le": "<=",
        # map SensorThings params with raw_data table params
        "phenomenonTime": "timestamp", "resultTime": "timestamp", "result": "value",
        # order
        "orderBy": "order by"
    }

    for i in range(len(elements)):
        if is_date(elements[i]):  # check if it is a date
            elements[i] = f"'{elements[i]}'"  # add ' around date
        for old, new in sta_to_pg_conversions.items():
            if old == elements[i]:
                elements[i] = new

    return " ".join(elements)


def process_sensorthings_options(request):
    """
    Process the options of a request and store them into a dictionary.
    :param request: api request object
    :return: dict with all the options processed
    """
    __valid_options = ["$top", "$skip", "$count", "$select", "$filter", "$orderBy"]
    sta_opts = {
        "full_path": request.full_path,
        "top": 100,
        "skip": 0,
        "filter": "",
        "orderBy": "resultTime asc"
    }
    params = request.args.to_dict()

    for key, value in params.items():
        if key not in __valid_options:
            raise SyntaxError(f"Unknown option {key}, expecting one of {' '.join(__valid_options)}")
        if key == "$top":
            sta_opts["top"] = int(value)
        elif key == "$skip":
            sta_opts["skip"] = int(params["$skip"])
        elif key == "$count":
            if params["$count"].lower() == "true":
                sta_opts["count"] = True
            elif params["$count"].lower() == "false":
                sta_opts["count"] = False
            else:
                raise SyntaxError("True or False exepcted, got \"{params['$count']}\"")

        elif key == "$select":
            sta_opts["select"] = params["$select"].split(",")

        elif key == "$filter":
            sta_opts["filter"] = sta_option_to_posgresql(params["$filter"])

        elif key == "$orderBy":
            sta_opts["orderBy"] = "order by " + sta_option_to_posgresql(params["$orderBy"])


    return sta_opts


@app.route('/Sensors(<int:sensor_id>)/Datastreams(<int:datastream_id>)/Observations', methods=['GET'])
def sensors_datastreams_observations(sensor_id, datastream_id):
    return datastreams_observations(datastream_id)


@app.route('/Datastreams(<int:datastream_id>)/Observations', methods=['GET'])
def datastreams_observations(datastream_id):
    try:
        opts = process_sensorthings_options(request)
        log.debug(f"Received Observations GET: {opts['full_path']}")

        if db.is_raw_datastream(datastream_id):
            init = time.time()
            foi_id = db.datastream_fois[datastream_id]
            pinit = time.time()
            list_data = db.get_raw_data(datastream_id, top=opts["top"], skip=opts["skip"], debug=True, format="list",
                                        filters=opts["filter"], orderby=opts["orderBy"])
            log.debug(f"{CYN}Get data from database took {time.time() - pinit:.03} seconds{RST}")
            text = data_list_to_sensorthings(list_data, foi_id, datastream_id, opts)
            log.debug(f"{CYN}Raw data query total time (with {len(list_data)} points) took {time.time() - init:.03} seconds{RST}")
            return Response(json.dumps(text), status=200, mimetype='application/json')
        else:
            init = time.time()
            text, code = get_sta_request(request)
            d = json.loads(text)
            log.debug(f"{CYN}Regular SensorThings query with {len(d['value'])} points took {time.time() - init:.03} seconds{RST}")
            return Response(text, code, mimetype='application/json')
    except SyntaxError as e:
        error_message = {
            "code": 400,
            "type": "error",
            "message": str(e)
        }
        return Response(json.dumps(error_message), 400, mimetype='application/json')


if __name__ == "__main__":
    argparser = ArgumentParser()
    argparser.add_argument("-v", "--verbose", action="store_true", help="Shows verbose output", default=False)
    argparser.add_argument("-s", "--secrets", help="File with sensible conf parameters", type=str,
                           default="secrets.json")
    argparser.add_argument("-u", "--url", help="url that will be used", type=str, default="http://localhost:5000/api")
    args = argparser.parse_args()

    with open(args.secrets) as f:
        secrets = json.load(f)
    dbconf = secrets["sensorThings"]["databaseConnectors"]["readOnly"]
    sta_base_url = secrets["sensorThings"]["url"]
    service_url = args.url
    log = setup_log("API", logger_name="API")
    if args.verbose:
        log.setLevel(logging.DEBUG)
    log.info("Setting up db connector")
    db = SensorthingsDbConnector(dbconf["host"], dbconf["port"], dbconf["name"], dbconf["user"], dbconf["password"],
                                 log)
    log.info("Getting sensor list...")

    app.run(debug=True)

