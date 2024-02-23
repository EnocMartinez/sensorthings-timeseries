#!/usr/bin/env python3
"""
API that returns raw data from a SensorThings Instance with a separate raw_data table. If a query is accessing regular
sensorthings data (all data that is not in raw data table), this API simply acts as a proxy. Otherwise, if a query is
trying to access data in raw_data table, the API accesses this data throuh a PostgresQL connector, formats in in JSON
and sends it back following the SensorThings API standard.

Some of the things to be taken into account:
        -> there are no observation ID in raw data, but a primary key composed by timestamp and datastream_id
    -> To generate a unique identifier for each data point, the following formula is used:
            observation_id = datastream_id * 1e10 + epoch_time (timeseries)
            observation_id = datastream_id * 2e10 + epoch_time (profiles)
            observation_id = datastream_id * 3e10 + epoch_time (detections)


author: Enoc Martínez
institufrom argparse import ArgumentParser
tion: Universitat Politècnica de Catalunya (UPC)
email: enoc.martinez@upc.edu
license: MIT
created: 6/9/22
"""
from argparse import ArgumentParser
import logging
import json
import requests
from flask import Flask, request, Response
from common import setup_log, SensorthingsDbConnector, assert_dict
import time
import psycopg2
from flask_cors import CORS
import os
from common import GRN, BLU, MAG, CYN, WHT, YEL, RED, NRM, RST
import rich
import dotenv
import datetime

app = Flask("SensorThings TimeSeries")
CORS(app)


def get_datastream_id(datastream: dict):
    """
    Tries to get the Datastrea_ID of a data structure blindly, trying to find a @iot.id or its name
    """
    if "@iot.id" in datastream.keys():
        return datastream["@iot.id"]
    elif "name" in datastream.keys():
        return db.datastreams_ids[datastream["name"]]
    else:
        raise ValueError("Can't get DatastreamID for this query, no iot.id nor name!")


def get_foi_id(foi: dict):
    if "@iot.id" in foi.keys():
        return foi["@iot.id"]
    else:
        raise ValueError("Can't get DatastreamID for this query, no iot.id nor name!")


def parse_options_within_expand(expand_string):
    """
    Parses options within a expand -> Datastreams($top=1;$select=id) ->  {"$top": "1", "$select": "id"}
    """
    # Selects only what's within (...)
    expand_string = expand_string[expand_string.find("(") + 1:]
    if expand_string[-1] == ")":
        expand_string = expand_string[:-1]

    opts = {}
    nested = False
    # check if we have a nested expand
    if "$expand" in expand_string:
        nested = True
        start = expand_string.find("$expand=") + len("$expand=")
        rest = expand_string[start:]
        if "(" not in rest:
            nested_expand = rest
        else:  # process options within nested expand
            level = 0
            end = -1
            for i in range(len(rest)):
                if rest[i] == "(":
                    level += 1
                elif rest[i] == ")":
                    if level > 1:
                        level -= 1
                    else:
                        end = i
                        break
            if end < 0:
                raise ValueError("Could not parse options!")
            nested_expand = rest[:i + 1]

    if nested:
        expand_string = expand_string.replace("$expand=" + nested_expand, "").replace(";;", ";")  # delete nested expand
        opts["$expand"] = nested_expand

    if "$" in expand_string:
        for pairs in expand_string.split(";"):
            if len(pairs) > 0:
                key, value = pairs.split("=")
                opts[key] = value

    return opts


def get_expand_value(expand_string):
    next_parenthesis = expand_string.find("(")
    if next_parenthesis < 0:
        next_parenthesis = 99999  # huge number to avoid being picked
    end = min(next_parenthesis, len(expand_string))
    return expand_string[:end]


def process_url_with_expand(url, opts):
    parent = url.split("?")[0].split("/")[-1]

    # delimit where the expanded key ends
    expand_value = get_expand_value(opts["expand"])
    expand_options = parse_options_within_expand(opts["expand"])
    return parent, expand_value, expand_options


def expand_element(resp, parent_element, expanding_key, opts):
    if expanding_key != "Observations":  # we should only worry about expanding observations
        return resp

    if parent_element.startswith("Datastreams"):
        datastream = resp
        datastream_id = get_datastream_id(datastream)
        foi_id = db.datastream_fois[datastream_id]

    elif parent_element.startswith("FeaturesOfInterest"):
        foi_id = get_foi_id(resp)
        datastream_id = db.datastream_fois(foi_id)
    else:
        raise ValueError(f"Unexpected parent element '{parent_element}'")

    data_type, average = db.get_data_type(datastream_id)
    if not average:  # If not average, data may need to be expanded
        opts = process_sensorthings_options(opts)  # from "raw" options to processed ones
        if data_type == "timeseries":
            list_data = db.get_timeseries_data(datastream_id, top=opts["top"], skip=opts["skip"], debug=False, format="list",
                                        filters=opts["filter"], orderby=opts["orderBy"])
        elif data_type == "profiles":
            list_data = db.get_profiles_data(datastream_id, top=opts["top"], skip=opts["skip"], debug=False, format="list",
                                        filters=opts["filter"], orderby=opts["orderBy"])

        observation_list = format_observation_list(list_data, foi_id, datastream_id, opts)
        datastream["Observations@iot.nextLink"] = generate_next_link(len(list_data), opts, datastream_id)
        datastream["Observations@iot.navigatioinLink"] = sta_base_url + f"/Datastreams({datastream_id})/Observations"
        datastream["Observations"] = observation_list

    return resp


def expand_query(resp, parent_element, expanding_key, opts):
    # list response, expand one by one
    if "value" in resp.keys() and type(resp["value"]) == list:
        for i in range(len(resp["value"])):
            resp["value"][i] = expand_element(resp["value"][i], parent_element, expanding_key, opts)
    elif type(resp) == list:
        for i in len(resp):
            resp[i] = expand_element(resp[i], parent_element, expanding_key, opts)

    else:  # just regular response, expand it all at once
        resp = expand_element(resp, parent_element, expanding_key, opts)

    if "$expand" in opts.keys():
        nested_options = parse_options_within_expand(opts["$expand"])
        nested_expanding_key = get_expand_value(opts["$expand"])

        if type(resp[expanding_key]) == list:
            # Expand every element within the list
            for i in range(len(resp[expanding_key])):
                element_to_expand = resp[expanding_key][i]
                nested_response = expand_query(element_to_expand, expanding_key, nested_expanding_key, nested_options)
                resp[expanding_key][i] = nested_response

        elif type(resp[expanding_key]) == dict:
            element_to_expand = resp[expanding_key]
            nested_response = expand_query(element_to_expand, expanding_key, nested_expanding_key, nested_options)
            resp[expanding_key] = nested_response

    return resp


def process_sensorthings_response(request, resp: json, mimetype="aplication/json", code=200):
    """
    process the resopnse and checks if further actions are needed (e.g. expand raw data).
    """
    opts = process_sensorthings_options(request.args.to_dict())
    if "expand" in opts.keys():
        parent, key, expand_opts = process_url_with_expand(request.full_path, opts)
        resp = expand_query(resp, parent, key, expand_opts)

    return generate_response(json.dumps(resp), status=200, mimetype="application/json")


def decode_expand_options(expand_string: str):
    """
    Converts from expand semicolon separated-tring options to a dict
    """
    if "(" not in expand_string:
        return {}
    expand_string = expand_string.split("(")[1]
    if expand_string[-1] == ")":
        expand_string = expand_string[:-1]
    if "$expand" in expand_string:
        raise ValueError("Unimplemented!")
    options = {}
    for option in expand_string.split(";"):
        key, value = option.split("=")
        option[key] = value
    return options


def get_sta_request(request):
    sta_url = f"{sta_base_url}{request.full_path}"
    log.debug(f"[blue]Generic query, fetching {sta_url}")
    resp = requests.get(sta_url)
    rich.print(f"[yellow]Getting STA: {sta_url}")
    code = resp.status_code
    text = resp.text.replace(sta_base_url, service_url)  # hide original URL
    return text, code


def observations_from_raw_dataframe(df, datastream: dict):
    df["resultTime"] = df.index.strftime("%Y-%m-%dT%H:%M:%Sz")
    df = df[["resultTime", "value", "qc_flag"]]
    df.values.tolist()

def data_point_to_sensorthings(data_point: list, datastream_id: int, opts, data_type: str):
    base_url = sta_base_url
    foi_id = db.datastream_fois[datastream_id]

    if data_type == "timeseries":
        timestamp, value, qc_flag = data_point
    elif data_type == "profiles":
        timestamp, depth, value, qc_flag = data_point
    else:
        raise ValueError("unimplemented")
    t = timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
    observation_id = int(1e10 * datastream_id + int(timestamp.strftime("%s")))
    observation = {
        "@iot.id": observation_id,
        "phenomenonTime": t,
        "result": value,
        "resultTime": t,
        "@iot.selfLink": f"{base_url}/Observations({observation_id})",
        "Datastream@iot.navigationLink": f"{base_url}/Datastreams({int(datastream_id)})",
        "FeatureOfInterest@iot.navigationLink": f"{base_url}/FeaturesOfInterest({int(foi_id)})"
    }
    if data_type == "profiles" or data_type == "timeseries":
        observation["resultQuality"] = {
            "qc_flag": qc_flag
        }
    if data_type == "profiles":   # add depth in profiles
        observation["parameters"] = {"depth": depth}

    if "select" in opts.keys():
        for key in observation.copy().keys():
            if key not in opts["select"]:
                del observation[key]
    return observation


def data_list_to_sensorthings(data_list, foi_id: int, datastream_id: int, opts: dict, data_type: str):
    """
    Generates a list Observations structure, such as
        {
            @iot.nextLink: ...
            value: [
                {observation 1...},
                {observation 2 ...}
                ...
            ]
         }
    """
    data = {}
    next_link = generate_next_link(len(data_list), opts, datastream_id, url=request.url)
    if next_link:
        data["@iot.nextLink"] = next_link

    data["value"] = format_observation_list(data_list, foi_id, datastream_id, opts, data_type)
    return data


def generate_next_link(n: int, opts: dict, datastream_id: int, url: str = ""):
    """
    Generate next link
    :param n: number of observations
    :param opts: options
    :param datastream_id: ID of the datasetream
    :param url: base url to modify, if not set a datastream(xx)/Observations url will be generated
    """
    if url:
        next_link = url
    else:
        next_link = sta_base_url + f"/Datastreams({datastream_id})/Observations"

    if "?" not in next_link:
        next_link += "?"  # add a ending ?

    if "$skip" in next_link:
        next_link = next_link.replace(f"$skip={opts['skip']}", f"$skip={opts['skip'] + opts['top']}")
    else:
        next_link = next_link.replace(f"?", f"?$skip={opts['top']}&")
    if n < opts["top"]:
        return ""

    return next_link


def format_observation_list(data_list, foi_id: int, datastream_id: int, opts: dict, data_type: str):
    """
    Formats a list of observations into a list
    """
    p = time.time()
    observations_list = []
    for data_point in data_list:
        o = data_point_to_sensorthings(data_point, datastream_id, opts, data_type)
        observations_list.append(o)
    log.debug(f"{CYN}format db data took {time.time() - p:.03} seconds{RST}")
    return observations_list


def is_date(s):
    """
    checks if string is a date in the format YYYY-mm-ddTHH:MM:SS
    :param s: input string
    :return: True/False
    """
    if len(s) > 19 and s[4] == "-" and s[7] == "-" and s[10] == "T" and s[13] == ":" and s[16] == ":":
        return True

    elif len(s) == 10 and s[4] == "-" and s[7] == "-": # check pure date (YYYY-mm-dd)
        return True
    return False


def sta_option_to_postgresql(filter_string):
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
        "orderBy": "order by",
        # manually change resultQuality/qc_flag to qc_flag
        "resultQuality/qc_flag": "qc_flag",
        # manually change parameters/depth to depth
        "parameters/depth": "depth"
    }

    for i in range(len(elements)):
        if ")" and "(" in elements[i]:  # trapped within a function, like 'date(phenomenonTime)'
            string = elements[i].split("(")[1].split(")")[0]  # process only contents of function
            trapped = True
        else:
            trapped = False
            string = elements[i]  # process whole element

        if is_date(string):  # check if it is a date
            string = f"'{string}'"  # add ' around date)

        for old, new in sta_to_pg_conversions.items():  # total match
            if old == string:
                string = new

        if trapped:  # trapped within a function
            elements[i] = elements[i].split("(")[0] + f"({string})"  # recreate the function with new string
        else:
            elements[i] = string

    return " ".join(elements)


def process_sensorthings_options(params: dict):
    """
    Process the options of a request and store them into a dictionary.
    :param full_path: URL requested
    :param params: JSON dictionary with the query options
    :return: dict with all the options processed
    """
    __valid_options = ["$top", "$skip", "$count", "$select", "$filter", "$orderBy", "$expand"]
    sta_opts = {
        "count": True,
        "top": 100,
        "skip": 0,
        "filter": "",
        "orderBy": "order by timestamp asc"
    }

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
            sta_opts["select"] = params["$select"].replace("resultQuality/qc_flag", "resultQuality").split(",")

        elif key == "$filter":
            sta_opts["filter"] = sta_option_to_postgresql(params["$filter"])

        elif key == "$orderBy":
            sta_opts["orderBy"] = "order by " + sta_option_to_postgresql(params["$orderBy"])

        elif key == "$expand":
            sta_opts["expand"] = params["$expand"]  # just propagate the expand value

    return sta_opts

def generate_response(text, status=200, mimetype="application/json", headers={}):
    """
    Finals touch before sending the result, mainly replacing FROST url with our url
    """
    text = text.replace(sta_base_url, service_url)
    response = Response(text, status, mimetype=mimetype)
    for key, value in headers.items():
        response.headers[key] = value
    return response

@app.route('/<path:path>', methods=['GET'])
def generic_query(path):
    text, code = get_sta_request(request)
    resp = json.loads(text)
    return process_sensorthings_response(request, resp)

@app.route('/', methods=['GET'])
def generic():
    rich.print("[purple]Regular query, forward to SensorThings API")
    text, code = get_sta_request(request)
    opts = process_sensorthings_options(request.args.to_dict())
    return process_sensorthings_response(request, json.loads(text))

@app.route('/Observations(<int:observation_id>)', methods=['GET'])
def observations(observation_id):
    """
    Observations
    """
    try:
        full_path = request.full_path
        opts = process_sensorthings_options(request.args.to_dict())

        log.info(f"Received Observations GET: {full_path}")
        if observation_id < 1e10:
            text, code = get_sta_request(request)
            return process_sensorthings_response(request, json.loads(text), sta_opts=opts)
        else:
            datastream_id = int(observation_id / 1e10)
            struct_time = time.localtime(observation_id - 1e10 * datastream_id)
            timestamp = time.strftime('%Y-%m-%dT%H:%M:%Sz', struct_time)
            filters = f"timestamp = '{timestamp}'"
            data = db.get_raw_data(datastream_id, filters=filters, debug=True, format="list")[0]
            observation = data_point_to_sensorthings(data, datastream_id, opts)
            return generate_response(json.dumps(observation), 200, mimetype='application/json')
    except SyntaxError as e:
        error_message = {
            "code": 400,
            "type": "error",
            "message": str(e)
        }
        return generate_response(json.dumps(error_message), 400, mimetype='application/json')


@app.route('/Sensors(<int:sensor_id>)/Datastreams(<int:datastream_id>)/Observations', methods=['GET'])
def sensors_datastreams_observations(sensor_id, datastream_id):
    return datastreams_observations_get(datastream_id)

@app.route('/Datastreams(<int:datastream_id>)', methods=['GET'])
def just_datastreams(datastream_id):
    rich.print(f"[green]Got a datastream request: {request.path}")
    text, code = get_sta_request(request)
    resp = json.loads(text)
    return process_sensorthings_response(request, resp)

@app.route('/Datastreams(<int:datastream_id>)/Observations', methods=['GET'])
def datastreams_observations_get(datastream_id):
    try:
        opts = process_sensorthings_options(request.args.to_dict())
        data_type, average = db.get_data_type(datastream_id)

        # Averaged data is stored in regular "OBSERVATIONS" table, so it can be managed directly by SensorThings API
        if average:
            init = time.time()
            text, code = get_sta_request(request)
            d = json.loads(text)
            if code < 300:
                log.debug(f"{CYN} SensorThings query (with {len(d['value'])} points) took {time.time()-init:.03} sec{RST}")
            return process_sensorthings_response(request, json.loads(text))

        elif data_type == "timeseries":
            init = time.time()
            foi_id = db.datastream_fois[datastream_id]
            pinit = time.time()
            list_data = db.get_timeseries_data(datastream_id, top=opts["top"], skip=opts["skip"], debug=False, format="list",
                                        filters=opts["filter"], orderby=opts["orderBy"])
            log.debug(f"{CYN}Get data from database took {time.time() - pinit:.03} seconds{RST}")
            text = data_list_to_sensorthings(list_data, foi_id, datastream_id, opts, data_type)
            log.debug(
                f"{CYN}Raw data query total time (with {len(list_data)} points) took {time.time() - init:.03} seconds{RST}")
            return generate_response(json.dumps(text), status=200, mimetype='application/json')

        elif data_type == "profiles":
            init = time.time()
            foi_id = db.datastream_fois[datastream_id]
            pinit = time.time()
            list_data = db.get_profiles_data(datastream_id, top=opts["top"], skip=opts["skip"], debug=False, format="list",
                                        filters=opts["filter"], orderby=opts["orderBy"])
            log.debug(f"{CYN}Get data from database took {time.time() - pinit:.03} seconds{RST}")
            text = data_list_to_sensorthings(list_data, foi_id, datastream_id, opts, data_type)
            log.debug(
                f"{CYN}Raw data query total time (with {len(list_data)} points) took {time.time() - init:.03} seconds{RST}")
            return generate_response(json.dumps(text), status=200, mimetype='application/json')
        else:
            rich.print(f"[red]unimplemented")
            raise ValueError("unimplemented")

    except SyntaxError as e:
        error_message = {
            "code": 400,
            "type": "error",
            "message": str(e)
        }
        return generate_response(json.dumps(error_message), 400, mimetype='application/json')

    except psycopg2.errors.InFailedSqlTransaction as db_error:
        log.error("psycopg2.errors.InFailedSqlTransaction" )
        error_message = {
            "code": 500,
            "type": "error",
            "message": f"Internal database connector error: {db_error}"
        }
        db.connection.rollback()
        return generate_response(json.dumps(error_message), 500, mimetype='application/json')


@app.route('/Datastreams(<int:datastream_id>)/Observations', methods=['POST'])
def datastreams_observations_post(datastream_id):
    try:
        opts = process_sensorthings_options(request.args.to_dict())
        data_type, average = db.get_data_type(datastream_id)
        # Averaged data is stored in regular "OBSERVATIONS" table, so it can be managed directly by SensorThings API
        if average:
            #
            init = time.time()
            text, code = get_sta_request(request)
            d = json.loads(text)
            if code < 300:
                log.debug(f"{CYN} SensorThings query (with {len(d['value'])} points) took {time.time()-init:.03} sec{RST}")
            return process_sensorthings_response(request, json.loads(text))

        # Process "special" data types (timeseries, profiles or detections)
        data = json.loads(request.data.decode())

        if data_type == "timeseries":
            errmsg = inject_timeseries(data, datastream_id)
        elif data_type == "profiles":
            errmsg = inject_profiles(data, datastream_id)
        elif data_type == "detections":
            errmsg = inject_detections(data, datastream_id)
        else:
            raise ValueError("Unimplemented")

        if errmsg:  # manage errors
            r = {"status": "error", "message": errmsg }
            return generate_response(json.dumps(r), status=400, mimetype='application/json')

        observation_url = generate_observation_url(data_type, datastream_id, data["resultTime"])
        return generate_response("", status=200, mimetype='application/json', headers={"Location": observation_url})

    except psycopg2.errors.InFailedSqlTransaction as db_error:
        rich.print(f"[red]")
        log.error("psycopg2.errors.InFailedSqlTransaction" )
        error_message = {
            "code": 500,
            "type": "error",
            "message": f"Internal database connector error: {db_error}"
        }
        db.connection.rollback()
        return generate_response(json.dumps(error_message), 500, mimetype='application/json')


def inject_timeseries(data: dict, datastream_id: int) -> str:
    keys = {
        "resultTime": str,
        "result": float,
        "resultQuality/qc_flag": int,
    }
    try:
        assert_dict(data, keys, verbose=True)
    except AssertionError as e:
        rich.print(f"[red]Wrong data format! {e}")
        err_msg = {
            "code": 400,
            "type": "error",
            "message": "Profiles data not properly formatted! expected fields 'resultTime' (str), 'value' (float)," \
                       " and 'resultQuality/qc_flag' (int)"
        }
        return err_msg

    db.timescale.insert_to_timeseries(data["resultTime"], data["result"], data["resultQuality"]["qc_flag"],
                                      datastream_id)
    return ""  # success


def inject_profiles(data, datastream_id):
    rich.print("")
    keys = {
        "resultTime": str,
        "result": float,
        "parameters/depth": float,
        "resultQuality/qc_flag": int,
    }
    try:
        assert_dict(data, keys, verbose=True)
    except AssertionError as e:
        rich.print(f"[red]Wrong data format! {e}")
        err_msg = {
            "code": 400,
            "type": "error",
            "message": "Profiles data not properly formatted! expected fields 'resultTime' (str), 'value' (float)," \
                       " 'parameters/depth' (float) and 'resultQuality/qc_flag' (int)"
        }
        return err_msg
    db.timescale.insert_to_profiles(data["resultTime"], data["parameters"]["depth"], data["result"],
                                    data["resultQuality"]["qc_flag"], datastream_id)

    return ""  # success

def inject_detections(data, datastream_id):
    keys = {
        "resultTime": str,
        "result": int,
    }
    try:
        assert_dict(data, keys, verbose=True)
    except AssertionError as e:
        rich.print(f"[red]Wrong data format! {e}")
        err_msg = {
            "code": 400,
            "type": "error",
            "message": "Detections data not properly formatted! expected fields 'resultTime' (str), 'value' (int)"
        }
        return err_msg

    db.timescale.insert_to_detections(data["resultTime"], data["result"], datastream_id)
    rich.print("data inserted!")
    return ""  # success


def generate_observation_url(data_type: str, datastream_id, timestamp):
    data_type_digit = {
        "timeseries": "1",
        "profiles": "2",
        "detections": "3"
    }
    prefix = data_type_digit[data_type]
    t = datetime.datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")
    observation_id = int(1e10 * datastream_id + int(t.strftime("%s")))
    return service_url + f"/Observations({prefix}{observation_id})"


@app.after_request
def add_cors_headers(response):
    response.headers.add('access-control-allow-origin', '*')
    return response


if __name__ == "__main__":
    argparser = ArgumentParser()
    argparser.add_argument("-e", "--environment", help="File with environment variables", type=str, default="")
    argparser.add_argument("-p", "--port", help="HTTP port", type=int, default=8082)

    args = argparser.parse_args()

    if args.environment:
        dotenv.load_dotenv(args.environment)

    required_env_variables = ["STA_DB_HOST", "STA_DB_USER", "STA_DB_PORT", "STA_DB_PASSWORD", "STA_DB_NAME", "STA_URL",
                              "STA_TS_ROOT_URL"]

    for key in required_env_variables:
        if key not in os.environ.keys():
            raise EnvironmentError(f"Environment variable '{key}' not set!")

    log = setup_log("API", logger_name="API", log_level="info")

    db_name = os.environ["STA_DB_NAME"]
    db_port = os.environ["STA_DB_PORT"]
    db_user = os.environ["STA_DB_USER"]
    db_password = os.environ["STA_DB_PASSWORD"]
    db_host = os.environ["STA_DB_HOST"]
    service_url = os.environ["STA_TS_ROOT_URL"]
    sta_base_url = os.environ["STA_URL"]

    if "STA_TS_RAW_DATA_TABLE" not in os.environ.keys():
        raw_data_table = "raw_data"
    else:
        raw_data_table = os.environ["STA_TS_RAW_DATA_TABLE"]


    if "STA_TS_DEBUG" in os.environ.keys():
        log.setLevel(logging.DEBUG)
        log.debug("Setting log to DEBUG level")


    log.info(f"Service URL: {service_url}")
    log.info(f"SensorThings URL: {sta_base_url}")

    log.info("Setting up db connector")
    db = SensorthingsDbConnector(db_host, db_port, db_name, db_user, db_password, log)
    log.info("Getting sensor list...")
    app.run(host="0.0.0.0", debug=False, port=args.port)
