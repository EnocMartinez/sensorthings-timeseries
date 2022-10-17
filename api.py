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
from flask import Flask, request, jsonify, Response
from common import setup_log, SensorthingsDbConnector, GRN, RST, RED, YEL, CYN
import time
import psycopg2
import rich
from flask_cors import CORS

app = Flask(__name__)
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
        rich.print("[yellow]nested $expand detected")
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
            rich.print(f"Nested expand string='{nested_expand}'")

    if nested:
        rich.print(f"[green]Extracting options from {expand_string}")
        expand_string = expand_string.replace("$expand=" + nested_expand, "").replace(";;", ";")  # delete nested expand
        opts["$expand"] = nested_expand

    rich.print(f"[yellow]Expanding string '{expand_string}'")
    if "$" in expand_string:
        for pairs in expand_string.split(";"):
            rich.print(f"[cyan]Processing pair {pairs}")
            if len(pairs) > 0:
                rich.print(f"my pair is \"{pairs}\"")
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

    if parent_element == "Datastreams":
        datastream = resp
        datastream_id = get_datastream_id(datastream)
        foi_id = db.datastream_fois[datastream_id]
        rich.print(f"[blue]Datastream_id={datastream_id}")

    elif parent_element == "FeatureOfInterest":
        foi_id = get_foi_id(resp)
        datastream_id = db.datastream_fois(foi_id)

    if db.is_raw_datastream(datastream_id):
        rich.print(f"[magenta]    Expanding Datastream {db.datastream_names[datastream_id]}")
        opts = process_sensorthings_options(opts)  # from "raw" options to processed ones

        list_data = db.get_raw_data(datastream_id, top=opts["top"], skip=opts["skip"], debug=True, format="list",
                                    filters=opts["filter"], orderby=opts["orderBy"])
        observation_list = format_observation_list(list_data, foi_id, datastream_id, opts)
        datastream["Observations@iot.nextLink"] = generate_next_link(len(list_data), opts, datastream_id)
        datastream["Observations@iot.navigatioinLink"] = args.url + f"/Datastreams({datastream_id})/Observations"
        datastream["Observations"] = observation_list

    else:
        rich.print("Regular datastream, no need to expand anything...")

    return resp


def expand_query(resp, parent_element, expanding_key, opts):
    rich.print(f"[green]Expanding {expanding_key}")

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
        rich.print(f"[cyan]Expanding nested expand! {opts['$expand']}")
        nested_options = parse_options_within_expand(opts["$expand"])
        nested_expanding_key = get_expand_value(opts["$expand"])

        if type(resp[expanding_key]) == list:
            # Expand every element within the list
            for i in range(len(resp[expanding_key])):
                element_to_expand = resp[expanding_key][i]
                rich.print("before:", element_to_expand)
                nested_response = expand_query(element_to_expand, expanding_key, nested_expanding_key, nested_options)
                resp[expanding_key][i] = nested_response
                rich.print("after:", nested_response)

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
    rich.print("[cyan]Checking if further actions are needed...")

    if "expand" in opts.keys():
        rich.print("[green]Expand detected!")
        rich.print(f"URL '{request.full_path}'")
        rich.print(f"Expand options {opts['expand']}")
        parent, key, expand_opts = process_url_with_expand(request.full_path, opts)
        rich.print(f"===> Expanding {parent}->{key} with options {json.dumps(expand_opts)}")
        resp = expand_query(resp, parent, key, expand_opts)

    return Response(json.dumps(resp), status=200, mimetype="application/json")


def decode_expand_options(expand_string: str):
    """
    Converts from expand semicolon separated-tring options to a dict
    """
    if "(" not in expand_string:
        return {}
    rich.print("Remove sorrounding paranthesis")
    expand_string = expand_string.split("(")[1]
    if expand_string[-1] == ")":
        expand_string = expand_string[:-1]
    if "$expand" in expand_string:
        raise ValueError("UNimplemented!")
    options = {}
    for option in expand_string.split(";"):
        key, value = option.split("=")
        option[key] = value
    return options


@app.route('/<path:path>', methods=['GET'])
def generic_query(path):
    text, code = get_sta_request(request)
    resp = json.loads(text)
    return process_sensorthings_response(request, resp)


@app.route('/Sensors(<int:sensor_id>)/Datastreams(<int:datastream_id>)/Observations', methods=['GET'])
def sensors_datastreams_observations(sensor_id, datastream_id):
    return datastreams_observations(datastream_id)


@app.route('/Datastreams(<int:datastream_id>)/Observations', methods=['GET'])
def datastreams_observations(datastream_id):
    try:
        opts = process_sensorthings_options(request.args.to_dict())
        if db.is_raw_datastream(datastream_id):
            init = time.time()
            foi_id = db.datastream_fois[datastream_id]
            pinit = time.time()
            list_data = db.get_raw_data(datastream_id, top=opts["top"], skip=opts["skip"], debug=True, format="list",
                                        filters=opts["filter"], orderby=opts["orderBy"])
            log.debug(f"{CYN}Get data from database took {time.time() - pinit:.03} seconds{RST}")
            text = data_list_to_sensorthings(list_data, foi_id, datastream_id, opts)
            log.debug(
                f"{CYN}Raw data query total time (with {len(list_data)} points) took {time.time() - init:.03} seconds{RST}")
            return Response(json.dumps(text), status=200, mimetype='application/json')
        else:
            init = time.time()
            text, code = get_sta_request(request)
            d = json.loads(text)
            log.debug(
                f"{CYN}Regular SensorThings query (with {len(d['value'])} points) took {time.time() - init:.03} seconds{RST}")
            return process_sensorthings_response(request, json.loads(text))
    except SyntaxError as e:
        error_message = {
            "code": 400,
            "type": "error",
            "message": str(e)
        }
        return Response(json.dumps(error_message), 400, mimetype='application/json')

    except psycopg2.errors.InFailedSqlTransaction as db_error:
        error_message = {
            "code": 500,
            "type": "error",
            "message": f"Internal database connector error: {db_error}"
        }
        db.connection.rollback()
        return Response(json.dumps(error_message), 500, mimetype='application/json')


def get_sta_request(request):
    sta_url = f"{sta_base_url}{request.full_path}"
    log.debug(f"Generic query, fetching {sta_url}")
    resp = requests.get(sta_url)
    code = resp.status_code
    text = resp.text.replace(sta_base_url, service_url)  # hide original URL
    return text, code


@app.route('/', methods=['GET'])
def generic():
    text, code = get_sta_request(request)
    opts = process_sensorthings_options(request.args.to_dict())
    return process_sensorthings_response(request, json.loads(text))


@app.route('/Observations(<int:observation_id>)', methods=['GET'])
def observations(observation_id):
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
            return Response(json.dumps(observation), 200, mimetype='application/json')
    except SyntaxError as e:
        error_message = {
            "code": 400,
            "type": "error",
            "message": str(e)
        }
        return Response(json.dumps(error_message), 400, mimetype='application/json')


@app.after_request
def add_cors_headers(response):
    response.headers.add('access-control-allow-origin', '*')
    return response


def observations_from_raw_dataframe(df, datastream: dict):
    df["resultTime"] = df.index.strftime("%Y-%m-%dT%H:%M:%Sz")
    df = df[["resultTime", "value", "qc_flag"]]

    df.values.tolist()


def data_point_to_sensorthings(data_point: list, datastream_id: int, opts):
    base_url = args.url
    foi_id = db.datastream_fois[datastream_id]
    timestamp, value, qc_flag = data_point
    t = timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
    observation_id = int(1e10 * datastream_id + int(timestamp.strftime("%s")))
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
    """
    Generates a list Observations structure, such as
        {
            @iot.nextLink: ...
            value: [
                {observation 1...},
                {observation 2 ...}
                ...
            }
    """
    n = len(data_list)
    data = {}
    next_link = generate_next_link(len(data_list), opts, datastream_id, url=request.url)
    if next_link:
        data["@iot.nextLink"] = next_link

    data["value"] = format_observation_list(data_list, foi_id, datastream_id, opts)
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
        rich.print(f"[green]Using exsiting url {url}")
        next_link = url
    else:
        next_link = args.url + f"/Datastreams({datastream_id})/Observations"

    if "?" not in next_link:
        next_link += "?"  # add a ending ?

    if "$skip" in next_link:
        rich.print(f"[green]Skip present")
        next_link = next_link.replace(f"$skip={opts['skip']}", f"$skip={opts['skip'] + opts['top']}")
    else:
        rich.print(f"[magenta]Adding skip to '{next_link}'")
        next_link = next_link.replace(f"?", f"?$skip={opts['top']}&")
    if n < opts["top"]:
        return ""

    return next_link


def format_observation_list(data_list, foi_id: int, datastream_id: int, opts: dict):
    """
    Formats a list of observations into a list
    """
    p = time.time()
    observations_list = []
    for data_point in data_list:
        observations_list.append(data_point_to_sensorthings(data_point, datastream_id, opts))
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
        "orderBy": "order by",
        # manually change resultQuality/qc_flag to qc_flag
        "resultQuality/qc_flag": "qc_flag"
    }

    for i in range(len(elements)):
        if is_date(elements[i]):  # check if it is a date
            elements[i] = f"'{elements[i]}'"  # add ' around date
        for old, new in sta_to_pg_conversions.items():
            if old == elements[i]:
                elements[i] = new

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
            sta_opts["filter"] = sta_option_to_posgresql(params["$filter"])

        elif key == "$orderBy":
            sta_opts["orderBy"] = "order by " + sta_option_to_posgresql(params["$orderBy"])

        elif key == "$expand":
            sta_opts["expand"] = params["$expand"]  # just propagate the expand value

    return sta_opts


if __name__ == "__main__":
    argparser = ArgumentParser()
    argparser.add_argument("-v", "--verbose", action="store_true", help="Shows verbose output", default=False)
    argparser.add_argument("-s", "--secrets", help="File with sensible conf parameters", type=str,
                           default="secrets.json")
    argparser.add_argument("-u", "--url", help="url that will be used", type=str, default="http://localhost:5000")
    args = argparser.parse_args()

    with open(args.secrets) as f:
        secrets = json.load(f)
    dbconf = secrets["sensorThings"]["databaseConnectors"]["readOnly"]
    sta_base_url = secrets["sensorThings"]["url"]
    service_url = args.url
    log = setup_log("API", logger_name="API")

    log.info(f"Service URL: {service_url}")
    log.info(f"SensorThings URL: {sta_base_url}")

    if args.verbose:
        log.setLevel(logging.DEBUG)
        log.debug("Setting log to DEBUG level")
    log.info("Setting up db connector")
    db = SensorthingsDbConnector(dbconf["host"], dbconf["port"], dbconf["name"], dbconf["user"], dbconf["password"],
                                 log)
    log.info("Getting sensor list...")

    app.run(host="0.0.0.0", debug=True)
