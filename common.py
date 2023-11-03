#!/usr/bin/env python3
"""

author: Enoc Martínez
institution: Universitat Politècnica de Catalunya (UPC)
email: enoc.martinez@upc.edu
license: MIT
created: 28/03/2022
"""
from threading import Thread, Event
import logging
from logging.handlers import TimedRotatingFileHandler
from queue import Queue
import os
from socket import socket, AF_INET, SOCK_DGRAM, SOL_SOCKET, SO_REUSEADDR, timeout
import rich
import psycopg2
import pandas as pd
import json
import time
import requests
from rich.progress import Progress
import multiprocessing as mp
from concurrent import futures
import numpy as np
import gc

# Color codes
GRN = "\x1B[32m"
BLU = "\x1B[34m"
MAG = "\x1B[35m"
CYN = "\x1B[36m"
WHT = "\x1B[37m"
YEL = "\x1B[33m"
RED = "\x1B[31m"
NRM = "\x1B[0m"
RST = "\033[0m"

qc_flags = {
    "good": 1,
    "not_applied": 2,
    "suspicious": 3,
    "bad": 4,
    "missing": 9
}


def setup_log(name, path="log", logger_name="QualityControl", log_level="debug"):
    """
    Setups the logging module
    :param name: log name (.log will be appended)
    :param path: where the logs will be stored
    :param log_level: log level as string, it can be "debug, "info", "warning" and "error"
    """

    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    # Check arguments
    if len(name) < 1 or len(path) < 1:
        raise ValueError("name \"%s\" not valid", name)
    elif len(path) < 1:
        raise ValueError("name \"%s\" not valid", name)

    # Convert to logging level
    if log_level == 'debug':
        level = logging.DEBUG
    elif log_level == 'info':
        level = logging.INFO
    elif log_level == 'warning':
        level = logging.WARNING
    elif log_level == 'error':
        level = logging.ERROR
    else:
        raise ValueError("log level \"%s\" not valid" % log_level)

    if not os.path.exists(path):
        os.makedirs(path)

    filename = os.path.join(path, name)
    if not filename.endswith(".log"):
        filename += ".log"
    print("Creating log", filename)
    print("name", name)

    logger = logging.getLogger()
    logger.setLevel(level)
    log_formatter = logging.Formatter('%(asctime)s.%(msecs)03d %(levelname)-7s: %(message)s',
                                      datefmt='%Y/%m/%d %H:%M:%S')
    handler = TimedRotatingFileHandler(filename, when="midnight", interval=1, backupCount=7)
    handler.setFormatter(log_formatter)
    logger.addHandler(handler)

    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(log_formatter)
    logger.addHandler(consoleHandler)

    logger.info("")
    logger.info(f"===== {logger_name} =====")

    return logger


def dataframe_to_dict(df, key, value):
    """
    Takes two columns of a dataframe and converts it to a dictionary
    :param df: input dataframe
    :param key: column name that will be the key
    :param value: column name that will be the value
    :return: dict
    """

    keys = df[key]
    values = df[value]
    d = {}
    for i in range(len(keys)):
        d[keys[i]] = values[i]
    return d


def varname_from_datastream(ds_name):
    """
    Extracts a variable name from a datastream name. The datastream name must follow the following pattern:
        <station>:<sensor>:<VARNAME>:<data_type>
    :param ds_name:
    :raises: SyntaxError if the patter doesn't match
    :return: variable name
    """
    splitted = ds_name.split(":")
    if len(splitted) != 4:
        raise SyntaxError(f"Datastream name {ds_name} doesn't have the expected format!")
    varname = splitted[2]
    return varname


def assert_dict(conf: dict, required_keys: dict):
    """
    Checks if all the expected keys in a dictionary are there. The lists __required_keys and the dict __type are
    required
    :param conf: dict with configuration to be checked
    :param required_keys: dictionary with required keys
    :raises: AssertionError if the input does not match required_keys
    """
    for key, expected_type in required_keys.items():
        if key not in conf.keys():
            raise AssertionError(f"Required key \"{key}\" not found in configuration")

        value = conf[key]
        if type(value) != expected_type:
            raise AssertionError(f"Value for key \"{key}\" wring type, expected type {expected_type}, but got "
                                 f"{type(value)}")


def reverse_dictionary(data):
    """
    Takes a dictionary and reverses key-value pairs
    :param data: any dict
    :return: reversed dictionary
    """
    return {value: key for key, value in data.items()}


def normalize_string(instring, lower_case=False):
    """
    This function takes a string and normalizes by replacing forbidden chars by underscores.The following chars
    will be replaced: : @ $ % & / + , ; and whitespace
    :param instring: input string
    :return: normalized string
    """
    forbidden_chars = [":", "@", "$", "%", "&", "/", "+", ",", ";", " ", "-"]
    outstring = instring
    for char in forbidden_chars:
        outstring = outstring.replace(char, "_")
    if lower_case:
        outstring = outstring.lower()
    return outstring


class LoggerSuperclass:
    def __init__(self, logger, name: str, colour=NRM):
        """
        SuperClass that defines logging as class methods adding a heading name
        """
        self.__logger_name = name
        self.__logger = logger
        if not logger:
            self.__logger = logging  # if not assign the generic module
        self.__log_colour = colour

    def warning(self, *args):
        mystr = YEL + "[%s] " % self.__logger_name + str(*args) + RST
        self.__logger.warning(mystr)

    def error(self, *args, exception=False):
        mystr = "[%s] " % self.__logger_name + str(*args)

        self.__logger.error(RED + mystr + RST)
        if exception:
            raise ValueError(mystr)

    def debug(self, *args):
        mystr = self.__log_colour + "[%s] " % self.__logger_name + str(*args) + RST
        self.__logger.debug(mystr)

    def info(self, *args):
        mystr = self.__log_colour + "[%s] " % self.__logger_name + str(*args) + RST
        self.__logger.info(mystr)


class GenericThread(Thread, LoggerSuperclass):
    def __init__(self, name, logger, log_colour=NRM, daemon=True):
        """
        Constructor for MyGenericThread
        :param name: Name of the thread
        :param log_colour: Colour that will be used by the thread logs
        """
        Thread.__init__(self, name=name, daemon=daemon)
        LoggerSuperclass.__init__(self, logger, name, colour=log_colour)
        self.stop_event = Event()
        self.name = name
        self.required_keys = []
        self.key_types = {}


class UdpListener(GenericThread):
    def __init__(self, port: int, queue: Queue, logger, timeout=2):
        """
        Listens to UDP data coming a specific port and puts the received data into a queue
        :param port: input port
        :param queue: Queue where input data will be put
        :param timeout: timeout in seconds
        """
        GenericThread.__init__(self, "Listener", logger, daemon=True, log_colour=BLU)
        self.info(f"Starting UdpListener, on {port}")
        self.socket = socket(AF_INET, SOCK_DGRAM)
        self.socket.bind(('', port))  # bind to any IP, let the OS choose a port

        # self.socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        self.queue = queue

    def run(self, size=4096):
        while not self.stop_event.is_set():
            bindata, address = self.socket.recvfrom(size)
            data = bindata.decode()
            if data == "hello?":
                # if we received hello? a Dispatcher is pinging this process
                self.socket.sendto("hello!".encode(), address)
                self.debug(f"Pinged by {address}")
            else:
                try:
                    data = json.loads(bindata.decode())
                except json.decoder.JSONDecodeError as e:
                    self.error(f"Could not convert data to JSON {e} ")
                    self.error(f"Received message \"{bindata.decode()}\"")
                    continue
                self.queue.put(data)
                self.info(f"received {len(bindata)} bytes, putting to queue (queue has {self.queue.qsize()} elements)")


class UdpDispatcher(GenericThread):
    def __init__(self, host: str, port: int, queue: Queue, logger, timeout=2):
        """
        Inhertis from UdpDispatcher adding threading capabilities
        :param host: output host
        :param port: output port
        :param queue: Queue where input data will be put
        """
        GenericThread.__init__(self, "UDP Dispatcher", logger, daemon=True, log_colour=MAG)

        self.out_host = host  #
        self.out_port = port
        self.socket = socket(AF_INET, SOCK_DGRAM)
        self.socket.bind(('', 0))  # bind to any IP, let the OS choose a port
        self.socket.settimeout(timeout)
        self.queue = queue

    def anyone_listening(self):
        """
        Ping a UdpListener and return True if it is responding or Flase if it is not...
        """
        self.debug(f"pinging {self.out_host}:{self.out_port}...")
        self.socket.sendto("hello?".encode(), (self.out_host, self.out_port))
        try:
            data = self.socket.recv(1024)
        except timeout:
            self.warning(f"Timeout exception, no one listening! (pinged {self.out_host}:{self.out_port})")
            return False

        if data.decode() == "hello!":
            self.debug("Listener alive!")
            return True
        self.error(f"Unexpected response from \"{data.decode()}\"")
        raise ValueError()
        return False  #

    def send(self, indata):
        """
        Sends data over UDP, input data (string or dictionary) will be formmatted as bindata
        :param indata: data in str or dict
        :return: nothing
        """
        if type(indata) == dict:
            data = json.dumps(indata, indent=2)
        elif type(indata) == str:
            data = indata
        else:
            self.error(f"Expected dict or str, got {type(indata)}")
            raise TypeError(f"Expected dict or str, got {type(indata)}")
        bindata = data.encode()
        self.socket.sendto(bindata, (self.out_host, self.out_port))

    def run(self):
        while not self.stop_event.is_set():
            if self.queue.qsize() > 0:
                if self.anyone_listening():
                    while self.queue.qsize() > 0:
                        data = self.queue.get()
                        self.send(data)
                        self.debug("Element sent")
                        self.queue.task_done()
                else:
                    self.info(f"Data not sent... {self.queue.qsize()} elements in queue")
            time.sleep(3)


class Connection:
    def __init__(self, host, port, db_name, db_user, db_password, timeout):
        self.__host = host
        self.__port = port
        self.__name = db_name
        self.__user = db_user
        self.__pwd = db_password
        self.__timeout = timeout

        self.available = True  # flag that determines if this connection is available or not
        # Create a connection
        self.connection = psycopg2.connect(host=self.__host, port=self.__port, dbname=self.__name, user=self.__user,
                                           password=self.__pwd, connect_timeout=self.__timeout)
        self.cursor = self.connection.cursor()
        self.last_used = -1

        self.index = 0
        self.__closing = False

    def run_query(self, query, description=False, debug=False):
        """
        Executes a query and returns the result. If description=True the desription will also be returned
        """
        self.available = False
        if debug:
            rich.print("[magenta]%s" % query)
        self.cursor.execute(query)
        self.connection.commit()
        resp = self.cursor.fetchall()
        self.available = True

        if description:
            return resp, self.cursor.description
        return resp

    def close(self):
        if not self.__closing:
            self.__closing = True
            rich.print(f"[cyan]Closing connection {id(self)}")
            self.connection.close()
        else:
            rich.print(f"[red]Someone else is closing {id(self)}!")




class PgDatabaseConnector(LoggerSuperclass):
    """
    Interface to access a PostgresQL database
    """

    def __init__(self, host, port, db_name, db_user, db_password, logger, timeout=5):
        LoggerSuperclass.__init__(self, logger, "DB connector")
        self.__host = host
        self.__port = port
        self.__name = db_name
        self.__user = db_user
        self.__pwd = db_password
        self.__timeout = timeout

        self.query_time = -1  # stores here the execution time of the last query
        self.db_initialized = False
        self.connections = []  # list of connections, starts with one
        self.max_connections = 50

    def new_connection(self) -> Connection:
        c = Connection(self.__host, self.__port, self.__name, self.__user, self.__pwd, self.__timeout)
        self.connections.append(c)
        return c

    def get_available_connection(self):
        """
        Loops through the connections and gets the first available. If there isn't any available create a new one (or
        wait if connections reached the limit).
        """

        for i in range(len(self.connections)):
            c = self.connections[i]
            if c.available:
                return c

        while len(self.connections) >= self.max_connections:
            time.sleep(0.5)
            self.debug("waiting for conn")

        self.info(f"Creating DB connection {len(self.connections)}..")
        return self.new_connection()


    def exec_query(self, query, description=False, debug=False):
        """
        Runs a query in a free connection
        """
        c = self.get_available_connection()
        results = None
        try:
            results = c.run_query(query, description=description, debug=debug)
        except Exception as e:
            rich.print(f"[red]{e}")
            try:
                c.close()
            except:  # ignore errors
                pass
            self.connections.remove(c)
        return results


    def list_from_query(self, query, debug=False):
        """
        Makes a query to the database using a cursor object and returns a DataFrame object
        with the reponse
        :param query: string with the query
        :param debug:
        :returns list with the query result
        """
        return self.exec_query(query, debug=debug)

    def dataframe_from_query(self, query, debug=False):
        """
        Makes a query to the database using a cursor object and returns a DataFrame object
        with the reponse
        :param cursor: database cursor
        :param query: string with the query
        :param debug:
        :returns DataFrame with the query result
        """
        response, description = self.exec_query(query, debug=debug, description=True)
        colnames = [desc[0] for desc in description]  # Get the Column names
        return pd.DataFrame(response, columns=colnames)

    def close(self):
        for c in self.connections:
            c.close()


class SensorthingsDbConnector(PgDatabaseConnector, LoggerSuperclass):
    def __init__(self, host, port, db_name, db_user, db_password, logger, keep_trying=True, raw_data_table="raw_data"):
        """
        initializes  DB connector specific for SensorThings API database (FROST implementation)
        :param host:
        :param port:
        :param db_name:
        :param db_user:
        :param db_password:
        :param logger:
        """
        PgDatabaseConnector.__init__(self, host, port, db_name, db_user, db_password, logger)
        LoggerSuperclass.__init__(self, logger, "STA DB")
        self.info("Initialize database connector...")
        self.raw_data_table = "raw_data"
        self.__sensor_properties = {}

        # dictionaries where sensors key is name and value is ID
        while keep_trying:
            try:
                self.sensor_ids = self.get_sensors()
                self.datastreams_ids = self.get_datastream_names()
                self.obs_properties_ids = self.get_obs_properties()
                self.things_ids = self.get_things()
                self.datastream_fois = self.get_datastream_fois()
                self.foi_datastream = reverse_dictionary(self.datastream_fois)

                keep_trying = False
            except ConnectionError as e:
                self.error("Can't access database, trying again in 10s")
                time.sleep(10)

        # dictionaries where key is ID and value is name
        self.sensor_names = reverse_dictionary(self.sensor_ids)
        self.datastream_names = reverse_dictionary(self.datastreams_ids)
        self.things_names = reverse_dictionary(self.things_ids)
        self.obs_properties_names = reverse_dictionary(self.obs_properties_ids)

        self.datastream_properties = self.get_datastream_properties()

    def is_raw_datastream(self, datastream_id):
        if self.datastream_properties[datastream_id]["rawSensorData"]:
            return True
        else:
            return False

    def get_sensor_datastreams(self, sensor_id):
        """
        Returns a dataframe with all datastreams belonging to a sensor
        :param sensor_id: ID of a sensor
        :return: dataframe with datastreams ID, NAME and PROPERTIES
        """
        query = 'select "ID" as id , "NAME" as name, "PROPERTIES" as properties ' \
                f'from "DATASTREAMS" where "SENSOR_ID" = {sensor_id};'
        df = self.dataframe_from_query(query)
        return df

    def get_sensor_qc_metadata(self, name: str):
        """
        Returns an object with all the necessary information to apply QC to all variables from the sensor
        :return:
        """
        sensor_id = self.sensor_ids[name]
        data = {
            "name": name,
            "id": int(sensor_id),  # from numpy.int64 to regular int
            "variables": {}
        }
        query = 'select "ID" as id , "NAME" as name, "PROPERTIES" as properties ' \
                f'from "DATASTREAMS" where "SENSOR_ID" = {sensor_id};'
        df = self.dataframe_from_query(query)
        for _, row in df.iterrows():
            ds_name = row["name"]
            properties = row["properties"]
            if "rawSensorData" not in properties.keys() or not properties["rawSensorData"]:
                # if rawSensorData = False of if there is no rawSensorData flag, just ignore this datastream
                self.debug(f"Ignoring datastream {ds_name}")
                continue
            self.info(f"Loading configuration for datastream {ds_name}")
            varname = varname_from_datastream(ds_name)

            qc_config = {}
            try:
                qc_config = properties["QualityControl"]["configuration"]
            except KeyError as e:
                self.warning(f"Quality Control for variable {varname} not found!")
                self.warning(f"KeyError: {e}")

            data["variables"][varname] = {
                "variable_name": varname,
                "datastream_name": ds_name,
                "datastream_id": int(self.datastreams_ids[ds_name]),
                "quality_control": qc_config
            }
        return data

    def get_qc_config(self, raw_sensor_data_flag="rawSensorData"):
        """
        Gets the QC config for all sensors from the database and stores it in a dictionary
        :return: dictionary with the configuration
        """
        sensors = {}

        for sensor, sensor_id in self.sensor_ids.items():
            sensors[sensor] = {"datastreams": {}, "id": sensor_id, "name": sensor}

            # Select Datastreams belonging to a sensor and expand rawData flag and quality control config
            q = f'select "ID" as id, "NAME" as name, ("PROPERTIES"->>\'QualityControl\') as qc_config, ' \
                f'("PROPERTIES"->>\'{raw_sensor_data_flag}\')::BOOLEAN as is_raw_data ' \
                f' from "DATASTREAMS" where "SENSOR_ID" = {sensor_id};'

            df = self.dataframe_from_query(q, debug=False)

            for index, row in df.iterrows():
                if not row["is_raw_data"]:
                    rich.print(f"[yellow] ignoring datastream {row['name']}")
                    continue

                sensors[sensor]["datastreams"][row["id"]] = {
                    "name": row["name"],
                    "qc_config": json.loads(row["qc_config"])
                }

    def get_sensors(self):
        """
        Returns a dictionary with sensor's names and their id, e.g. {"SBE37": 3, "AWAC": 5}
        :return: dictionary
        """
        df = self.dataframe_from_query('select "ID", "NAME" from "SENSORS";')
        return dataframe_to_dict(df, "NAME", "ID")

    def get_things(self):
        """
        Returns a dictionary with sensor's names and their id, e.g. {"SBE37": 3, "AWAC": 5}
        :return: dictionary
        """
        df = self.dataframe_from_query('select "ID", "NAME" from "THINGS";')
        return dataframe_to_dict(df, "NAME", "ID")

    def get_sensor_properties(self):
        """
        Returns a dictionary with sensor's names and their properties
        :return: dictionary
        """
        if self.__sensor_properties:
            return self.__sensor_properties
        df = self.dataframe_from_query('select "NAME", "PROPERTIES" from "SENSORS";')
        self.__sensor_properties = dataframe_to_dict(df, "NAME", "PROPERTIES")
        return self.__sensor_properties

    def get_datastream_names(self, fields=["ID", "NAME"]):
        select_fields = f'"{fields[0]}"'
        for f in fields[1:]:
            select_fields += f', "{f}"'

        df = self.dataframe_from_query(f'select {select_fields} from "DATASTREAMS";')
        return dataframe_to_dict(df, "NAME", "ID")

    def get_datastream_properties(self, fields=["ID", "PROPERTIES"]):
        select_fields = f'"{fields[0]}"'
        for f in fields[1:]:
            select_fields += f', "{f}"'

        df = self.dataframe_from_query(f'select {select_fields} from "DATASTREAMS";')
        return dataframe_to_dict(df, "ID", "PROPERTIES")

    def get_obs_properties(self):
        df = self.dataframe_from_query('select "ID", "NAME" from "OBS_PROPERTIES";')
        return dataframe_to_dict(df, "NAME", "ID")

    def get_thing_location(self, thing_id):
        query = f'select "LOCATION_ID" from "THINGS_LOCATION" where "THING_ID" = {thing_id};'
        df = self.list_from_query(query)
        rich.print(df)
        return df.values[0][0]

    def get_location_from_thing(self, thing_id):
        query = f'select "LOCATION_ID" from "THINGS_LOCATIONS" where "THING_ID" = {thing_id};'
        values = self.list_from_query(query)
        return values[0][0]

    def get_thing_from_datastream(self, datastream_id):
        query = f'select "THING_ID" from "DATASTREAMS" where "ID" = {datastream_id};'
        values = self.list_from_query(query)
        return values[0][0]

    def get_foi_from_thing(self, thing_id):
        location_id = self.get_location_from_thing(thing_id)
        query = f'select "GEN_FOI_ID" from "LOCATIONS" where "ID" = {location_id};'
        values = self.list_from_query(query)
        return values[0][0]


    def get_last_datastream_timestamp(self, datastream_id):
        """
        Returns a timestamp (pd.Timestamp) with the last data point from a certain datastream. If there's no data
        return None or pd.Timestamp
        """

        properties = self.datastream_properties[datastream_id]
        if properties["rawSensorData"]:
            query = f"select timestamp from {self.raw_data_table} where datastream_id = {datastream_id} order by" \
                    f" timestamp desc limit 1;"
            row = self.dataframe_from_query(query)

        else:
            query = f'select "PHENOMENON_TIME_START" as timestamp from "OBSERVATIONS" where "DATASTREAM_ID" = {datastream_id} ' \
                    f'order by "PHENOMENON_TIME_START" desc limit 1;'
            row = self.dataframe_from_query(query)

        if row.empty:
            return None

        return row["timestamp"][0]

    def get_datastream_fois(self):
        """
        Generates a dictionary with key datastream_id and value foi_id. Only valid for FOIs generated via location
        :return:
        """
        query = '''
        select "DATASTREAM_ID", "GEN_FOI_ID" from 
            (select "DATASTREAMS"."ID" AS "DATASTREAM_ID", "LOCATION_ID" from
            "DATASTREAMS" join "THINGS_LOCATIONS" on "DATASTREAMS"."THING_ID" = "THINGS_LOCATIONS"."THING_ID") as q1
            join
            "LOCATIONS" as q2
            on q1."LOCATION_ID" = q2."ID"'''
        df = self.dataframe_from_query(query)
        return dataframe_to_dict(df, "DATASTREAM_ID", "GEN_FOI_ID")

    def get_last_datastream_data(self, datastream_id, hours):
        """
        Gets the last N hours of data from a datastream
        :param datastream_id: ID of the datastream
        :param hours: get last X hours
        :return: dataframe with the data
        """
        query = f"select timestamp, value, qc_flag from raw_data where datastream_id = {datastream_id} " \
                f"and timestamp > now() - INTERVAL '{hours} hours' order by timestamp asc;"
        df = self.dataframe_from_query(query)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df.set_index("timestamp", inplace=True)
        df.sort_index(inplace=True)
        return df

    def get_raw_data_count(self, datastream_id, filters=""):
        query = f"select count(*) from {self.raw_data_table} where datastream_id = {datastream_id}"
        if filters:
            query += f" and {filters} "  # add custom filters
        query += ";"
        return self.list_from_query(query, debug=False)[0][0]

    def get_raw_data(self, identifier, time_start="", time_end="", top=100, skip=0, ascending=True, debug=False,
                     format="dataframe", filters="", orderby=""):
        """
        Access the raw data table and exports all data between time_start and time_end
        :param identifier: datastream name (str) or datastream id (int)
        :param time_start: start time
        :param time_end: end time (not included)
        """
        if type(identifier) == int:
            pass
        elif type(identifier) == str:  # if string, convert from name to ID
            identifier = self.datastreams_ids[identifier]
        query = f"select timestamp, value, qc_flag from {self.raw_data_table} where datastream_id = {identifier}"

        if filters:
            query += f" and {filters} "  # add custom filters

        if time_start:
            query += f" and timestamp >= '{time_start}'"
        if time_end:
            query += f" and timestamp <'{time_end}'"

        if orderby:
            query += f" {orderby}"
        else:
            if ascending:
                query += " order by timestamp asc"
            else:
                query += " order by timestamp desc"

        query = f"select * from ({query}) as e"
        if skip:
            query += f" offset {skip}"

        query += f" limit {top};"

        if format == "dataframe":
            df = self.dataframe_from_query(query, debug=debug)
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
            return df.set_index("timestamp")
        elif format == "list":
            return self.list_from_query(query, debug=debug)
        else:
            raise ValueError(f"format {format} not valid!")

    def get_avg_data(self, identifier, time_start: str, time_end: str):
        """
        Access the raw data table and exports all data between time_start and time_end
        :param identifier: datasream name (str) or datastream id (int)
        :param time_start: start time
        :param time_end: end time  (not included)
        """
        if type(identifier) == int:
            pass
        elif type(identifier) == str:  # if string, convert from name to ID
            identifier = self.datastreams_ids[identifier]

        query = f' select ' \
                f'    "PHENOMENON_TIME_START" AS timestamp, ' \
                f'    "RESULT_NUMBER" AS value,' \
                f'    ("RESULT_QUALITY" ->> \'qc_flag\'::text)::integer AS qc_flag,' \
                f'    ("RESULT_QUALITY" ->> \'stdev\'::text)::double precision AS stdev ' \
                f'from "OBSERVATIONS" ' \
                f'where "OBSERVATIONS"."DATASTREAM_ID" = {identifier} ' \
                f'and "PHENOMENON_TIME_START" >= \'{time_start}\' and  "PHENOMENON_TIME_START" < \'{time_end}\'' \
                f'order by timestamp asc;'

        df = self.dataframe_from_query(query)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        if not df.empty and np.isnan(df["stdev"].max()):
            self.debug(f"Dropping stdev for {self.datastream_names[identifier]}")
            del df["stdev"]
        return df.set_index("timestamp")

    def dataframe_from_datastream(self, datastream_id: int, time_start, time_end):
        """
        Takes the ID of a datastream and exports its data to a dataframe.
        """
        df = self.dataframe_from_query(f'select "PROPERTIES" from "DATASTREAMS" where "ID"={datastream_id};')
        if df.empty:
            raise ValueError(f"Datastream with ID={datastream_id} not found in database!")
        properties = df.values[0][0]  # only one value with the properties in JSON format
        # if the datastream has rawSensorData=True in properties, export from raw_data
        if properties["rawSensorData"]:
            df = self.get_raw_data(datastream_id, time_start, time_end)
        else:  # otherwise export from observations
            df = self.get_avg_data(datastream_id, time_start, time_end)

        df, _ = raw_data_to_oceansites_naming_dataframe(df, self.datastream_names[datastream_id])
        return df


def get_dataframe_precision(df, check_values=1000, min_precision=-1):
    """
    Retunrs a dict with the precision for each column in the dataframe
    :param df: input dataframes
    :param check_values: instead of checking the precision of all values, the first N values will be checked
    :param min_precision: If precision is less than min_precision, use this value instead
    """
    column_precision = {}
    length = min(check_values, len(df.index.values))
    for c in df.columns:
        if "_qc" in c:
            continue  # avoid QC columns
        precisions = np.zeros(length, dtype=int)
        for i in range(0, length):
            value = df[c].values[i]
            try:
                _, float_precision = str(value).split(".")
            except ValueError:
                float_precision = ""  # if error precision is 0 (point not found)
            precisions[i] = len(float_precision)  # store the precision
        column_precision[c] = max(precisions.max(), min_precision)
    return column_precision


def delete_vars_from_df(df, vars: list):
    # ignore (delete) variables in list
    for var in vars:
        # use it as a column name
        found = False
        if var in df.columns:
            found = True
            rich.print(f"[yellow]Variable {var} won't be resampled")
            del df[var]

        if not found:  # maybe it's a prefix, delete everything starting like var
            for v in [col for col in df.columns if col.startswith(var)]:
                rich.print(f"[yellow]Ignoring variable {v}")
                del df[v]  # delete it
    return df


def resample_dataframe(df, average_period="30min", std_column=True, log_vars=[], ignore=[]):
    """
    Resamples incoming dataframe, performing the arithmetic mean. If QC control is applied select only values with
    "good data" flag (1). If no values with good data found in a data segment, try to average suspicious data. Data
    segments with only one value will be ignored (no std can be computed).
    :param df: input dataframe
    :param average_period: average period (e.g. 1h, 15 min, etc.)
    :param std_colum: if True creates a columns with the standard deviation for each variable
    :return: averaged dataframe
    """
    df = df.copy()
    df = delete_vars_from_df(df, ignore)

    # Get the current precision column by column
    precisions = get_dataframe_precision(df)

    if log_vars:
        print("converting the following variables from log to lin:", log_vars)
        df = log_to_lin(df, log_vars)

    resampled_dataframes = []
    # print("Splitting input dataframe to a dataframe for each variable")
    for var in df.columns:
        if var.endswith("_qc"):  # ignore qc variables
            continue

        var_qc = var + "_qc"
        df_var = df[var].to_frame()
        df_var[var_qc] = df[var_qc]

        # Check if QC has been applied to this variable
        if max(df[var_qc].values) == min(df[var_qc].values) == 2:
            rich.print(f"[yellow]QC not applied to {var}")
            rdf = df_var.resample(average_period).mean().dropna(how="any")
            rdf[var_qc] = rdf[var_qc].fillna(0).astype(np.int8)

        else:
            var_qc = var + "_qc"
            # Generate a dataframe for good, suspicious and bad data
            df_good = df_var[df_var[var_qc] == qc_flags["good"]]
            df_na = df_var[df_var[var_qc] == qc_flags["not_applied"]]
            df_suspicious = df_var[df_var[var_qc] == qc_flags["suspicious"]]

            # Resample all dataframes independently to avoid averaging good data with bad data and drop all n/a records
            rdf = df_good.resample(average_period).mean().dropna(how="any")
            rdf_na = df_na.resample(average_period).mean().dropna(how="any")
            #  rdf["good_count"] = df_good[var_qc].resample(average_period).count().dropna(how="any")
            rdf[var_qc] = rdf[var_qc].astype(np.int8)

            rdf_suspicious = df_suspicious.resample(average_period).mean().dropna(how="any")

            # rdf_suspicious["suspicious_count"] = df_suspicious[var_qc].resample(average_period).count().dropna(how="any")
            rdf_suspicious[var_qc] = rdf_suspicious[var_qc].astype(np.int8)

            if std_column:
                rdf_std = df_good.resample(average_period).std().dropna(how="any")
                rdf_suspicious_std = df_suspicious.resample(average_period).std().dropna(how="any")
                rdf_na_std = rdf_na.resample(average_period).std().dropna(how="any")

            # Join good and suspicious data
            rdf = rdf.join(rdf_na, how="outer", rsuffix="_na")
            rdf = rdf.join(rdf_suspicious, how="outer", rsuffix="_suspicious")

            # Fill n/a in QC variable with 0s and convert it to int8 (after .mean() it was float)
            rdf[var_qc] = rdf[var_qc].fillna(0).astype(np.int8)

            # Select all lines where there wasn't any good data (missing good data -> qc = 0)
            i = 0

            for index, row in rdf.loc[rdf[var_qc] == 0].iterrows():
                i += 1
                if not np.isnan(row[var + "_na"]):
                    # modify values at resampled dataframe (rdf)
                    rdf.at[index, var] = row[var + "_na"]
                    rdf.at[index, var_qc] = qc_flags["not_applied"]

                if not np.isnan(row[var + "_suspicious"]):
                    # modify values at resampled dataframe (rdf)
                    rdf.at[index, var] = row[var + "_suspicious"]
                    rdf.at[index, var_qc] = qc_flags["suspicious"]

            # Calculate standard deviations
            if std_column:
                var_std = var + "_std"
                rdf[var_std] = 0

                # assign good data stdev where qc = 1
                rdf.loc[rdf[var_qc] == qc_flags["good"], var_std] = rdf_std[var]
                # assign data with QC not applied
                if not rdf.loc[rdf[var_qc] == qc_flags["not_applied"]].empty:
                    rdf.loc[rdf[var_qc] == qc_flags["not_applied"], var_std] = rdf_na_std[var]
                # assign suspicious stdev where qc = 3
                if not rdf.loc[rdf[var_qc] == qc_flags["suspicious"]].empty:
                    rdf.loc[rdf[var_qc] == qc_flags["suspicious"], var_std] = rdf_suspicious_std[var]

                del rdf_std
                del rdf_suspicious_std
                del rdf_na_std
                gc.collect()

            # delete suspicious and bad columns
            del rdf[var + "_na"]
            del rdf[var + "_suspicious"]
            del rdf[var + "_qc_na"]
            del rdf[var + "_qc_suspicious"]

            # delete all unused dataframes
            del df_var
            del rdf_na
            del rdf_suspicious
            del df_good
            del df_suspicious
            rdf = rdf.dropna(how="all")
            gc.collect()  # try to free some memory with garbage collector

        # append dataframe
        resampled_dataframes.append(rdf)

    # Join all dataframes
    df_out = resampled_dataframes[0]
    for i in range(1, len(resampled_dataframes)):
        merge_df = resampled_dataframes[i]
        df_out = df_out.join(merge_df, how="outer")

    df_out.dropna(how="all", inplace=True)

    if log_vars:  # Convert back to linear
        print("converting back to log", log_vars)
        df_out = lin_to_log(df_out, log_vars)

    # Apply the precision
    for colname, precision in precisions.items():
        df_out[colname] = df_out[colname].round(decimals=precision)

    # Set the precisions for the standard deviations (precision + 2)
    for colname, precision in precisions.items():
        std_var = colname + "_std"
        if std_var in df_out.keys():
            df_out[std_var] = df_out[std_var].round(decimals=(precision + 2))

        if colname in log_vars:  # If the variable is logarithmic it makes no sense calculating the standard deviation
            del df_out[std_var]

    return df_out


def resample_polar_dataframe(df, magnitude_label, angle_label, units="degrees", average_period="30min", log_vars=[],
                             ignore=[]):
    """
    Resamples a polar dataset. First converts from polar to cartesian, then the dataset is resampled using the
    resample_dataset function, then the resampled dataset is converted back to polar coordinates.
    :param df: input dataframe
    :param magnitude_label: magnitude column label
    :param angle_label: angle column label
    :param units: angle units 'degrees' or 'radians'
    :param average_period:
    :return: average period (e.g. 1h, 15 min, etc.)
    """
    df = df.copy()
    df = delete_vars_from_df(df, ignore)

    # column_order = df.columns  # keep the column order

    columns = [col for col in df.columns if not col.endswith("_qc")]  # get columns names (no qc vars)
    column_order = []
    for col in columns:
        column_order += [col, col + "_qc", col + "_std"]

    precisions = get_dataframe_precision(df)

    # replace 0s with a tiny number to avoid 0 angle when module is 0
    almost_zero = 10 ** (-int((precisions[magnitude_label] + 2)))  # a number 100 smaller than the variable precision
    df[magnitude_label] = df[magnitude_label].replace(0, almost_zero)

    # Calculate sin and cos of the angle to allow resampling
    expanded_df = expand_angle_mean(df, magnitude_label, angle_label, units=units)
    # Convert from polar to cartesian
    cartesian_df = polar_to_cartesian(expanded_df, magnitude_label, angle_label, units=units, x_label="x", y_label="y",
                                      delete=False)
    # Resample polar dataframe
    resampled_cartesian_df = resample_dataframe(cartesian_df, average_period=average_period, std_column=True,
                                                log_vars=log_vars, ignore=ignore)
    # re-calculate the angle (use previously calculated sin / cos values)
    resampled_cartesian_df = calculate_angle_mean(resampled_cartesian_df, angle_label, units=units)
    # convert to polar
    resampled_df = cartesian_to_polar(resampled_cartesian_df, "x", "y", units=units, magnitude_label=magnitude_label,
                                      angle_label=angle_label)

    resampled_df = resampled_df.reindex(columns=column_order)  # reindex columns

    # Convert all QC flags to int8
    for c in resampled_df.columns:
        if c.endswith("_qc"):
            resampled_df[c] = resampled_df[c].replace(np.nan, -1)
            resampled_df[c] = resampled_df[c].astype(np.int8)
            resampled_df[c] = resampled_df[c].replace(-1, np.nan)

    # apply precision
    for c, precision in precisions.items():
        resampled_df[c] = resampled_df[c].round(decimals=precision)

    # delete STD for angle column
    if angle_label + "_std" in resampled_df.columns:
        del resampled_df[angle_label + "_std"]

    resampled_df = resampled_df.dropna(how="all")
    return resampled_df


def polar_to_cartesian(df, magnitude_label, angle_label, units="degrees", x_label="X", y_label="y", delete=True):
    """
    Converts a dataframe from polar (magnitude, angle) to cartesian (X, Y)
    :param df: input dataframe
    :param magnitude_label: magnitude's name in dataframe
    :param angle_label: angle's name in dataframe
    :param units: angle units, it must be "degrees" or "radians"
    :param x_label: label for the cartesian x values (defaults to X)
    :param y_label: label for the cartesian y values ( defaults to Y)
    :param delete: if True (default) deletes old polar columns
    :return: dataframe expanded with north and east vectors
    """
    magnitudes = df[magnitude_label].values
    angles = df[angle_label].values
    if units == "degrees":
        angles = np.deg2rad(angles)

    x = abs(magnitudes) * np.sin(angles)
    y = abs(magnitudes) * np.cos(angles)
    df[x_label] = x
    df[y_label] = y

    # Delete old polar columns
    if delete:
        del df[magnitude_label]
        del df[angle_label]

    if magnitude_label + "_qc" in df.columns:
        # If quality control has been applied to the dataframe, add also qc to the cartesian dataframe
        # Get greater QC flag, so both x and y have the most restrictive quality flag in magnitudes / angles
        df[x_label + "_qc"] = np.maximum(df[magnitude_label + "_qc"].values, df[angle_label + "_qc"].values)
        df[y_label + "_qc"] = df[x_label + "_qc"].values
        # Delete old qc flags
        if delete:
            del df[magnitude_label + "_qc"]
            del df[angle_label + "_qc"]

    return df


def cartesian_to_polar(df, x_label, y_label, units="degrees", magnitude_label="magnitude", angle_label="angle",
                       delete=True):
    """
    Converts a dataframe from cartesian (X, Y) to polar (magnitud, angle)
    :param df: input dataframe
    :param x_label: X column name (input)
    :param y_label: Y column name (input)
    :param magnitude_label: magnitude column name (output)
    :param angle_label: angle column name (output)
    :param units: angle units, it must be "degrees" or "radians"
    :param delete: if True (default) deletes old cartesian columns
    :return: dataframe expanded with north and east vectors
    """
    x = df[x_label].values
    y = df[y_label].values
    magnitudes = np.sqrt((np.power(x, 2) + np.power(y, 2)))  # magnitude is the module of the x,y vector
    angles = np.arctan2(x, y)
    angles[angles < 0] += 2 * np.pi  # change the range from -pi:pi to 0:2pi

    if units == "degrees":
        angles = np.rad2deg(angles)

    df[magnitude_label] = magnitudes
    df[angle_label] = angles

    if delete:
        del df[x_label]
        del df[y_label]
    if x_label + "_qc" in df.columns:
        # If quality control has been applied to the dataframe, add also qc to the cartesian dataframe
        # Get greater QC flag, so both x and y have the most restrictive quality flag in magnitudes / angles
        df[magnitude_label + "_qc"] = np.maximum(df[y_label + "_qc"].values, df[x_label + "_qc"].values)
        df[angle_label + "_qc"] = df[x_label + "_qc"].values
        # Delete old qc flags
        del df[x_label + "_qc"]
        del df[y_label + "_qc"]
    return df


def average_angles(magnitude, angle):
    """
    Averages
    :param wind:
    :param angle:
    :return:
    """
    u = []
    v = []
    for i in range(len(magnitude)):
        u.append(np.sin(angle[i] * np.pi / 180.) * magnitude[i])
        v.append(np.cos(angle[i] * np.pi / 180.) * magnitude[i])
    u_mean = np.nanmean(u)
    v_mean = np.nanmean(v)
    angle_mean = np.arctan2(u_mean, v_mean)
    angle_mean_deg = angle_mean * 180 / np.pi  # angle final en degreee
    return angle_mean_deg


def expand_angle_mean(df, magnitude, angle, units="degrees"):
    """
    Expands a dataframe with <angle>_sin and <angle>_cos. This
    :param df: input dataframe
    :param magnitude: magnitude columns name
    :param angle: angle column name
    :param units: "degrees" or "radians"
    :return: expanded dataframe
    """
    if units in ["degrees", "deg"]:
        angle_rad = np.deg2rad(df[angle].values)
    elif units in ["radians" or "rad"]:
        angle_rad = df[angle].values
    else:
        raise ValueError("Unkonwn units %s" % units)

    df[angle + "_sin"] = np.sin(angle_rad) * df[magnitude].values
    df[angle + "_cos"] = np.cos(angle_rad) * df[magnitude].values

    if magnitude + "_qc" in df.columns:
        # genearte a qc column with the MAX from magnitude qc and angle qc
        df[angle + "_sin" + "_qc"] = np.maximum(df[angle + "_qc"].values, df[magnitude + "_qc"].values)
        df[angle + "_cos" + "_qc"] = df[angle + "_sin" + "_qc"].values

    return df


def log_to_lin(df: pd.DataFrame, vars: list):
    """
    Converts some columns of a dataframe from logarithmic to linear
    :param df: dataframe
    :param vars: list of variables to convert
    :returns: dataframe with convert columns
    """

    for var in df.columns:
        if var in vars:
            df[var] = 10 ** (df[var] / 10)
    return df


def lin_to_log(df: pd.DataFrame, vars: list):
    """
    Converts some columns of a dataframe from linear to logarithmic
    :param df: dataframe
    :param vars: list of variables to convert
    :returns: dataframe with convert columns
    """
    for var in df.columns:
        if var in vars:
            df[var] = 10 * np.log10(df[var])
    return df


def calculate_angle_mean(df, angle, units="degrees"):
    """
    This function calculates the mean of an angle (expand_angle_mean should be called first).
    :param df: input df (expanded with expand angle mean)
    :param angle: angle column name
    :param units: "degrees" or "radians"
    :return:
    """
    sin = df[angle + "_sin"].values
    cos = df[angle + "_cos"].values
    df[angle] = np.arctan2(sin, cos)
    if units in ["degrees", "deg"]:
        df[angle] = np.rad2deg(df[angle].values)

    del df[angle + "_sin"]
    del df[angle + "_cos"]
    return df


def datastream_from_name(datastreams: pd.DataFrame, name: str):
    """
    Loops through a dataframe until an element with a certain name is found.
    :param datastreams: dataframe
    :param name: string to be compared with "name" column
    :return" matching dataframe row
    """
    for _, ds in datastreams.iterrows():
        if ds["name"] == name:
            return ds
    raise LookupError(f"Datastream with name {name} not found")


def raw_data_to_oceansites_naming(datastream_name):
    """
    Takes a raw data name (e.g. OBSEA:SBE37:PSAL:raw_data) to oceansites name (PSAL)
    """
    return datastream_name.split(":")[2]


def raw_data_to_oceansites_naming_dataframe(df, datastream_name):
    """
    Renames a dataframe from raw_data table (value, qc_flag) to variable names, e.g. if variable name TEMP, then
    value->TEMP qc_flag->TEMP_qc
    """
    varname = raw_data_to_oceansites_naming(datastream_name)
    varname_qc = varname + "_qc"
    varname_std = varname + "_std"
    df = df.rename(columns={"value": varname, "qc_flag": varname_qc, "stdev": varname_std})
    return df, varname


def oceansites_to_raw_data_naming_dataframe(df, datastream_name):
    """
    Renames a dataframe from variables names to raw data names:
    TEMP->value, TEMP_qc->qc_flag, etc.
    """
    varname = datastream_name.split(":")[2]
    varname_qc = varname + "_qc"
    varname_std = varname + "_std"
    df = df.rename(columns={varname: "value", varname_qc: "qc_flag", varname_std: "stdev"})
    return df


def merge_dataframes_by_columns(dataframes: list, timestamp="timestamp"):
    """
    Merge a list of dataframes to a single dataframe by merging on timestamp
    :param dataframes:
    :param timestamp:
    :return:
    """
    df = dataframes[0]
    for i in range(1, len(dataframes)):
        temp_df = dataframes[i].copy()
        df = df.merge(temp_df, on=timestamp, how="outer")
        del temp_df
        dataframes[i] = None
        gc.collect()
    return df


def __threadify_index_handler(index, handler, args):
    """
    This function adds the index to the return of the handler function. Useful to sort the results of a
    multi-threaded operation
    :param index: index to be returned
    :param handler: function handler to be called
    :param args: list with arguments of the function handler
    :return: tuple with (index, xxx) where xxx is whatever the handler function returned
    """
    result = handler(*args)  # call the handler
    return index, result  # add index to the result


def threadify(arg_list, handler, max_threads=10, text: str = "progress..."):
    """
    Splits a repetitive task into several threads
    :param arg_list: each element in the list will crate a thread and its contents passed to the handler
    :param handler: function to be invoked by every thread
    :param max_threads: Max threads to be launched at once
    :return: a list with the results (ordered as arg_list)
    """
    index = 0  # thread index
    with futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        threads = []  # empty thread list
        results = []  # empty list of thread results
        for args in arg_list:
            # submit tasks to the executor and append the tasks to the thread list
            threads.append(executor.submit(__threadify_index_handler, index, handler, args))
            index += 1

        # wait for all threads to end
        with Progress() as progress:  # Use Progress() to show a nice progress bar
            task = progress.add_task(text, total=index)
            for future in futures.as_completed(threads):
                future_result = future.result()  # result of the handler
                results.append(future_result)
                progress.update(task, advance=1)

        # sort the results by the index added by __threadify_index_handler
        sorted_results = sorted(results, key=lambda a: a[0])

        final_results = []  # create a new array without indexes
        for result in sorted_results:
            final_results.append(result[1])
        return final_results


def multiprocess(arg_list, handler, max_workers=20):
    """
    Splits a repetitive task into several processes
    :param arg_list: each element in the list will crate a thread and its contents passed to the handler
    :param handler: function to be invoked by every thread
    :param max_threads: Max threads to be launched at once
    :return: a list with the results (ordered as arg_list)
    :param text: text to be displayed in the progress bar
    """
    index = 0  # thread index
    ctx = mp.get_context('spawn')
    with futures.ProcessPoolExecutor(max_workers=max_workers, mp_context=ctx) as executor:
        processes = []  # empty thread list
        results = []  # empty list of thread results
        for args in arg_list:
            # submit tasks to the executor and append the tasks to the thread list
            processes.append(executor.submit(__threadify_index_handler, index, handler, args))
            index += 1

        for future in futures.as_completed(processes):
            future_result = future.result()  # result of the handler
            results.append(future_result)

        # sort the results by the index added by __threadify_index_handler
        sorted_results = sorted(results, key=lambda a: a[0])

        final_results = []  # create a new array without indexes
        for result in sorted_results:
            final_results.append(result[1])
        return final_results


class CkanClient(LoggerSuperclass):
    def __init__(self, baseurl, api_key, logger, log_colour=CYN):
        """
        Creates a CKAN client
        :param baseurl: CKAN's base URL
        :param api_key: CKAN's API key
        :param config_file: M3's JSON configuration file
        """
        LoggerSuperclass.__init__(self, logger, "CKAN", colour=log_colour)

        if baseurl[-1] != "/":
            baseurl += "/"
        self.baseurl = baseurl + "api/3/action/"
        self.api_key = api_key
        self.dataset = {}  # CKAN dataset info

    def get_organization_list(self):
        """
        Get the organizations in the CKAN
        :return: list of dict's with organizations
        """
        url = self.baseurl + "organization_list"
        return self.ckan_get(url)

    def get_license_list(self):
        url = self.baseurl + "license_list"
        return self.ckan_get(url)

    def get_dataset_list(self):
        url = self.baseurl + "current_package_list_with_resources"
        resp = self.ckan_get(url)
        return resp

    def package_create(self, name, title, description="", id="", private=False, author="", author_email="",
                       license_id="cc-by", groups=[], owner_org=""):
        """
        Generates a CKAN dataset (package), more info:
        https://docs.ckan.org/en/2.9/api/index.html#ckan.logic.action.create.package_create

        :param name:
        :param title:
        :param private:
        :param author:
        :param author_email:
        :param license_id:
        :param groups:
        :param owner_org:
        :return: CKAN's response as JSON dict
        """
        url = self.baseurl + "package_create"
        id = normalize_string(id)
        data = {
            "name": name,
            "title": title,
            "notes": description,
            "id": id,
            "private": private,
            "author": author,
            "author_email": author_email,
            "groups": groups,
            "license_id": license_id,
            "owner_org": owner_org
        }
        return self.ckan_post(url, data)

    def upload_file(self, dataset_id, name, filename, description=""):
        t = time.time()
        self.info(f"Uploading {filename}...")
        description = description
        resource_id = normalize_string(os.path.basename(filename)).split(".")[0]
        r = self.__add_resource(dataset_id, resource_id, description=description, upload_file=filename, name=name)
        self.debug(f"done! took {time.time() - t :.03f} s")
        return r

    def __add_resource(self, package_id, id, description="", name="", upload_file=None, resource_url="", format=""):
        """
        Adds a resource to a dataset
        :param datadict: Dictionary with metadata
        :param dataset_id: ID of the dataset
        :param resource: resource file
        """
        # package_id (string) – id of package that the resource should be added to.
        # url (string) – url of resource
        # description (string) – (optional)
        # format (string) – (optional)
        # hash (string) – (optional)
        # name (string) – (optional)
        # resource_type (string) – (optional)
        # mimetype (string) – (optional)
        # mimetype_inner (string) – (optional)
        # cache_url (string) – (optional)
        # size (int) – (optional)
        # created (iso date string) – (optional)
        # last_modified (iso date string) – (optional)
        # cache_last_updated (iso date string) – (optional)
        # upload (FieldStorage (optional) needs multipart/form-data) – (optional)

        resource = self.__check_if_resource_exists(id)
        if resource:
            rich.print("[yellow]Resource %s already registered!" % id)
            return resource

        datadict = {
            "id": id,
            "package_id": package_id,
            "description": description,
            "name": name,
        }

        if format:
            datadict["format"] = format
        elif upload_file:
            datadict["format"] = upload_file.split(".")[-1]

        if resource_url:
            datadict["url"] = resource_url

        url = self.baseurl + "resource_create"
        return self.ckan_post(url, datadict, file=upload_file)

    def __check_if_package_exists(self, id):
        """
        Checkds if a pacakge exists
        :param id: package id
        :return:
        """
        return self.__check_if_exists("package_show", id)

    def __check_if_resource_exists(self, id):
        """
        Checkds if a pacakge exists
        :param id: package id
        :return:
        """
        return self.__check_if_exists("resource_show", id)

    def __check_if_exists(self, endpoint, id):
        """
        Tries to get an entity to check if it exists or not
        :param endpoint: entity enpoint, e.g. "package_show" for dataset, "resource_show" for resource...
        :param id: id of the entity
        :return: None if it does not exit, otherwise returns the entity
        """
        url = self.baseurl + endpoint
        data = {"id": id}

        try:
            entity = self.ckan_get(url, data=data)
        except ValueError:
            return None

        return entity  # only if it exists!

    def get_resource(self, resource_id):
        url = self.baseurl + "resource_show"
        data = {
            "id": resource_id
        }
        return self.ckan_get(url, data=data)

    def get_group_list(self):
        """
        Gets CKAN's groups
        """
        url = self.baseurl + "group_list"
        return self.ckan_get(url)

    def group_create(self, name, id="", title="", description="", image_url="", extras=[]):
        """
        Creates a group (project) in CKAN
        +info: https://docs.ckan.org/en/2.9/api/index.html#ckan.logic.action.create.group_create
        :param name: group's name
        :param id: group's id (optional)
        :param title: group's title (optinal)
        :param description: grop's description (optional)
        :param image_url: group's image (optinal)
        :param extras: additional key-value pairs
        :return:
        """
        group_data = {
            "name": name,
            "id": id,
            "title": title,
            "description": description,
            "image_url": image_url,
            "extras": extras
        }
        url = self.baseurl + "group_create"
        return self.ckan_post(url, group_data)

    def ckan_get(self, url, data={}):
        headers = {"Authorization": self.api_key, 'Content-Type': "application/x-www-form-urlencoded"}
        resp = requests.get(url, headers=headers, params=data)
        if resp.status_code > 300:
            json_resp = json.loads(resp.text)
            raise ValueError("CKAN returned error")
        return json.loads(resp.text)["result"]

    def ckan_post(self, url, data, file=None):
        headers = {"Authorization": self.api_key}
        resource = []
        if file:
            resource = [("upload", open(file))]
        else:
            data = json.dumps(data)
            headers['Content-Type'] = "application/x-www-form-urlencoded"
        resp = requests.post(url, data=data, headers=headers, files=resource)
        if resp.status_code > 300:
            raise ValueError(f"CKAN returned http error {resp.status_code}: {resp.text}")
        return json.loads(resp.text)["result"]


if __name__ == "__main__":
    pass
