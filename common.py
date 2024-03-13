#!/usr/bin/env python3
"""

author: Enoc Martínez
institution: Universitat Politècnica de Catalunya (UPC)
email: enoc.martinez@upc.edu
license: MIT
created: 28/03/2022
"""
import traceback
from threading import Thread, Event
import logging
from logging.handlers import TimedRotatingFileHandler
from queue import Queue
import os
from socket import socket, AF_INET, SOCK_DGRAM, SOL_SOCKET, SO_REUSEADDR, timeout
import rich
import psycopg2
import json
import time
import numpy as np
import pandas as pd

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


def merge_dataframes_by_columns(dataframes: list, timestamp="timestamp"):
    """
    Merge a list of dataframes to a single dataframe by merging on timestamp
    :param dataframes:
    :param timestamp:
    :return:
    """
    if len(dataframes) < 1:
        raise ValueError(f"Got {len(dataframes)} dataframes in list!")
    df = dataframes[0]
    for i in range(1, len(dataframes)):
        temp_df = dataframes[i].copy()
        df = df.merge(temp_df, on=timestamp, how="outer")
        del temp_df
        dataframes[i] = None
        gc.collect()
    return df


def merge_dataframes(df_list, sort=False):
    """
    Appends together several dataframes into a big dataframe
    :param df_list: list of dataframes to be appended together
    :param sort: If True, the resulting dataframe will be sorted based on its index
    :return:
    """
    df = df_list[0]
    for new_df in df_list[1:]:
        df = df.append(new_df)

    if sort:
        df = df.sort_index(ascending=True)
    return df


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


def assert_dict(conf: dict, required_keys: dict, verbose=False):
    """
    Checks if all the expected keys in a dictionary are there. The expected format is field name as key and type as
    value:
        { "name": str, "importantNumber": int}

    One level of nesting is supported:
    value:
        { "someData/nestedData": str}
    expects something like
        {
        "someData": {
            "nestedData": "hi"
            }
        }

    :param conf: dict with configuration to be checked
    :param required_keys: dictionary with required keys
    :raises: AssertionError if the input does not match required_keys
    """
    for key, expected_type in required_keys.items():
        if "/" in key:
            pass
        elif key not in conf.keys():
            raise AssertionError(f"Required key \"{key}\" not found in configuration")

        # Check for nested dicts
        if "/" in key:
            parent, son = key.split("/")
            if parent not in conf.keys():
                msg =f"Required key \"{parent}\" not found!"
                if verbose:
                    rich.print(f"[red]{msg}")
                raise AssertionError(msg)

            if type(conf[parent]) != dict:
                msg = f"Value for key \"{parent}\" wrong type, expected type dict, but got {type(conf[parent])}"
                if verbose:
                    rich.print(f"[red]{msg}")
                raise AssertionError(msg)
            if son not in conf[parent].keys():
                msg =f"Required key \"{son}\" not found in configuration/{parent}"
                if verbose:
                    rich.print(f"[red]{msg}")
                raise AssertionError(msg)
            value = conf[parent][son]
        else:
            value = conf[key]

        if type(value) != expected_type:
            msg = f"Value for key \"{key}\" wrong type, expected type {expected_type}, but got '{type(value)}'"
            if verbose:
                rich.print(f"[red]{msg}")
            raise AssertionError(msg)


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

    def run_query(self, query, description=False, debug=False, fetch=True):
        """
        Executes a query and returns the result. If description=True the desription will also be returned
        """
        self.available = False
        if debug:
            rich.print("[magenta]%s" % query)
        self.cursor.execute(query)
        self.connection.commit()
        if fetch:
            resp = self.cursor.fetchall()
            self.available = True
            if description:
                return resp, self.cursor.description
            return resp
        else:
            self.available = True
            return

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


    def exec_query(self, query, description=False, debug=False, fetch=True):
        """
        Runs a query in a free connection
        """
        c = self.get_available_connection()
        results = None
        try:
            results = c.run_query(query, description=description, debug=debug, fetch=fetch)

        except psycopg2.errors.UniqueViolation as e:
            # most likely a duplicated key, raise it again
            c.connection.rollback()
            c.available = True  # set it to available
            rich.print(f"[yellow]{traceback.format_exc()}")
            raise e

        except Exception as e:
            rich.print(f"[yellow]{traceback.format_exc()}")
            rich.print(f"[red]Exception in exec_query {e}")
            try:
                rich.print("[white]closing db connection due to exception")
                c.close()
                rich.print("[white]closed")
            except:  # ignore errors
                pass
            rich.print(f"[red]REMOVING CONNECTION")
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
    def __init__(self, host, port, db_name, db_user, db_password, logger, timescaledb=True):
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
        self.__sensor_properties = {}

        if timescaledb:
            self.timescale = TimescaleDB(self, logger)
        else:
            self.timescale = None

        # This dicts provide a quick way to get the relations between elements in the database without doing a query
        # they are initialized with __initialize_dicts()
        self.datastream_id_varname = {}      # key: datastream_id, value: variable name
        self.datastream_id_sensor_name = {}  # key: datastream_id, value: sensor name
        self.sensor_id_name = {}             # key: sensor id, value: name
        self.thing_id_name = {}             # key: sensor id, value: name
        self.datastream_name_id = {}         # key: datastream name, value: datastream id
        self.obs_prop_name_id = {}           # key: observed property name, value: observed property id

        # dictionaries where sensors key is name and value is ID
        self.__initialize_dicts()

        # dictionaries where key is ID and value is name
        self.sensor_name_id = reverse_dictionary(self.sensor_id_name)
        self.datastream_id_name = reverse_dictionary(self.datastream_name_id)
        #self.obs_prop_id_name = reverse_dictionary(self.obs_prop_name_id)
        self.thing_name_id = reverse_dictionary(self.thing_id_name)
        self.obs_prop_id_name = reverse_dictionary(self.obs_prop_name_id)
        self.datastream_properties = self.get_datastream_properties()

        # Get the list of current constraints
        self.pg_constraints = self.list_from_query(f"select conname from pg_constraint;")


        # Create unique name constrain in Datastreams, Sensors, Things, FeaturesOfInterest, Obseerve
        self.add_unique_name_constraints()
        self.add_unique_observation_constraint()

        # TODO: Instead of a unique constraint for OBSERVATION, add these two index
        # /*CREATE UNIQUE INDEX observations_unique_time_datastream_idx ON "OBSERVATIONS" ("PHENOMENON_TIME_START", "DATASTREAM_ID") WHERE  "PARAMETERS" IS NULL;
        # CREATE UNIQUE INDEX observations_unique_time_datastream_param_idx ON "OBSERVATIONS" ("PHENOMENON_TIME_START", "DATASTREAM_ID", "PARAMETERS") WHERE  "PARAMETERS" IS NOT NULL;*/

    def add_unique_name_constraints(self):
        __unique_list = ["DATASTREAMS", "SENSORS", "THINGS", "FEATURES", "OBS_PROPERTIES"]
        for table in __unique_list:
            # First, check if it already exists
            constraint_name = table.lower() + "_unique_name"
            constraint_query = f"alter table \"{table}\" add constraint {constraint_name} unique (\"NAME\")"
            self.add_constraint(constraint_name, constraint_query)

    def add_unique_observation_constraint(self):
        """
        Adds unique index to OBSERVATIONS table to avoid duplicated measures. Two indexes are created, one for
        regular data when parameters=null, and another one for profiles (parameters!=null).
        """
        q = 'CREATE UNIQUE INDEX observations_unique_time_datastream_idx ON "OBSERVATIONS" ("PHENOMENON_TIME_START", "DATASTREAM_ID") WHERE  "PARAMETERS" IS NULL;'
        self.add_constraint("observations_unique_time_datastream_idx", q)
        q = 'CREATE UNIQUE INDEX observations_unique_time_datastream_param_idx ON "OBSERVATIONS" ("PHENOMENON_TIME_START", "DATASTREAM_ID", "PARAMETERS") WHERE  "PARAMETERS" IS NOT NULL;'
        self.add_constraint("observations_unique_time_datastream_param_idx", q)

    def add_constraint(self, constraint_name, query):
        """
        Checks if a constraint is already present in pg_constraint table. If not, create it using the query
        """
        if constraint_name not in self.pg_constraints:
            self.exec_query(query, fetch=False)
            self.pg_constraints.append(constraint_name)
        else:
            self.info(f"Constraint {constraint_name} already exists!")


    def sensor_var_from_datastream(self, datastream_id):
        """
        Returns the sensor name and variable name from a datastream_id
        :param datastream_id: datastream ID
        :returns: a tuple of sensor_name, varname
        """
        sensor_name = self.datastream_id_sensor_name[datastream_id]
        varname = self.datastream_id_varname[datastream_id]
        return sensor_name, varname

    def __initialize_dicts(self):
        """
        Initialize the dicts used for quickly access to relations without querying the database
        """
        # DATASTREAM -> SENSOR relation
        query = """
            select "DATASTREAMS"."ID" as datastream_id, "SENSORS"."ID" as sensor_id, "SENSORS"."NAME" as sensor_name, 
            "DATASTREAMS"."NAME" as datastream_name
            from "DATASTREAMS", "SENSORS" 
            where "DATASTREAMS"."SENSOR_ID" = "SENSORS"."ID" order by datastream_id asc;"""
        df = self.dataframe_from_query(query)

        # key: datastream_id ; value: sensor name
        self.datastream_id_sensor_name = dataframe_to_dict(df, "datastream_id", "sensor_name")
        datastream_name_dict = dataframe_to_dict(df, "datastream_id", "datastream_name")

        # convert datastream name into variable name "OBSEA:SBE16:TEMP:raw_data -> TEMP
        self.datastream_id_varname = {key: name.split(":")[2] for key, name in datastream_name_dict.items()}

        # SENSOR ID -> SENSOR NAME
        self.sensor_id_name = self.get_sensors()  # key: sensor_id, value: sensor_name
        self.thing_id_name = self.get_things()  # key: sensor_id, value: sensor_name

        # DATASTREAM_NAME -> DATASTREAM_ID
        df = self.dataframe_from_query(f'select "ID", "NAME" from "DATASTREAMS";')
        self.datastream_name_id = dataframe_to_dict(df, "NAME", "ID")

        # OBS_PROPERTY NAME -> OBS_PROPERTY ID
        df = self.dataframe_from_query('select "ID", "NAME" from "OBS_PROPERTIES";')
        self.obs_prop_name_id = dataframe_to_dict(df, "NAME", "ID")

    def get_sensor_datastreams(self, sensor_id):
        """
        Returns a dataframe with all datastreams belonging to a sensor
        :param sensor_id: ID of a sensor
        :return: dataframe with datastreams ID, NAME and PROPERTIES
        """
        query = (f'select "ID" as id , "NAME" as name, "THING_ID" as thing_id, "OBS_PROPERTY_ID" AS obs_prop_id,'
                 f' "PROPERTIES" as properties from "DATASTREAMS" where "SENSOR_ID" = {sensor_id};')
        df = self.dataframe_from_query(query)
        return df

    def get_sensor_qc_metadata(self, name: str):
        """
        Returns an object with all the necessary information to apply QC to all variables from the sensor
        :return:
        """
        sensor_id = self.sensor_id_name[name]
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
                "datastream_id": int(self.datastream_name_id[ds_name]),
                "quality_control": qc_config
            }
        return data

    def get_qc_config(self, raw_sensor_data_flag="rawSensorData") -> dict:
        """
        Gets the QC config for all sensors from the database and stores it in a dictionary
        :return: dict with the configuration
        """
        sensors = {}

        for sensor, sensor_id in self.sensor_id_name.items():
            sensors[sensor] = {"datastreams": {}, "id": sensor_id, "name": sensor}

            # Select Datastreams belonging to a sensor and expand rawData flag and quality control config
            q = f'select "ID" as id, "NAME" as name, ("PROPERTIES"->>\'QualityControl\') as qc_config, ' \
                f'("PROPERTIES"->>\'{raw_sensor_data_flag}\')::BOOLEAN as is_raw_data ' \
                f' from "DATASTREAMS" where "SENSOR_ID" = {sensor_id};'

            df = self.dataframe_from_query(q, debug=False)

            for index, row in df.iterrows():
                if not row["is_raw_data"]:
                    self.warning(f"[yellow] ignoring datastream {row['name']}")
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


    def get_datastream_sensor(self, fields=["ID", "SENSOR_ID"]):
        select_fields = ", ".join(fields)
        df = self.dataframe_from_query(f'select {select_fields} from "DATASTREAMS";')
        return dataframe_to_dict(df, "NAME", "SENSOR_ID")


    def get_datastream_properties(self, fields=["ID", "PROPERTIES"]):
        select_fields = f'"{fields[0]}"'
        for f in fields[1:]:
            select_fields += f', "{f}"'

        df = self.dataframe_from_query(f'select {select_fields} from "DATASTREAMS";')
        return dataframe_to_dict(df, "ID", "PROPERTIES")

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


    def get_data(self, identifier, time_start: str, time_end: str):
        """
        Access the 0BSERVATIONS data table and exports all data between time_start and time_end
        :param identifier: datasream name (str) or datastream id (int)
        :param time_start: start time
        :param time_end: end time  (not included)
        """
        if type(identifier) == int:
            pass
        elif type(identifier) == str:  # if string, convert from name to ID
            identifier = self.datastream_name_id[identifier]

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
            self.debug(f"Dropping stdev for {self.datastream_id_name[identifier]}")
            del df["stdev"]
        return df.set_index("timestamp")

    def dataframe_from_datastream(self, datastream_id: int, time_start: pd.Timestamp, time_end: pd.Timestamp):
        """
        Takes the ID of a datastream and exports its data to a dataframe.
        """
        df = self.dataframe_from_query(f'select "PROPERTIES" from "DATASTREAMS" where "ID"={datastream_id};')
        if df.empty:
            raise ValueError(f"Datastream with ID={datastream_id} not found in database!")
        properties = df.values[0][0]  # only one value with the properties in JSON format
        # if the datastream has rawSensorData=True in properties, export from raw_data
        if self.timescale and properties["rawSensorData"]:
            df = self.timescale.get_raw_data(datastream_id, time_start, time_end)
        else:  # otherwise export from observations
            df = self.get_data(datastream_id, time_start, time_end)
        df, _ = varname_from_datastream_dataframe(df, self.datastream_id_name[datastream_id])
        return df

    def get_dataset(self, sensor:str, station_name:str, options: dict, time_start: pd.Timestamp, time_end:pd.Timestamp, variables: list=[])\
            -> pd.DataFrame:
        """
        Gets all data from a sensor deployed at a certain station from time_start to time_end. If variables is specified
        only a subset of variables will be returned.

        options is a dict with "rawData" key (true/false) and if False, averagePeriod MUST be specified like:
            {"rawData": false, "averagePeriod": "30min"}
                OR
            {"rawData": true}

        :param sensor: sensor name
        :param station: station name
        :param time_start:
        :param time_end:
        :param options: dict with rawData and optionally averagePeriod.
        :param variables: list of variable names
        :return:
        """
        # Make sure options are correct
        assert("rawData" in options.keys())
        if not options["rawData"]:
            assert("averagePeriod" in options.keys())

        raw_data = options["rawData"]

        if variables:
            rich.print(f"Keeping only variables: {variables}")

        sensor_id = self.sensor_id_name[sensor]
        station_id = self.thing_id_name[station_name]
        # Get all datastreams for a sensor
        datastreams = self.get_sensor_datastreams(sensor_id)
        # Keep only datastreams at a specific station
        datastreams = datastreams[datastreams["thing_id"] == station_id]

        # Drop datastreams with variables that are not in the list
        if variables:
            variable_ids = [self.obs_prop_name_id[v] for v in variables]
            rich.print(f"variables {variable_ids}")
            # keep only variables in list
            all_variable_ids = np.unique(datastreams["obs_prop_id"].values)
            remove_this = [var_id for var_id in all_variable_ids if var_id not in variable_ids]
            for remove_id in remove_this:
                datastreams = datastreams[datastreams["obs_prop_id"] != remove_id]  # keep others

        # Keep only datastreams that match data type
        remove_idxs = []
        for idx, row in datastreams.iterrows():
            if raw_data:
                if not row["properties"]["rawSensorData"]:
                    remove_idxs.append(idx)
            else: # Keep only those variables that are not raw_data and whose period matches the data_type
                if row["properties"]["rawSensorData"]:  # Do not keep raw data
                    remove_idxs.append(idx)
                elif row["properties"]["averagePeriod"] != options["averagePeriod"]:  # do not keep data with different period
                    remove_idxs.append(idx)

        datastreams = datastreams.drop(remove_idxs)

        # Query data for every datastream
        data = []
        for idx, row in datastreams.iterrows():
            datastream_id = row["id"]
            obs_prop_id = row["obs_prop_id"]
          #  df = self.get_data(datastream_id, time_start, time_end)
            df = self.dataframe_from_datastream(datastream_id, time_start, time_end)

            varname = self.obs_prop_id_name[obs_prop_id]
            rename = {"value": varname, "qc_flag": varname + "_QC"}
            if "stdev" in df.columns:
                rename["stdev"] = varname  + "_SD"
            df = df.rename(columns=rename)
            data.append(df)
        return merge_dataframes_by_columns(data)

    def check_if_table_exists(self, view_name):
        """
        Checks if a view already exists
        :param view_name: database view to check if exists
        :return: True if exists, False if it doesn't
        """
        # Select all from information_schema
        query = "SELECT table_name FROM information_schema.tables"
        df = self.dataframe_from_query(query)
        table_names = df["table_name"].values
        if view_name in table_names:
            return True
        return False

    def get_data_type(self, datastream_id):
        """
        Returns the data type of a datastream
        :param datastream_id: (int) id of the datastream
        :returns: (data_type: str, average: bool)
        """
        props = self.datastream_properties[datastream_id]
        data_type = props["dataType"]
        if "averagePeriod" in props.keys():
            average = True
        else:
            average = False
        return data_type, average


class TimescaleDB(LoggerSuperclass):
    def __init__(self, sta_db_connector, logger):
        """
        Creates a TimescaleDB object, which adds timeseries capabilities to the SensorThings Database.
        The following hypertables will be created:
            timeseries: regular timeseries data (timestamp, value, qc, datastream_id)
            profiles: depth-specific data (timestamp, depth, value, qc, datastream_id)
            detections: integer data (timestamp, counts, datastream_id)

        """
        timeseries_table = "timeseries"
        profiles_table = "profiles"
        detections_table = "detections"

        LoggerSuperclass.__init__(self, logger, "TSDB", colour=MAG)

        self.db = sta_db_connector
        self.timeseries_hypertable = timeseries_table
        self.profiles_hypertable = profiles_table

        default_interval = "30days"

        if not self.db.check_if_table_exists(self.timeseries_hypertable):
            self.info(f"TimescaleDB, initializing {self.timeseries_hypertable} as a hypertable")
            self.create_timeseries_hypertable(timeseries_table, chunk_interval_time=default_interval)
            self.add_compression_policy(timeseries_table, policy="30d")

        if not self.db.check_if_table_exists(profiles_table):
            self.info(f"TimescaleDB, initializing {profiles_table} as a hypertable")
            self.create_profiles_hypertable(profiles_table, chunk_interval_time=default_interval)
            self.add_compression_policy(profiles_table, policy="30d")

        if not self.db.check_if_table_exists(detections_table):
            self.info(f"TimescaleDB, initializing {detections_table} as a hypertable")
            self.create_detections_hypertable(detections_table, chunk_interval_time=default_interval)
            self.add_compression_policy(detections_table, policy="30d")


    def create_timeseries_hypertable(self, name, chunk_interval_time="30days"):
        """
        Creates a table with four parameters, the timestamp, the value, the qc_flag and aa datastream_id as foreing key
        :return:
        """
        if self.db.check_if_table_exists(name):
            self.warning(f"table '{name}' already exists")
            return None
        query = """
        CREATE TABLE {table_name} (
        timestamp TIMESTAMPTZ NOT NULL,
        value DOUBLE PRECISION NOT NULL,   
        qc_flag smallint,
        datastream_id smallint NOT NULL,
        CONSTRAINT {table_name}_pkey PRIMARY KEY (timestamp, datastream_id),        
        CONSTRAINT "{table_name}_datastream_id_fkey" FOREIGN KEY (datastream_id)
            REFERENCES public."DATASTREAMS" ("ID") MATCH SIMPLE
            ON UPDATE CASCADE
            ON DELETE CASCADE
        );""".format(table_name=name)
        self.info(f"creating table '{name}'...")
        self.db.exec_query(query, fetch=False)
        self.info("converting to hypertable...")
        query = f"SELECT create_hypertable('{name}', 'timestamp', 'datastream_id', 4, " \
                f"chunk_time_interval => INTERVAL '{chunk_interval_time}');"
        self.db.exec_query(query)

    def create_profiles_hypertable(self, name, chunk_interval_time="30days"):
        """
        Creates a table with four parameters, the timestamp, the value, the qc_flag and aa datastream_id as foreing key
        :return:
        """
        if self.db.check_if_table_exists(name):
            self.info("[yellow]WARNING: table already exists")
            return None
        query = """
        CREATE TABLE {table_name} (
        timestamp TIMESTAMPTZ NOT NULL,
        depth DOUBLE PRECISION NOT NULL,
        value DOUBLE PRECISION NOT NULL,   
        qc_flag INT8,
        datastream_id smallint NOT NULL,
        CONSTRAINT {table_name}_pkey PRIMARY KEY (timestamp, depth, datastream_id),        
        CONSTRAINT "{table_name}_datastream_id_fkey" FOREIGN KEY (datastream_id)
            REFERENCES public."DATASTREAMS" ("ID") MATCH SIMPLE
            ON UPDATE CASCADE
            ON DELETE CASCADE
        );""".format(table_name=name)
        self.info("creating table...")
        self.db.exec_query(query, fetch=False)
        self.info("converting to hypertable...")
        query = f"SELECT create_hypertable('{name}', 'timestamp', 'datastream_id', 4, " \
                f"chunk_time_interval => INTERVAL '{chunk_interval_time}');"
        self.db.exec_query(query)

    def create_detections_hypertable(self, name, chunk_interval_time="30days"):
        """
        Creates a hypertable to store AI predictions with their
        :return:
        """
        if self.db.check_if_table_exists(name):
            self.info("[yellow]WARNING: table already exists")
            return None
        query = """
        CREATE TABLE {table_name} (
        timestamp TIMESTAMPTZ NOT NULL,
        value smallint NOT NULL,
        datastream_id smallint NOT NULL,
        CONSTRAINT {table_name}_pkey PRIMARY KEY (timestamp, datastream_id),        
        CONSTRAINT "{table_name}_datastream_id_fkey" FOREIGN KEY (datastream_id)
            REFERENCES public."DATASTREAMS" ("ID") MATCH SIMPLE
            ON UPDATE CASCADE
            ON DELETE CASCADE
        );""".format(table_name=name)
        self.info("creating table...")
        self.db.exec_query(query, fetch=False)
        self.info("converting to hypertable...")
        query = f"SELECT create_hypertable('{name}', 'timestamp', 'datastream_id', 4, " \
                f"chunk_time_interval => INTERVAL '{chunk_interval_time}');"
        self.db.exec_query(query)

    def add_compression_policy(self, table_name, policy="30d"):
        """
        Adds compression policy to a hypertable
        """

        query = f"ALTER TABLE {table_name} SET (timescaledb.compress, timescaledb.compress_orderby = " \
                "'timestamp DESC', timescaledb.compress_segmentby = 'datastream_id');"
        self.db.exec_query(query, fetch=False)
        query = f"SELECT add_compression_policy('{table_name}', INTERVAL '{policy}', if_not_exists=>True); "
        self.db.exec_query(query)

    def compress_all(self, table_name, older_than="30days"):
        query = f"""
            SELECT compress_chunk(i, if_not_compressed => true) from 
            show_chunks(
            '{table_name}',
             now() - interval '{older_than}',
              now() - interval '100 years') i;
              """
        self.db.exec_query(query)

    def compression_stats(self, table) -> (float, float, float):
        """
        Returns compression stats
        :param table: hypertable name
        :returns: tuple like (MBytes before, MBytes after, compression ratio)
        """
        df = self.db.dataframe_from_query(f"SELECT * FROM hypertable_compression_stats('{table}');")
        bytes_before = df["before_compression_total_bytes"].values[0]
        bytes_after = df["after_compression_total_bytes"].values[0]
        if type(bytes_after) is type(None):
            return 0, 0, 0
        else:
            ratio = bytes_before / bytes_after
            return round(bytes_before / 1e6, 2), round(bytes_after / 1e6, 2), round(ratio, 2)

    def insert_to_timeseries(self,  timestamp: str, value: float, qc_flag: int, datastream_id: int):
        """
        Insert a single data point into the timeseries hypertable
        """
        query = f"insert into timeseries (timestamp, value, qc_flag, datastream_id) VALUES('{timestamp}', " \
                f"{value}, {qc_flag}, {datastream_id})"
        try:
            self.db.exec_query(query, fetch=False)
        except psycopg2.errors.UniqueViolation as e:
            return str(e)
        return None


    def insert_to_profiles(self, timestamp: str, depth: float, value: float, qc_flag: int, datastream_id: int, depth_precision=2):
        """
        Insert a single data point into the profiles hypertable
        """
        depth = round(float(depth), depth_precision)
        query = f"insert into profiles (timestamp, depth, value, datastream_id) " \
                 f"VALUES('{timestamp}', {depth}, {value}, {qc_flag}, {datastream_id})"
        try:
            self.db.exec_query(query, fetch=False)
        except psycopg2.errors.UniqueViolation as e:
            return str(e)
        return None

    def insert_to_detections(self, timestamp: str, value: int, datastream_id: int):
        """
        Insert a single data point into the timeseries hypertable
        """
        query = f"insert into detections (timestamp, value, datastream_id) VALUES('{timestamp}', " \
                f"{value},{datastream_id})"
        try:
            self.db.exec_query(query, fetch=False)
        except psycopg2.errors.UniqueViolation as e:
            return str(e)
        return None


if __name__ == "__main__":
    pass
