"""A python script to send SMS about the info of an event"""

import os
import sys
import hmac
import json
import pprint
import logging
import hashlib

import pymysql
import requests
import pandas as pd


def get_config(file_name):
    """
    A function which takes the path of the configuration
    file and returns a config object.

    Parameters
    ----------

    file_name: str
        Path of JSON configuration file.

    Returns
    -------

    config: dict
        Dictionary object created from JSON configuration.

    Raises
    ------

    FileNotFoundError:
        This error is raised when the configuration file
        path is invalid.

    json.decoder.JSONDecodeError:
        When the configuration file does not contain
        valid JSON.
    """

    try:
        with open(file_name) as config_file:
            config = json.load(config_file)
        return config

    except FileNotFoundError as file_err:
        logging.exception(str(file_err))
    except json.decoder.JSONDecodeError as json_err:
        logging.exception(str(json_err))
    except Exception as ex:
        logging.exception(str(ex))


def sha_256_hmac(key, msg):
    """
    A function to find first 6 characters of a given message using
    provided key value.

    Parameters
    ----------
    key: str
        The key for finding HMAC.
    msg: str
        The message for which HMAC has to be found.

    Returns
    -------
    str:
        First 6 characters of the HMAC.
    """

    return hmac.new(
        key.encode("utf-8"),
        msg=msg.encode("utf-8"),
        digestmod=hashlib.sha256
    ).hexdigest()[:6]


def get_data(connection, query):
    """
    A function which takes a database connection and a query,
    executes the query and returns the results.

    Parameters
    ----------
    connection: str
        Database connection object.

    query: str
        The SQL query to be executed on the database.

    Returns
    -------
    pandas.core.frame.DataFrame
        DataFrame containing the results obtained by
        executing the query.
    """

    return pd.read_sql(query, connection)


def send_sms(data):
    """
    A function to send SMS using Textlocal API.

    Parameters
    ----------
    data: dict
        A dictionary containing all parameters for the request.

    Returns
    -------
    requests.Response
        The response of the API call.
    """
    return requests.post("http://api.textlocal.in/send/", data=data)


def main():
    try:
        # Initialize logging module
        logging.basicConfig(filename="logs.log", filemode="a", level=logging.DEBUG,
                            format="\n%(asctime)s  %(levelname)s: %(message)s")
        # Name of configuration file
        config_file_name = "config.json"
        # Get absolute file path of configuration file
        abs_config_path = os.path.join(os.path.abspath("."), config_file_name)
        # Get configuration from the configuration file
        config = get_config(abs_config_path)

        # Retrieve database credentials
        host = config["mysql_host"]
        user = config["mysql_user"]
        password = config["mysql_pass"]
        database = config["mysql_db"]

        # Connect to MySQL database
        connection = pymysql.connect(host=host, user=user, passwd=password, db=database)
        logging.info("Connected to database {} on host {} ".format(database, host))

        event = sys.argv[1]

        query = "SELECT name_1, mobile FROM participations WHERE event = '{}'"
        logging.info(query)

        participants = get_data(connection, query.format(event))
        participants["password"] = participants.apply(lambda row: sha_256_hmac("key", str(row["mobile"])), axis=1)

        data = {
            "numbers": "Comma separated mobile numbers or a single mobile number",
            "message": "Message",
            "username": config["text_local_user"],
            "hash": config["text_local_hash"],
            "sender": config["text_local_sender"],
            "custom": ""
        }

        for _, row in participants.iterrows():
            data["numbers"] = str(int(row["mobile"]))
            data["message"] = config[event].format(password=sha_256_hmac("event-secret", str(int(row["mobile"]))))
            data["custom"] = row["name_1"]

            logging.info("API call params = {}".format(data))
            # Send SMS
            response = send_sms(data)
            pprint.pprint(response.json())
            logging.info("API call response = {}".format(json.dumps(response.json())))

        connection.close()

    except FileNotFoundError as file_err:
        logging.exception(str(file_err))
    except json.decoder.JSONDecodeError as json_err:
        logging.exception(str(json_err))
    except KeyError as key_err:
        logging.exception(str(key_err))
    except pymysql.err.Error as pymysql_err:
        logging.exception(pymysql_err)
    except Exception as ex:
        logging.exception(str(ex))


if __name__ == '__main__':
    main()
