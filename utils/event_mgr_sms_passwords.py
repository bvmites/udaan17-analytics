"""A python script to email Messenger API passwords to the event managers"""

import os
import json
import hmac
import hashlib
import logging
from functools import partial

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
    """

    with open(file_name) as config_file:
        return json.load(config_file)


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


def sha_256_hmac(key, msg):
    """
    A function to find first 8 characters of a given message using
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
        First 8 characters of the HMAC.
    """

    return hmac.new(
        key.encode("utf-8"),
        msg=msg.encode("utf-8"),
        digestmod=hashlib.sha256
    ).hexdigest()[:8]


def send_mail(api_url, api_user, api_key, data):
    """
    A to send Email with an attachment using
    Mailgun API.

    Parameters
    ----------
    api_url: str
        Mailgun API URL.

    api_user: str
        Name of Mailgun user.

    api_key: str
        Mailgun API key.

    data: dict
        The data for the API call

    Returns
    -------
    requests.Response:
        Response of the API call
    """

    authorization = (api_user, api_key)

    return requests.post(api_url, auth=authorization, data=data)


def main():
    """
        Main function

    Raises
    ------
    FileNotFoundError:
        This error is raised when the configuration file
        path is invalid.

    json.decoder.JSONDecodeError:
        When the configuration file does not contain
        valid JSON.

    KeyError:
        When a particular parameter does not exist in
        the config or the DataFrame object.

    pymysql.err.Error:
        Base class of all errors raised by PyMySQL library.

    Exception:
        Handle any other exceptions.
    """

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
        logging.info("{}".format(str(connection)))

        # SELECT name and email id from database
        query = "A VALID SQL QUERY"
        logging.info("{}".format(str(query)))

        # Fetch the data
        event_managers = get_data(connection, query)
        # Drop rows having any null value
        filtered_event_managers = event_managers.dropna()

        # The key value for generating HMAC
        secret = "secret"
        # Create a partial function with default key value for reuse
        partial_hmac = partial(sha_256_hmac,secret)
        # Generate the password column from email
        filtered_event_managers["password"] = filtered_event_managers.apply(lambda row: partial_hmac(row["email"]), axis=1)

        # Get Mail API Credentials
        api_url = config["mailgun_api_url"]
        api_user = config["mailgun_user"]
        api_key = config["mailgun_key"]
        sender = config["mailgun_sender"]

        for index, row in filtered_event_managers.iterrows():
            # Prepare the data
            data = {
                "from": sender,
                "to": "<{}>".format(row["email"]),
                "subject": config["subject"],
                "text": config["text"].format(
                    event=row["event"],
                    password=row["password"]
                )
            }
            logging.info("API call params = {}".format(data))
            # Send the email
            response = send_mail(api_url, api_user, api_key, data)
            logging.info("API call response = {}".format(response.json()))

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


if __name__ == "__main__":
    main()
