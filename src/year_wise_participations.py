"""A python script to find number of participants from each academic for all events of a department"""

import os
import sys
import json
import zipfile

import pymysql
import logging
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

        event_type = sys.argv[1]
        department = sys.argv[2]
        logging.info("Event type = {} Department = {}".format(event_type, department))
        query = "SELECT name FROM events WHERE type = '{}' AND department = '{}'"
        logging.info(query)

        department_events = get_data(connection, query.format(event_type, department))

        for _, row in department_events.iterrows():
            logging.info(query)
            query = "SELECT year, COUNT(*) AS count FROM participations WHERE event = '{}' GROUP BY year"
            year_wise_participants = get_data(connection, query.format(row["name"]))
            year_wise_participants.fillna("0").to_csv("{}.csv".format(row["name"]), index=False)

        csv_files = list(filter(lambda x: ".csv" in x, os.listdir(".")))

        zip_name = "{}_{}.zip".format(event_type, department)

        with zipfile.ZipFile(zip_name, "w") as target_file:
            for file in csv_files:
                target_file.write(file)

        # Get Mail API Credentials
        api_url = config["mailgun_api_url"]
        api_user = config["mailgun_user"]
        api_key = config["mailgun_key"]

        # TODO - Need to check whether mail sends zip file or not
        data = {
            "from": config["mailgun_sender"],
            "to": "Sender Name <sender email address>",
            "subject": "Year wise participations",
            "text": "Year wise participations for each {event_type} event for {dept} Department".format(
                event_type=event_type,
                dept=department
            ),
            "files": [("attachment",  open(zip_name, "rb"))]
        }

        logging.info("API call params = {}".format(data))

        # Send the mail
        response = send_mail(api_url, api_user, api_key, data)

        logging.info("API call response = {}".format(json.dumps(response.json())))
        print(response.json())

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
