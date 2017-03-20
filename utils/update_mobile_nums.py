"""A python script to update mobile numbers for specified participants"""

import json
import logging

import os
import pymysql
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

        # The SQL to be update mobile number for specified event
        # and name of the participant
        query = "A VALID SQL QUERY"
        logging.info("{}".format(str(query)))

        cursor = connection.cursor()

        # Read a CSV file containing names and mobile
        # numbers of participants
        df = pd.read_csv("filename.csv")

        for _, row in df.iterrows():
            logging.info(query.format(mobile=row["Mobile"], name=row["Name"]))
            status = cursor.execute(query.format(mobile=row["Mobile"], name=row["Name"]))
            logging.info(status)

        cursor.close()
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