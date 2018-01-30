"""A python script to generate attendance sheets for """

import os
import json
import logging

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


def get_combined_names(row):
    return "{}\n{}\n{}\n{}\n{}\n{}".format(
        row["name_1"],
        row["name_2"],
        row["name_3"],
        row["name_4"],
        row["name_5"],
        row["name_6"]
    ).strip()


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

        # Attendance sheets are not required for adventure events
        query = "SELECT name FROM events WHERE type != 'adventure'"
        logging.info(query)

        events = get_data(connection, query)

        participants_query = "SELECT receipt_no, name_1, name_2, name_3, name_4, name_5, name_6, mobile " \
                             "FROM participations WHERE event = \"{}\""

        for index, row in events.iterrows():
            participants = get_data(connection, participants_query.format(row["name"]))
            participants = participants.fillna("")
            participants["name"] = participants.apply(lambda row: get_combined_names(row), axis=1)
            # TODO - modify index parameter for correct output
            # Excel file should not skip any participants
            pd.DataFrame(
                participants[["receipt_no", "name", "mobile"]],
                index=list(range(1, len(participants)))
            ).to_excel("{}.xlsx".format(row["name"]), row["name"], index_label="Sr. No.")

        print("Done")

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
