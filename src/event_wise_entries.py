import json
import pymysql
import datetime
import requests
import pandas as pd


CONFIG_FILE = "config.json"
CSV_FILE = "Event wise entries upto {}.csv"


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
        print(file_err)
    except json.decoder.JSONDecodeError as json_err:
        print(json_err)
    except Exception as ex:
        print(type(ex))
        print(ex)


def disable_group_by(connection):
    """
        A function to disable only_full_group_by mode in MySQL

        Parameters
        ----------

        connection: pymysql.connections.Connection
            Connection object of the MySQL database connection.
    """

    try:
        cursor = connection.cursor()
        cursor.execute("SET SQL_MODE = ''")
        cursor.close()
    except Exception as ex:
        print(type(ex))
        print(ex)


def save_csv(connection, query, columns, name):
    """
        A function to save CSV file created from data
        fetched from a MySQL database.

        Parameters
        ----------

        connection: pymysql.connections.Connection
            Connection object of the MySQL database connection.

        query: str
            The query to perform on the database.

        columns: list
            List of column names.

        name: str
            Name of the target CSV file.
    """

    try:
        df = pd.read_sql(query, connection, columns=columns)
        df.to_csv(name, index=False)
    except Exception as ex:
        print(type(ex))
        print(ex)


def send_mail(api_url, user, key, sender, receiver, subject, text, file_name):
    """
        A to send Email with an attachment using
        Mailgun API.

        Parameters
        ----------

        api_url: str
            Mailgun API URL.

        user: str
            Name of Mailgun user.

        key: str
            Mailgun API key.

        sender: str
            Sender's name and email address in `Name <email>` format.

        receiver: str
            Receiver's name and email address in `Name <email>` format.

        subject: str
            Subject of the email.

        text: str
            Content of the email.

        file_name: str
            Name of the attachment file.
    """

    authorization = (user, key)
    data = {
        "from": sender,
        "to": receiver,
        "subject": subject,
        "text": text
    }
    try:
        return requests.post(api_url,
                             auth=authorization,
                             files=[("attachment", (file_name, open(file_name, "rb").read()))],
                             data=data
                             )
    except Exception as ex:
        print(type(ex))
        print(ex)


def main():
    # TODO - Add Logging facility
    try:
        config = get_config(CONFIG_FILE)

        host = config["mysql_host"]
        user = config["mysql_user"]
        password = config["mysql_pass"]
        database = config["mysql_db"]

        con = pymysql.connect(host=host, user=user, passwd=password, db=database)

        disable_group_by(con)
        query = "SELECT event, COUNT(*) AS `entries` FROM {}.participations WHERE id <= {last_id} " \
                "GROUP BY event ORDER BY`entries` DESC;".format(database)
        columns = ["Event Name", "No. of entries"]

        current_date = str(datetime.date.today()).split(".")[0]
        name = CSV_FILE.format(current_date)
        save_csv(con, query, columns, name)

        url = config["mailgun_api"]
        user = config["mailgun_user"]
        key = config["mailgun_key"]
        sender = config["mailgun_sender"]
        receiver = config["receiver"]
        subject = "Total entries up to {}".format(current_date)
        text = config["text"]

        response = send_mail(url, user, key, sender, receiver, subject, text, name)
        print(response.status_code)
        print(response.json())

        con.close()

    except Exception as ex:
        print(type(ex))
        print(ex)


if __name__ == '__main__':
    main()
