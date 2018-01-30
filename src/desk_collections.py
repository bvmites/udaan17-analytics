"""A script to to find desk wise collections"""

import pymysql
import pandas as pd


def get_desk(row):
    receipt_no = row["receipt_no"]
    return receipt_no[0:receipt_no.rfind("/")]



def save_data(connection):
    select = "SELECT " \
             "p.id, p.receipt_no, p.event, e.fees " \
             "FROM " \
             "participations p " \
             "JOIN " \
             "events e " \
             "ON " \
             "p.event = e.name " \
             "WHERE " \
             "id >= {lower_limit_id} " \
             "AND " \
             "id <= {upper_limit_id}"

    df = pd.read_sql(select, connection)
    df["desk"] = df.apply(lambda row: get_desk(row), axis=1)
    df.to_csv("DD-MM-YYYY.csv", index=False, encoding="utf-8")


def get_desk_collections(file_path):
    df = pd.read_csv(file_path, encoding="iso-8859-1")
    desks = df.groupby(["desk"])
    desks["fees"].sum().to_csv("DD-MM-YYYY collections.csv")


def main():
    con = pymysql.connect(host="host", db="db", user="user", passwd="password")
    save_data(con)
    con.close()
    file_path = "DD-MM-YYYY.csv"
    get_desk_collections(file_path)


if __name__ == '__main__':
    main()
