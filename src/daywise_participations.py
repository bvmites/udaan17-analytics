import pandas as pd
import pymysql

con = pymysql.connect(host="host", db="db", user="user", passwd="password")
cur = con.cursor()
cur.execute("SET sql_mode = ''")

# Get total number of participants in each event till date
cur.execute("SELECT event, count(*) as total FROM participations GROUP by event order by total desc")

df = pd.DataFrame(list(cur.fetchall()), columns=["Event name", "No. of entries"])
df.to_csv("total.csv")

# lower limit = Starting Id number
# upper limit = Last Id number
# event_type = Event types to exclude e.g. 'adventure'

df = pd.read_sql(
    "SELECT "
    "receipt_no, name_1, name_2, name_3, name_4, name_5, name_6, event, year, mobile "
    "FROM "
    "participations p "
    "JOIN "
    "events e "
    "ON p.event = e.name "
    "WHERE "
    "id >= {lower_limit} AND id <= {upper_limit} AND e.type != {event_type}"
    ,
    con
)

# Can mention date explicitly or from a config file or
# datetime.datetime.now() if script is executed on the same day
df.to_csv("Entries on {date}.csv")

cur.close()
con.close()
