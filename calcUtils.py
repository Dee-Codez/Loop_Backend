import os
from dotenv import load_dotenv
import psycopg2
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pytz
from pytz import utc, timezone
import csv

load_dotenv()
DB_URI = os.getenv('DATABASE_URL')

#Getting the store opening and closing timings for a specific day
def storeStatus(cursor,store_id,timezone_str,current_time):

    cursor.execute("SELECT * FROM hours WHERE store_id = {} AND day = {} ;".format(store_id,current_time.weekday()))
    res = cursor.fetchall()

    open_times_utc = []
    close_times_utc = []

    # If no data is available for the store, consider it open for the whole day
    if len(res) == 0:
        open_times_utc.append(current_time.replace(hour=0, minute=0, second=0).isoformat())
        close_times_utc.append(current_time.replace(hour=23, minute=59, second=59).isoformat())
        return open_times_utc,close_times_utc
    
    # Iterate through the different timings and get the open and close times array
    for row in res:
        open_time = row[2]
        close_time = row[3]

        # Create timezone aware datetime objects for the open and close times
        tz = timezone(timezone_str)
        open_time_tz = tz.localize(datetime.combine(current_time.date(), open_time))
        close_time_tz = tz.localize(datetime.combine(current_time.date(), close_time))

        # Convert the open and close times to UTC
        open_time_utctime = open_time_tz.astimezone(utc)
        close_time_utctime = close_time_tz.astimezone(utc)

        # # Remove the date component from the open and close times
        # open_time_utctime = open_time_utctime.time()
        # close_time_utctime = close_time_utctime.time()

        # Add the open and close times to the lists
        open_times_utc.append(open_time_utctime.isoformat())
        close_times_utc.append(close_time_utctime.isoformat())
    
    return open_times_utc,close_times_utc

#Getting the activity status for a whole day as per it's open and close timings
def dayStatus(cursor,store_id,timezone_str,current_time):

    # Get the date of next day
    nxtday = current_time + relativedelta(days=1)

    # Get the open and close times for the day before
    daybfr_open_times_utc,daybfr_close_times_utc = storeStatus(cursor,store_id,timezone_str,current_time)

    # Get the data for the whole day before
    cursor.execute("SELECT * FROM status WHERE store_id={} AND timestamp_utc>'{}' AND timestamp_utc<='{}';".format(store_id,current_time.date().isoformat(), nxtday.date().isoformat()))
    dayres = cursor.fetchall()
    dayres = [[item.isoformat() if isinstance(item, datetime) else item for item in row] for row in dayres]

    # Filter dayres to keep only the data within the store's open and close timings on that day
    dayres_filtered = []
    for row in dayres:
        row_time = datetime.fromisoformat(row[2])
        for i in range(len(daybfr_open_times_utc)):
            if row_time.isoformat() >= daybfr_open_times_utc[i] and row_time.isoformat() <= daybfr_close_times_utc[i]:
                dayres_filtered.append(row)
    if len(dayres_filtered) > 0:
        dayres = dayres_filtered

    return dayres


def calculations(report_id: str):

    complete_flag = False

    # Get the current date and time in UTC  and replace year,month and date as 2023 January 25 respectively as data avl is from 18-25 JAN 2023
    current_time = datetime.now(pytz.UTC)
    current_time = current_time.replace(year=2023, month=1, day=25)

    # Get the date and time 1 hour before, 1 day before and 1 week before the current date and time
    hourbefore = current_time - relativedelta(hours=1)
    daybefore = current_time - relativedelta(days=1)
    weekbefore = current_time - relativedelta(weeks=1)

    # Get today's day of the week in integer format(UTC Timezone)
    weekday = datetime.now(pytz.UTC).weekday()

    # Connect to the database
    conn = psycopg2.connect(DB_URI)
    # Create a cursor object
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM timezones LIMIT {2}; ")
    k=0
    res1 = cur.fetchall()
    for row in res1:
        k=k+1
        store_id = row[0]
        timezone_str = row[1]
        print(store_id,timezone_str)

        sameday_open_times_utc,sameday_close_times_utc = storeStatus(cur,store_id,timezone_str,current_time)

        open_times_str = ','.join(sameday_open_times_utc)
        close_times_str = ','.join(sameday_close_times_utc)
        
        hourres = []

        #Checking if the current time is within the store's open hours
        for i in range(0, len(sameday_open_times_utc)):
            #If the current time is within the store's open hours, get the data for the last hour
            if current_time.time().isoformat() >= sameday_open_times_utc[i] and current_time.time().isoformat() <= sameday_close_times_utc[i]:
                cur.execute("SELECT * FROM status WHERE store_id={} AND timestamp_utc>'{}' AND timestamp_utc<='{}';".format(store_id,hourbefore.isoformat(), current_time.isoformat()))
                hourres = cur.fetchall()
                hourres = [[item.isoformat() if isinstance(item, datetime) else item for item in row] for row in hourres]
        
        #Interpolating Uptime and Downtime for the last hour
        if len(hourres) == 0:
            #If no data is available for the last hour, considering it active for the whole hour
            hour_uptime = 60
            hour_downtime = 0
        else:
            #If data is available for the last hour, calculating the uptime and downtime using ratio of active and inactive data
            hour_up = 0
            hour_down = 0
            for row in hourres:
                if row[1] == 'active':
                    hour_up += 1
                else:
                    hour_down += 1
            tot_hr = hour_up + hour_down
            hour_uptime = (hour_up/tot_hr)*60
            hour_downtime = (hour_down/tot_hr)*60
            
                
        #Starting computation for the day before
        dayres = dayStatus(cur,store_id,timezone_str,daybefore)

        #Interpolating Uptime and Downtime for the whole day before
        if(len(dayres) == 0):
            #If no data is available for the whole day before, considering it active for the whole day
            day_uptime = 24
            day_downtime = 0
        else:
            #If data is available for the whole day before, calculating the uptime and downtime using ratio of active and inactive data
            day_up = 0
            day_down = 0
            for row in dayres:
                if row[1] == 'active':
                    day_up += 1
                else:
                    day_down += 1
            tot_day = day_up + day_down
            day_uptime = (day_up/tot_day)*24
            day_downtime = (day_down/tot_day)*24

        

        #Calculating For Whole Week
        weekres = []
        for i in range(0,7):
            day = current_time - relativedelta(days=i)
            dayres = dayStatus(cur,store_id,timezone_str,day)
            weekres.append(dayres)


        #Interpolating Uptime and Downtime for the whole week
        weekres_up=[]
        weekres_down=[]
        for singleday in weekres:
            if len(singleday) == 0:
                #If no data is available for the whole day before, considering it active for the whole day
                weekres_up.append(24)
                weekres_down.append(0)
            else:
                #If data is available for the whole day before, calculating the uptime and downtime using ratio of active and inactive data
                day_up = 0
                day_down = 0
                for row in singleday:
                    if row[1] == 'active':
                        day_up += 1
                    else:
                        day_down += 1
                tot_day = day_up + day_down
                day_uptime = (day_up/tot_day)*24
                day_downtime = (day_down/tot_day)*24
                weekres_up.append(day_uptime)
                weekres_down.append(day_downtime)
        weekres_uptime = sum(weekres_up)
        weekres_downtime = sum(weekres_down)

        filename = f'output_{report_id}.csv'


        data = {"store_id": store_id, "timezone": timezone_str, "open_time": open_times_str, "close_time": close_times_str,"hour_uptime":hour_uptime,"hour_downtime":hour_downtime ,"day_uptime":day_uptime,"day_downtime":day_downtime ,"week_uptime":weekres_uptime,"week_downtime":weekres_downtime}

        # Check if file exists to write headers
        if not os.path.isfile(filename):
            with open(filename, 'w', newline='') as csvfile:
                fieldnames = data.keys()
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()

        # Append data to CSV file if it exists
        with open(filename, 'a', newline='') as csvfile:
            fieldnames = data.keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writerow(data)


        print(f"Report for store {k},i.e. {store_id} has been generated successfully.")
        fin_data = {"store_id": store_id, "timezone": timezone_str, "open_time": open_times_str, "close_time": close_times_str,"hour_uptime":hour_uptime,"hour_downtime":hour_downtime ,"day_uptime":day_uptime,"day_downtime":day_downtime ,"week_uptime":weekres_uptime,"week_downtime":weekres_downtime ,"hr_data": hourres, "day_data": dayres,"week_data":weekres}

    # Close the cursor and connection
    cur.close()
    conn.close()
    complete_flag = True
    return complete_flag
