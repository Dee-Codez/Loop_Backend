import os,psycopg2,pytz,csv,boto3,io,botocore
from dotenv import load_dotenv
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pytz import utc, timezone

load_dotenv()
DB_URI = os.getenv('DATABASE_URL')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS')
AWS_SECRET_KEY = os.getenv('AWS_SECRET')

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

    # Get the date of next day to filter the data for the whole current day
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

    # Get the date and time 1 hour before and a day before the current date and time
    hourbefore = current_time - relativedelta(hours=1)
    daybefore = current_time - relativedelta(days=1)

    # Connect to the database
    conn = psycopg2.connect(DB_URI,sslmode='verify-ca', sslrootcert='root.crt')
    # Create a cursor object
    cur = conn.cursor()
    # Get the store IDs and timezones of only 5 stores from the timezones table
    cur.execute(f"SELECT * FROM timezones LIMIT {5}; ")
    k=0
    res1 = cur.fetchall()
    for row in res1:
        k=k+1
        store_id = row[0]
        timezone_str = row[1]
        print(store_id,timezone_str)

        # Get the open and close times of the store for the current day
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
        
        #Extrapolating Uptime and Downtime for the last hour
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

        #Extrapolating Uptime and Downtime for the whole day before
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


        #Extrapolating Uptime and Downtime for the whole week
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

        data = {"store_id": store_id,"hour_uptime":hour_uptime,"hour_downtime":hour_downtime ,"day_uptime":day_uptime,"day_downtime":day_downtime ,"week_uptime":weekres_uptime,"week_downtime":weekres_downtime}

        # Initialize the S3 client
        s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY, region_name='ap-southeast-2')

        # Check if the file exists in S3
        try:
            s3.head_object(Bucket='testbucket-debam', Key=filename)
            file_exists = True
        except botocore.exceptions.ClientError as e:
            # If not, raise an error and falsify flag
            if e.response['Error']['Code'] == '404':
                file_exists = False
            else:
                # Something else has gone wrong.
                raise

        # Create a StringIO object
        csvio = io.StringIO()

        # If the file exists, download it and load its content into the StringIO object
        if file_exists:
            obj = s3.get_object(Bucket='testbucket-debam', Key=filename)
            csvio.write(obj['Body'].read().decode('utf-8'))

        # Write the CSV data to the StringIO object
        fieldnames = data.keys()
        writer = csv.DictWriter(csvio, fieldnames=fieldnames)

        # Write the headers if the file doesn't exist
        if not file_exists:
            writer.writeheader()

        # Write the data
        writer.writerow(data)

        # Get the CSV data from the StringIO object
        csv_data = csvio.getvalue()

        # Upload the CSV data to S3
        s3.put_object(Body=csv_data, Bucket='testbucket-debam', Key=filename)
        
        print(f"Report for store {k},i.e. {store_id} has been generated successfully.")
        fin_data = {"store_id": store_id, "timezone": timezone_str, "open_time": open_times_str, "close_time": close_times_str,"hour_uptime":hour_uptime,"hour_downtime":hour_downtime ,"day_uptime":day_uptime,"day_downtime":day_downtime ,"week_uptime":weekres_uptime,"week_downtime":weekres_downtime ,"hr_data": hourres, "day_data": dayres,"week_data":weekres}

    # Close the cursor and connection
    cur.close()
    conn.close()
    complete_flag = True
    return complete_flag
