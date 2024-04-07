from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import requests
import uvicorn
import os
from dotenv import load_dotenv
import psycopg2
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pytz
from pytz import utc, timezone

load_dotenv()

DB_URI = os.getenv('DATABASE_URL')

app = FastAPI()

@app.get('/')
def home():
    try:
        # Get the current date and time and replace year,month and date as 2023 January 25 respectively as data avl is from 18-25 JAN 2023
        date = datetime.now().replace(year=2023, month=1, day=25)

        # Get the date and time 1 hour before, 1 day before and 1 week before the current date and time
        hourbefore = date - relativedelta(hours=1)
        daybefore = date - relativedelta(days=1)
        weekbefore = date - relativedelta(weeks=1)

        # Get today's day of the week in integer format
        weekday = datetime.now().weekday()

        # Connect to the database
        conn = psycopg2.connect(DB_URI)
        # Create a cursor object
        cur = conn.cursor()
        cur.execute("SELECT * FROM timezones;")
        res1 = cur.fetchall()
        for row in res1:
            store_id = row[0]
            timezone_str = row[1]

            tz=timezone(timezone_str)

            # Your code here
            cur.execute("SELECT * FROM hours WHERE store_id={} AND day={} ;".format(store_id,weekday))
            res2 = cur.fetchall()
            for row2 in res2:
                open_time = row2[2]
                close_time = row2[3]
                
                # Create timezone aware datetime objects for the open and close times
                open_time_tz = tz.localize(datetime.combine(date.date(), open_time))
                close_time_tz = tz.localize(datetime.combine(date.date(), close_time))
                
                # Convert the open and close times to UTC
                open_time_utc = open_time_tz.astimezone(utc)
                close_time_utc = close_time_tz.astimezone(utc)

                # Your code here
                cur.execute("SELECT * FROM status WHERE store_id={} AND timestamp_utc>'{}' AND timestamp_utc<='{}';".format(store_id,hourbefore.isoformat(), date.isoformat()))
                res3 = cur.fetchall()
                
                res3 = [[item.isoformat() if isinstance(item, datetime) else item for item in row] for row in res3]
                data = {"store_id": store_id, "timezone": timezone_str, "open_time": open_time_utc.isoformat(), "close_time": close_time_utc.isoformat(), "data": res3}
                return JSONResponse(status_code=200,content=data)


        # cur.execute("SELECT * FROM hours WHERE day='{}';".format(weekday))
        # # Your code here
        # cur.execute("SELECT * FROM status WHERE timestamp_utc>'{}' AND timestamp_utc<='{}';".format(hourbefore.isoformat(), date.isoformat()))
        # res = cur.fetchall()
 
        # res = [[item.isoformat() if isinstance(item, datetime) else item for item in row] for row in res]

        # data = {"message": "Welcome to the summarization API","now":date.isoformat(), "data": res}
        # return JSONResponse(status_code=200,content=data)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get('/generate_report')
def report():
    try:

        # Connect to the database
        conn = psycopg2.connect(DB_URI)
        # Create a cursor object
        cur = conn.cursor() 


    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    pass

if __name__ == '__main__':
    uvicorn.run(app, port=8000, host='0.0.0.0')