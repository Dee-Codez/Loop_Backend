from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse, FileResponse
import uuid,uvicorn,boto3,os,redis
from calcUtils import calculations
from dotenv import load_dotenv

app = FastAPI()

load_dotenv()
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS')
AWS_SECRET_KEY = os.getenv('AWS_SECRET')
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD')

# Redis dictionary to store the status of each report
r = redis.Redis(host=REDIS_HOST,port=16139,password=REDIS_PASSWORD)

def calculations_wrapper(report_id):
    try:
        # Call the calculations function
        calculations(report_id)

        # Update the status of the report in Redis
        r.set(str(report_id), 'Completed')
    except Exception as e:
        r.set(str(report_id), f'Error: {str(e)}')

@app.get('/')
def home():
    resp ={"message":"Welcome to the Loop Report Generation API. To trigger the report generation, use the /trigger_reportgen endpoint.",
           "endpoints":{"/trigger_reportgen":"Triggers the report generation process and returns the report ID.",
                        "/status/{report_id}":"Returns the status of the report with the given report ID. If the report is completed, the report file will be downloaded."},
           "example":"https://loop-api-gqet.onrender.com/trigger_reportgen",            
           }
    return JSONResponse(status_code=200,content=resp)
    

@app.get('/trigger_reportgen')
def reportgen(background_tasks: BackgroundTasks):
    try:
        # Generate a unique report ID
        report_id = uuid.uuid4()

        # Start the calculations for the report in the background
        background_tasks.add_task(calculations_wrapper, report_id)

        # Update the status of the report in Redis
        r.set(str(report_id), 'In progress')

        # Return the report ID
        output = {"message":"Report Generation has been triggered. Use the /status/{report_id} endpoint to check the status of the report.",
                  "report_id":str(report_id), 
                  "file_name":f'output_{report_id}.csv',
                  "details":"The report is being generated in the background. To conserve compute power(Limited Resources On Free Instance) the number of stores to be reported have been limited to 5. The report will be available at the get_report endpoint shortly.Get Report Status at the URL below",
                  "status_url":f"https://loop-api-gqet.onrender.com/status/{report_id}",
                  }
        
        return JSONResponse(status_code=200,content=output)
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get('/status/{report_id}')
def get_return(report_id: str):

    # Initialize the S3 client
    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY, region_name='ap-southeast-2')

    # Return the status of the report
    status = r.get(report_id)

    if status is None:
        return JSONResponse(status_code=404, content={"status": status, "message":"Report ID not found. Please check the report ID and try again."})
    else:
        status = status.decode('utf-8')
        if status == 'In progress':
            return JSONResponse(status_code=200, content={"status": status+"...",
                                                      "message":"Please wait for the report to be generated. On Completion, You will get the download url for the file."})
        else:
            file_name = f'output_{report_id}.csv'

            # Download the file from S3
            s3.download_file('testbucket-debam', file_name, file_name)

            with open(file_name, 'r') as file:
                file_contents = file.read()
            file_contents = file_contents.split('\n')
            output = {"status": status,
                    "message":"The report has been generated successfully. Use the download_url to download the report file.",
                    "download_url": f"https://loop-api-gqet.onrender.com/download/{report_id}",
                    "File Content": file_contents,
                    }
            return JSONResponse(status_code=200, content=output)

@app.get('/download/{report_id}')
def download_report(report_id: str):
    file_name = f'output_{report_id}.csv'
    return FileResponse(file_name, media_type='text/csv', headers={"Content-Disposition": f"attachment; filename={file_name}"})

if __name__ == '__main__':
    uvicorn.run(app, port=8000, host='0.0.0.0')