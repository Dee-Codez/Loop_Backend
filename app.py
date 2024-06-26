from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse, RedirectResponse, FileResponse
import uuid,uvicorn,boto3,os,redis,botocore
from calcUtils import calculations
from dotenv import load_dotenv

app = FastAPI()

load_dotenv()
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS')
AWS_SECRET_KEY = os.getenv('AWS_SECRET')
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD')
PORT = os.getenv('PORT', 8000)

# Redis dictionary to store the status of each report
r = redis.Redis(host=REDIS_HOST,port=16139,password=REDIS_PASSWORD)

# Initialize the S3 client
s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY, region_name='ap-southeast-2')

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
           "example":"https://loop-api200-7278760e7ad4.herokuapp.com/trigger_reportgen",            
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
                  "status_url":f"https://loop-api200-7278760e7ad4.herokuapp.com/status/{report_id}",
                  }
        
        return JSONResponse(status_code=200,content=output)
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get('/status/{report_id}')
def get_return(report_id: str):

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

            # Check if the file exists in S3
            try:
                response = s3.head_object(Bucket='testbucket-debam', Key=file_name)
            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == '404':
                    return JSONResponse(status_code=404, content={"status": status, "message":"File not found in S3. Please check the file name and try again."})
                else:
                    # Something else has gone wrong.
                    raise

            # Get the file from S3
            obj = s3.get_object(Bucket='testbucket-debam', Key=file_name)

            # Get the last modified time
            last_modified = response['LastModified']

            # Read the file content
            file_content = obj['Body'].read().decode('utf-8')
            file_contents = file_content.split('\n')

            output = {"status": status,
                      "report_id": report_id,
                      "report_time": last_modified.strftime('%Y-%m-%d %H:%M:%S'),
                    "message":"The report has been generated successfully. Use the download_url or hit the /download/{report_id} endpoint to download the report file.",
                    "download_url": f"https://loop-api200-7278760e7ad4.herokuapp.com/download/{report_id}",
                    "Note":"The report is generated with a limit of 5 stores to conserve resources. The full report can be generated by changing the limit in the code.",
                    "File Content": file_contents,
                    }
            return JSONResponse(status_code=200, content=output)

@app.get('/download/{report_id}')
def download_report(report_id: str):
    file_name = f'output_{report_id}.csv'
    # Generate a presigned URL for the S3 object
    url = s3.generate_presigned_url(
        'get_object',
        Params={'Bucket': 'testbucket-debam', 'Key': file_name},
        ExpiresIn=3600)
    # Redirect to the presigned URL
    return RedirectResponse(url)

@app.get('/favicon.ico', include_in_schema=False)
async def favicon():
    return FileResponse('favicon.ico')

# if __name__ == '__main__':
#     uvicorn.run(app, port=PORT, host='0.0.0.0')