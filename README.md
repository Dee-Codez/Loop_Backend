
# Loop_Backend

A Simple Backend API For Testing Purposes

### What is it?

In essence, it's an amalgamation of databases coming together to handle big data and thus create an interface for programmers to interact with data.

In layman terms, it is an API(Application Programming Interface) enabling report generation from numerous large sets of data.



## Deployment

Deployed At : https://loop-api200-7278760e7ad4.herokuapp.com


## Demo

Code Functionality And Explaination Videos :

https://drive.google.com/drive/folders/1aZMX8hF7tgLllcL0JLAp4PXcWYyyqcb8?usp=sharing
## Tech Stack

**Non Persisent Storage** : RedisDB

**Local Storage** : AWS S3

**Cloud Storage** : CockroachDB(Serverless Postgresql)

**Route Handling** : FastAPI


## API Reference


#### Landing Page

```http
  GET /
```
Gives welcome message with endpoints of how to navigate through

#### Trigger Report Generation

```http
  GET /trigger_reportgen
```
Returns a report_id to track and fetch the report further on

| Output | Type     | Description                |
| :-------- | :------- | :------------------------- |
| `report_id` | `string` |  Randomly generated UUID       |


#### Get Status Of Report

```http
  GET /status/${report_id}
```

| Parameter | Type     | Description                       |
| :-------- | :------- | :-------------------------------- |
| `report_id`      | `string` | **Required**. ID of report to fetch |

| Output | Type     | Description                |
| :-------- | :------- | :------------------------- |
| `status` | `string` |  Tells if report generation is in progress or completed|

#### Download Report

```http
  GET /download/${report_id}
```
| Parameter | Type     | Description                       |
| :-------- | :------- | :-------------------------------- |
| `report_id`      | `string` | **Required**. ID of report to download |

| Output | Type     | Description                |
| :-------- | :------- | :------------------------- |
| `download_url` | `string` |  Gives a presigned url from S3 to download report|

Automatically redirects to download_url and thus downloads the report
