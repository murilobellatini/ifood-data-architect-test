# iFood Data Architect Test

This is my solution for this [test](https://github.com/ifood/ifood-data-architect-test) 

> Work in progress! Solution not ready yet...

## How to Run

### Requirements

* `docker >= 19.03.9`
* `docker-compose >= 1.25.0`

### Step-by-step

* Place your AWS Credentials into `/spark-dev-env/docker-img-volume/.aws/secrets.json` according the following format:

```json
{
    "PUBLIC_KEY": "<PUBLIC_KEY>",
    "SECRET_KEY": "<SECRET_KEY>"
}
```

* Run `docker-compose` at root directory

```bash
docker-compose up --build
```

* Run [notebooks](./spark-dev-env/docker-img-volume/notebooks) as you wish.

## Test Scope

Please find the code challenge [here](./TestScope.md).

## References

* [Medium Article | Running PySpark on Jupyter Notebook with Docker](https://medium.com/@suci/running-pyspark-on-jupyter-notebook-with-docker-602b18ac4494)
* [Reading Files from S3 Bucket to PySpark Dataframe Boto3](https://stackoverflow.com/questions/50570381/reading-files-from-s3-bucket-to-pyspark-dataframe-boto3)
