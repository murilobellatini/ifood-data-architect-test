# iFood Data Architect Test

This is my solution for this [test](https://github.com/ifood/ifood-data-architect-test) 

> Work in progress! Solution not ready yet...

## How to Run

### Requirements

* `docker >= 19.03.9`
* `docker-compose >= 1.25.0`

### Step-by-step

* Place your AWS Credentials into `/dev/docker-volume/.aws/secrets.json` according the following format:

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

* Access `http://localhost:8888` at your browser

* Then decide to run production script or developement notebooks:

1. `Production`: Open terminal window within Jupyter Notebook and run `python main.py`.
2. `Development`: Run [notebooks](./spark-dev-env/docker-img-volume/notebooks).

## Test Scope

Please find the code challenge [here](./TestScope.md).

## References

* [Medium Article | Running PySpark on Jupyter Notebook with Docker](https://medium.com/@suci/running-pyspark-on-jupyter-notebook-with-docker-602b18ac4494)
* [How to read JSON files from S3 using PySpark and the Jupyter notebook](https://medium.com/@bogdan.cojocar/how-to-read-json-files-from-s3-using-pyspark-and-the-jupyter-notebook-275dcb27e124)
