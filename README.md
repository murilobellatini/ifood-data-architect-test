# iFood Data Architect Test

This is my solution for the [iFood Test](https://github.com/ifood/ifood-data-architect-test), whose proposal is to create a prototype datalake from `json` and `csv` files. Raw and trusted layers where required. My solution runs locally inside a docker container with `Pyspark` and all further necessary dependencies. It gets the data ingested, wrangled, processed and finally exported as `parquet` files. Partitioning was done based on test requirements.

The solution is split into two parts:

1. `Development`: Jupyter Notebooks with a development walkthrough can be found [here](./dev/docker-volume/notebooks).
2. `Final script`: `main.py` can be found here [here](./dev/docker-volume/main.py)

The raw data had some duplicated values which I've decided to drop after having a look into some of them and understanding it was safe to do so. Data validation was accomplished by casting data types after manually understanding each present column. For cases when I was unsure, the data was left as string in order to avoid possible crashes. Regarding anonymization I've just dropped all sensitive data columns since their owners, customers and merchants, can be identified via their unique ids anyways.

The complete solution was run on my local laptop, that's why the Spark session has modest configurations. But once the final application script gets deployed to a proper development environment, such as suggested Databricks, it should scale accordingly.

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
2. `Development`: Run [notebooks](./dev/docker-volume/notebooks).

## Test Scope

Please find the code challenge [here](./TestScope.md).

## References

* [Medium Article | Running PySpark on Jupyter Notebook with Docker](https://medium.com/@suci/running-pyspark-on-jupyter-notebook-with-docker-602b18ac4494)
* [How to read JSON files from S3 using PySpark and the Jupyter notebook](https://medium.com/@bogdan.cojocar/how-to-read-json-files-from-s3-using-pyspark-and-the-jupyter-notebook-275dcb27e124)
* [Pyspark create dictionary within groupby](https://stackoverflow.com/questions/55308482/pyspark-create-dictionary-within-groupby)
* [Pyspark export partioned data](https://stackoverflow.com/questions/56748522/in-pyspark-how-to-partitionby-parts-of-the-value-of-a-certain-column-when-using)


## Folder Organization

    ├── README.md                   <- The top-level README.
    │
    ├── media                       <- Folder for storing media such as images.
    │
    ├── dev                         <- Development folder where  application scripts are stored.
    │   ├── docker-volume           <- Shared volume with docker container.
    │   │   ├── credentials         <- Folder for storing credentials, only one required so far is AWS account.
    │   │   ├── data                <- Data for file system, here is here raw and trusted layers shall reside.
    │   │   ├── notebooks           <- Development notebooks to have a walkthrough of development phase.
    │   │   ├── src                 <- Custom library for storing required code.
    │   │   └── main.py             <- Main application to run full pipeline from single script.
    │   │
    │   ├── Dockerfile              <- Defines docker container and installs dependencies.
    │   ├── requirements.txt        <- Stores Python required libraries.
    │   └── setup.py                <- Installs `src` custom library
    │
    ├── .gitignore                  <- Used for ignoring uploading unecessary data to repo.
    │
    ├── docker-compose.yml          <- Docker-compose file for runing docker containers with environemnt specs. 
    │
    └── TestScope.md                <- Clone of original test scope in case the original repo gets deleted.
