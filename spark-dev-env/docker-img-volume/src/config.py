import os
import json
import pathlib as pl
from collections import namedtuple

root_dir = pl.Path('/home/jovyan') # docker's home folder (copied from repos relative path of /dev/docker-img)

# local paths
RAW_DATA_PATH = root_dir / 'data/raw'
TRUSTED_DATA_PATH = root_dir / 'data/trusted'
CREDENTIALS_PATH = root_dir / 'credentials' # all files within this folder are hidden for security purposes
NOTEBOOKS_PATH = root_dir / 'notebooks'

# remote paths
S3_SOURCE_BUCKET_NAME = 'ifood-data-architect-test-source'

# AWS secrets
AWS_SECRETS_PATH = CREDENTIALS_PATH / '.aws/secrets.json'
with open(AWS_SECRETS_PATH) as fp:
    aws_secrets = json.load(fp)