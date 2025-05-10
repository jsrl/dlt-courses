import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources
from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator
import os

# Extraction. Number of parallel threads with parrallelized = True
os.environ['EXTRACT__WORKERS'] = '3'
# Extraction. Enable file rotation by setting a file size limit. By default all records to one file.
os.environ["EXTRACT__DATA_WRITER__FILE_MAX_ITEMS"] = "50000"
# Extraction. Control buffer size. By default 5000
os.environ['DATA_WRITER__BUFFER_MAX_ITEMS'] = '10000'
# Normalization. Enable parallelism: Use multiple processes
os.environ['NORMALIZE__WORKERS'] = '3'
# Loading. Control the number of threads
os.environ["LOAD__WORKERS"] = "3"



@dlt.source
def jaffle_source():
  config: RESTAPIConfig = {
        "client": {
            "base_url": "https://jaffle-shop.scalevector.ai/api/v1",
            "paginator": {
            "type": "header_link",
            },
        },
        "resource_defaults": {
            "write_disposition": "replace",
            "parallelized": True,
            "endpoint": {
                "params": {
                    "page_size": 2000,
                }
            },
        },
        "resources": [
            {
                "name": "orders",
                "endpoint": {
                    "path": "orders"
                },
                "primary_key": "id"
            },
            {
                "name": "customers",
                "endpoint": {
                    "path": "customers",
                },
                "primary_key": "id"
            },
            {
                "name": "products",
                "endpoint": {
                    "path": "products"
                },
                "primary_key": "sku",
            }
        ]
  }
  yield from rest_api_resources(config)

pipeline = dlt.pipeline(
    pipeline_name="extract_jaffle_example1",
    destination="duckdb",
    dataset_name="mydata",
    dev_mode=True,
)

load_info = pipeline.run(jaffle_source())
print(pipeline.last_trace)
print(pipeline.dataset().row_counts().df())
# print(load_info)
# pipeline.dataset().orders.df()
# print(pipeline.dataset().row_counts().fetchall())