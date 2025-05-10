import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources
from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator


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
            "primary_key": "id"
        },
        "resources": [
            {
                "name": "orders",
                "endpoint": {
                    "path": "orders"
                }                
            },
            {
                "name": "customers",
                "endpoint": {
                    "path": "customers",
                }
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
    #dev_mode=True,
)

load_info = pipeline.run(jaffle_source())
print(pipeline.last_trace)
print(pipeline.dataset().row_counts().df())
# print(load_info)
# pipeline.dataset().orders.df()
# print(pipeline.dataset().row_counts().fetchall())