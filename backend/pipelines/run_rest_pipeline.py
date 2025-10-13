# replace the whole file with this

from typing import Dict, Any, Iterable, Optional
import os
import requests
import dlt
from dlt.destinations import duckdb as duckdb_dest

def _iter_dummyjson_products(
    base_url: str,
    endpoint: str,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    page_limit: int = 100
) -> Iterable[dict]:
    """
    Pages DummyJSON-style endpoints:
      response = { "products": [...], "total": N, "skip": X, "limit": L }
    """
    headers = headers or {}
    params = dict(params or {})
    # default server page size is 30; we can request a higher limit (<=100)
    params.setdefault("limit", min(page_limit, 100))
    skip = int(params.get("skip", 0))
    total = None

    while True:
        params["skip"] = skip
        url = base_url.rstrip("/") + "/" + endpoint.lstrip("/")
        r = requests.get(url, headers=headers, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()

        # products array is required for this demo
        items = data.get("products", [])
        if items:
            yield items

        # update paging based on response
        resp_limit = int(data.get("limit", params["limit"]))
        resp_total = int(data.get("total", 0))
        total = resp_total if total is None else total
        skip += resp_limit

        if skip >= resp_total or resp_limit <= 0:
            break

@dlt.resource(name="products", write_disposition="merge", primary_key="id")
def products_resource(
    base_url: str,
    endpoint: str,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
):
    for page in _iter_dummyjson_products(base_url, endpoint, headers, params):
        # dlt will normalize nested objects/arrays (dimensions, reviews, etc.)
        yield page

@dlt.source
def products_source(
    base_url: str,
    endpoint: str,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
):
    return products_resource(base_url, endpoint, headers, params)

def run_rest_to_destination(
    pipeline_name: str,
    connector_config: Dict[str, Any],
    destination_type: str = "duckdb",
    destination_config: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    destination_config = destination_config or {}

    base_url = connector_config.get("base_url", "https://dummyjson.com")
    endpoint = connector_config.get("endpoint", "/products")
    headers = connector_config.get("headers")
    params = connector_config.get("params")

    src = products_source(base_url, endpoint, headers, params)

    if destination_type == "duckdb":
        os.makedirs("/app/data", exist_ok=True)
        db_path = destination_config.get("database", f"/app/data/{pipeline_name}.duckdb")
        dest = duckdb_dest(credentials={"database": db_path})
        pipeline = dlt.pipeline(pipeline_name=pipeline_name, destination=dest, dataset_name="dummyjson")
    elif destination_type == "postgres":
        pipeline = dlt.pipeline(pipeline_name=pipeline_name, destination="postgres", dataset_name="dummyjson")
    elif destination_type == "snowflake":
        pipeline = dlt.pipeline(pipeline_name=pipeline_name, destination="snowflake", dataset_name="dummyjson")
    elif destination_type == "bigquery":
        pipeline = dlt.pipeline(pipeline_name=pipeline_name, destination="bigquery", dataset_name="dummyjson")
    else:
        raise ValueError(f"Unsupported destination: {destination_type}")

    load_info = pipeline.run(src)

    # robust row counting across dlt versions
    rows = 0
    try:
        # v1.x exposes load packages with 'loads_ids' mapping
        rows = sum((lp.row_count or 0) for lp in getattr(load_info, "loads_ids", {}).values())
    except Exception:
        pass

    return {
        "rows_loaded": rows,
        "schemas": list(pipeline.default_schema.tables.keys()),  # should include 'products'
    }
