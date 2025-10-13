
from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Dict, Optional, Literal
from uuid import uuid4
from datetime import datetime
from typing import Any
import duckdb
import os

# In-memory stores for an MVP. Swap for a real DB (Postgres + SQLAlchemy) later.
CONNECTORS: Dict[str, dict] = {}
DESTINATIONS: Dict[str, dict] = {}
PIPELINES: Dict[str, dict] = {}
RUNS: Dict[str, dict] = {}

app = FastAPI(title="dlt GUI API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class ConnectorCreate(BaseModel):
    type: Literal["rest_generic", "postgres"] = "rest_generic"
    display_name: str
    config: Dict = Field(default_factory=dict)

class DestinationCreate(BaseModel):
    type: Literal["duckdb", "postgres", "snowflake", "bigquery"] = "duckdb"
    display_name: str
    config: Dict = Field(default_factory=dict)

class PipelineCreate(BaseModel):
    name: str
    connector_id: str
    destination_id: str
    schedule_cron: Optional[str] = None  # ignored in MVP
    config: Dict = Field(default_factory=dict)

@app.get("/health")
def health():
    return {"ok": True, "time": datetime.utcnow().isoformat() + "Z"}

@app.post("/connectors")
def create_connector(body: ConnectorCreate):
    cid = str(uuid4())
    CONNECTORS[cid] = {"id": cid, **body.dict(), "created_at": datetime.utcnow().isoformat() + "Z"}
    return CONNECTORS[cid]

@app.get("/connectors")
def list_connectors():
    return list(CONNECTORS.values())

@app.post("/destinations")
def create_destination(body: DestinationCreate):
    did = str(uuid4())
    DESTINATIONS[did] = {"id": did, **body.dict(), "created_at": datetime.utcnow().isoformat() + "Z"}
    return DESTINATIONS[did]

@app.get("/destinations")
def list_destinations():
    return list(DESTINATIONS.values())

@app.post("/pipelines")
def create_pipeline(body: PipelineCreate):
    if body.connector_id not in CONNECTORS:
        raise HTTPException(400, "connector_id not found")
    if body.destination_id not in DESTINATIONS:
        raise HTTPException(400, "destination_id not found")
    pid = str(uuid4())
    PIPELINES[pid] = {"id": pid, **body.dict(), "created_at": datetime.utcnow().isoformat() + "Z", "status": "idle"}
    return PIPELINES[pid]

@app.get("/pipelines")
def list_pipelines():
    return list(PIPELINES.values())

class RunResponse(BaseModel):
    run_id: str
    status: str

def run_dlt_job(pipeline_id: str):
    """Background job that executes a simple dlt pipeline."""
    from pipelines.run_rest_pipeline import run_rest_to_destination
    run_id = str(uuid4())
    RUNS[run_id] = {
        "id": run_id,
        "pipeline_id": pipeline_id,
        "status": "running",
        "started_at": datetime.utcnow().isoformat() + "Z",
        "ended_at": None,
        "rows_loaded": 0,
        "error_text": None,
        "logs_url": None,
    }
    try:
        pipeline = PIPELINES[pipeline_id]
        connector = CONNECTORS[pipeline["connector_id"]]
        destination = DESTINATIONS[pipeline["destination_id"]]
        result = run_rest_to_destination(
            pipeline_name=pipeline["name"],
            connector_config=connector["config"],
            destination_type=destination["type"],
            destination_config=destination["config"]
        )
        RUNS[run_id]["status"] = "succeeded"
        RUNS[run_id]["rows_loaded"] = result.get("rows_loaded", 0)
        RUNS[run_id]["schemas"] = result.get("schemas", [])
    except Exception as e:
        RUNS[run_id]["status"] = "failed"
        RUNS[run_id]["error_text"] = str(e)
    finally:
        RUNS[run_id]["ended_at"] = datetime.utcnow().isoformat() + "Z"

from fastapi import APIRouter
@app.post("/pipelines/{pipeline_id}/run", response_model=RunResponse)
def enqueue_run(pipeline_id: str, bg: BackgroundTasks):
    if pipeline_id not in PIPELINES:
        raise HTTPException(404, "pipeline not found")
    bg.add_task(run_dlt_job, pipeline_id)
    run_id = str(uuid4())
    RUNS[run_id] = {
        "id": run_id,
        "pipeline_id": pipeline_id,
        "status": "queued",
        "started_at": None,
        "ended_at": None,
    }
    return {"run_id": run_id, "status": "queued"}

@app.get("/runs")
def list_runs():
    return list(RUNS.values())

@app.get("/runs/{run_id}")
def get_run(run_id: str):
    if run_id not in RUNS:
        raise HTTPException(404, "run not found")
    return RUNS[run_id]

# --- Data Preview (read-only) ---
from pydantic import BaseModel

class PreviewResponse(BaseModel):
    columns: list[str]
    rows: list[list[Any]]

@app.get("/data/preview", response_model=PreviewResponse)
def data_preview(pipeline_id: str, table: str = "products", limit: int = 50):
    """Read-only preview of a table loaded by a pipeline into DuckDB."""
    if pipeline_id not in PIPELINES:
        raise HTTPException(404, "pipeline not found")
    pipeline = PIPELINES[pipeline_id]
    pipeline_name = pipeline["name"]

    # Cap limit for safety
    limit = max(1, min(int(limit), 500))

    # Our runner writes the DuckDB file here:
    db_path = f"/app/data/{pipeline_name}.duckdb"
    if not os.path.exists(db_path):
        raise HTTPException(404, f"DuckDB file not found for pipeline: {db_path}")

    # **Read-only**: SELECT only, no user-provided SQL
    try:
        con = duckdb.connect(db_path, read_only=True)
        # Use our known dataset/schema name "dummyjson" unless caller already schema-qualifies
        dataset = "dummyjson"
        qtable = table if "." in table else f'{dataset}."{table}"'
        q = f"SELECT * FROM {qtable} LIMIT {limit}"
        res = con.execute(q)
        cols = [d[0] for d in res.description]
        rows = res.fetchall()
        return {"columns": cols, "rows": rows}
    except duckdb.Error as e:
        raise HTTPException(400, f"DuckDB error: {e}")
    finally:
        try:
            con.close()
        except Exception:
            pass