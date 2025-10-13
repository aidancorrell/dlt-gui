
# dlt GUI Starter (FastAPI + React + dlt)

A minimal, runnable starter for a self-hosted web GUI on top of dlt.
It lets you create a demo **REST → DuckDB** pipeline and run it from the UI.

## Quickstart (Docker)

```bash
cd ops
docker compose up --build
```

- API: http://localhost:8000/docs
- Web: http://localhost:5173

In the web app, click **“Create demo (REST → DuckDB)”**, then click **Run**.

## Local dev

### Backend
```bash
cd backend
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

### Frontend
```bash
cd frontend
npm install
npm run dev
```

## Next steps
- Replace in-memory stores with Postgres (SQLAlchemy).
- Add auth (magic link), orgs/projects/roles.
- More connectors/destinations (Postgres, S3/CSV, Snowflake, BigQuery).
- Proper workers/scheduling (Temporal), logs & alerts, GitOps.
