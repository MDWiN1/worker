from fastapi import FastAPI
from worker import run_ingest

app = FastAPI()


@app.post("/admin/run-ingest")
def run_ingest_manual():
    run_ingest()
    return {"message": "ingest selesai"}