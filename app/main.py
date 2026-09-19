from starlette.middleware.gzip import GZipMiddleware
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.core.config import settings
from app.db.session import Base, engine
from app.api.routes import router
from app.api.live import router as live_router
from app.api.historical import router as historical_router

app = FastAPI(title="INDRA Urban Flood Intelligence", version="2.0")
app.add_middleware(GZipMiddleware, minimum_size=1000)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://indra-frontend-deployment.vercel.app",
        "http://localhost:5173",
        "http://localhost:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(router)
app.include_router(live_router, prefix="/api")
app.include_router(historical_router, prefix="/api")

@app.on_event("startup")
def startup():
    Base.metadata.create_all(bind=engine)

import asyncio
from app.db.session import SessionLocal
from app.services.ingestion import run_ingestion_cycle

INGESTION_INTERVAL_SECONDS = 120  # har 2 min me refresh

async def _ingestion_loop():
    while True:
        db = SessionLocal()
        try:
            await run_ingestion_cycle(db)
        except Exception as exc:
            print(f"[ingestion] cycle failed: {exc}")
        finally:
            db.close()
        await asyncio.sleep(INGESTION_INTERVAL_SECONDS)

@app.on_event("startup")
async def start_ingestion():
    asyncio.create_task(_ingestion_loop())

@app.get("/")
def root(): return {"name":"INDRA", "assistant":"Aeravat", "status":"operational", "mode":"real-data"}
