import time
import uuid
from typing import List, Optional
from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy import text
from database import get_db, engine, Base

# Initialize DB tables
# For SQLite/Local dev without migrations, we create tables if they don't exist
# Note: In a real app with migrations (Alembic), this is not recommended.
try:
    with engine.connect() as conn:
        conn.execute(text("SELECT 1 FROM monitors"))
except Exception:
    # Very basic schema creation for SQLite if tables missing
    # In reality, we should parse db_schema.sql or define models.
    # For this fix, we will define minimal models or execute raw SQL.
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS monitors (
                id TEXT PRIMARY KEY,
                name TEXT,
                query TEXT,
                platform TEXT,
                region TEXT,
                status TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS leads (
                id TEXT PRIMARY KEY,
                monitor_id TEXT,
                source TEXT,
                url TEXT,
                snippet TEXT,
                score FLOAT,
                fingerprint TEXT,
                status TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """))
        conn.commit()

app = FastAPI(title="Gravity API", version="0.1.0")

# Pydantic Models
class MonitorBase(BaseModel):
    name: str
    query: str
    platform: str
    region: Optional[str] = None

class MonitorCreate(MonitorBase):
    pass

class Monitor(MonitorBase):
    id: str
    status: str
    created_at: float

    class Config:
        orm_mode = True

class Lead(BaseModel):
    id: str
    monitor_id: str
    source: str
    url: Optional[str]
    snippet: Optional[str]
    score: float
    status: str
    created_at: float

# API Endpoints

@app.get("/monitors", response_model=List[Monitor])
def get_monitors(db: Session = Depends(get_db)):
    # Using raw SQL for now as we haven't defined full ORM models for the existing schema
    # Ideally we would map db_schema.sql to SQLAlchemy models
    try:
        result = db.execute(text("SELECT id, name, query, platform, region, status, created_at FROM monitors"))
        monitors = []
        for row in result:
            monitors.append({
                "id": row.id,
                "name": row.name,
                "query": row.query,
                "platform": row.platform,
                "region": row.region,
                "status": row.status,
                "created_at": row.created_at.timestamp() if row.created_at else 0.0
            })
        return monitors
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/monitors", response_model=Monitor)
def create_monitor(monitor: MonitorCreate, db: Session = Depends(get_db)):
    monitor_id = str(uuid.uuid4())
    try:
        db.execute(
            text("""
                INSERT INTO monitors (id, name, query, platform, region, status, created_at)
                VALUES (:id, :name, :query, :platform, :region, 'active', NOW())
            """),
            {
                "id": monitor_id,
                "name": monitor.name,
                "query": monitor.query,
                "platform": monitor.platform,
                "region": monitor.region
            }
        )
        db.commit()
        
        # Return created object
        return {
            "id": monitor_id,
            "name": monitor.name,
            "query": monitor.query,
            "platform": monitor.platform,
            "region": monitor.region,
            "status": "active",
            "created_at": time.time()
        }
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/leads", response_model=List[Lead])
def get_leads(monitor_id: Optional[str] = None, min_score: float = 0.0, db: Session = Depends(get_db)):
    query = "SELECT id, monitor_id, source, url, snippet, score, status, created_at FROM leads WHERE score >= :min_score"
    params = {"min_score": min_score}
    
    if monitor_id:
        query += " AND monitor_id = :monitor_id"
        params["monitor_id"] = monitor_id
        
    query += " ORDER BY created_at DESC LIMIT 100"
    
    try:
        result = db.execute(text(query), params)
        leads = []
        for row in result:
            leads.append({
                "id": row.id,
                "monitor_id": row.monitor_id,
                "source": row.source,
                "url": row.url,
                "snippet": row.snippet,
                "score": row.score,
                "status": row.status,
                "created_at": row.created_at.timestamp() if row.created_at else 0.0
            })
        return leads
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
def health_check():
    return {"status": "ok"}
