from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import List
import redis
import json
import uuid
import asyncio
from datetime import datetime
from enum import Enum
import time
from contextlib import asynccontextmanager

# Global variables
job_queue = asyncio.PriorityQueue()
processing_lock = asyncio.Lock()
last_processed_time = 0
background_task = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global background_task
    background_task = asyncio.create_task(process_jobs())
    yield
    if background_task:
        background_task.cancel()
        try:
            await background_task
        except asyncio.CancelledError:
            pass

app = FastAPI(lifespan=lifespan)

# Redis connection
redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

class Priority(str, Enum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"

    def get_value(self) -> int:
        if self == Priority.HIGH:
            return 0
        elif self == Priority.MEDIUM:
            return 1
        return 2

class IngestionRequest(BaseModel):
    ids: List[int] = Field(..., description="List of IDs to process")
    priority: Priority = Field(..., description="Priority level of the request")

class BatchStatus(str, Enum):
    YET_TO_START = "yet_to_start"
    TRIGGERED = "triggered"
    COMPLETED = "completed"

class Batch:
    def __init__(self, batch_id: str, ids: List[int], status: BatchStatus = BatchStatus.YET_TO_START):
        self.batch_id = batch_id
        self.ids = ids
        self.status = status

    def to_dict(self):
        return {
            "batch_id": self.batch_id,
            "ids": self.ids,
            "status": self.status
        }

class IngestionJob:
    def __init__(self, ingestion_id: str, ids: List[int], priority: Priority, created_time: float):
        self.ingestion_id = ingestion_id
        self.ids = ids
        self.priority = priority
        self.created_time = created_time
        self.batches = []
        self._create_batches()

    def _create_batches(self):
        for i in range(0, len(self.ids), 3):
            batch_ids = self.ids[i:i+3]
            batch = Batch(str(uuid.uuid4()), batch_ids)
            self.batches.append(batch)

    def to_dict(self):
        return {
            "ingestion_id": self.ingestion_id,
            "status": self.get_overall_status(),
            "batches": [batch.to_dict() for batch in self.batches]
        }

    def get_overall_status(self) -> str:
        statuses = [batch.status for batch in self.batches]
        if all(status == BatchStatus.COMPLETED for status in statuses):
            return BatchStatus.COMPLETED
        elif any(status == BatchStatus.TRIGGERED for status in statuses):
            return BatchStatus.TRIGGERED
        return BatchStatus.YET_TO_START

    def __lt__(self, other):
        # Priority comparison (lower value = higher priority)
        if self.priority.get_value() != other.priority.get_value():
            return self.priority.get_value() < other.priority.get_value()
        return self.created_time < other.created_time

async def process_batch(batch: Batch):
    await asyncio.sleep(1)  # Simulate API delay
    batch.status = BatchStatus.COMPLETED
    await update_redis_status(batch)

async def update_redis_status(batch: Batch):
    for key in redis_client.keys("ingestion:*"):
        job_data = json.loads(redis_client.get(key))
        for b in job_data["batches"]:
            if b["batch_id"] == batch.batch_id:
                b["status"] = batch.status
                redis_client.set(key, json.dumps(job_data))
                return

async def process_jobs():
    while True:
        try:
            async with processing_lock:
                global last_processed_time
                current_time = time.time()

                if current_time - last_processed_time < 5:
                    await asyncio.sleep(5 - (current_time - last_processed_time))

                if not job_queue.empty():
                    _, job = await job_queue.get()
                    for batch in job.batches:
                        batch.status = BatchStatus.TRIGGERED
                        await update_redis_status(batch)
                        await process_batch(batch)
                    last_processed_time = time.time()
                else:
                    await asyncio.sleep(0.1)
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"Error in process_jobs: {e}")
            await asyncio.sleep(1)

@app.post("/ingest")
async def ingest_data(request: IngestionRequest):
    for id in request.ids:
        if not 1 <= id <= 10**9 + 7:
            raise HTTPException(status_code=400, detail=f"Invalid ID: {id}. Must be between 1 and 10^9+7")

    ingestion_id = str(uuid.uuid4())
    job = IngestionJob(
        ingestion_id=ingestion_id,
        ids=request.ids,
        priority=request.priority,
        created_time=time.time()
    )

    redis_client.set(f"ingestion:{ingestion_id}", json.dumps(job.to_dict()))
    
    # ✅ Put (priority_value, job) into PriorityQueue
    await job_queue.put((job.priority.get_value(), job))

    return {"ingestion_id": ingestion_id}

@app.get("/status/{ingestion_id}")
async def get_status(ingestion_id: str):
    job_data = redis_client.get(f"ingestion:{ingestion_id}")
    if not job_data:
        raise HTTPException(status_code=404, detail="Ingestion job not found")

    return json.loads(job_data)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5000)
