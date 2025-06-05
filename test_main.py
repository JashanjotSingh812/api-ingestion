import pytest
from fastapi.testclient import TestClient
import time
from main import app, BatchStatus

client = TestClient(app)

# Helper: Wait for a specific batch to reach expected status
def wait_for_status(ingestion_id: str, batch_index: int, expected_status: str, timeout: int = 30):
    start_time = time.time()
    while time.time() - start_time < timeout:
        response = client.get(f"/status/{ingestion_id}")
        if response.status_code != 200:
            time.sleep(0.2)
            continue
        data = response.json()
        current_status = data["batches"][batch_index]["status"]
        if current_status == expected_status:
            return True
        time.sleep(0.2)
    raise AssertionError(
        f"Timeout: batch {batch_index} of ingestion_id '{ingestion_id}' "
        f"did not reach expected status '{expected_status}'. "
        f"Last known status: '{current_status}'"
    )


def test_ingest_invalid_id():
    response = client.post(
        "/ingest",
        json={"ids": [0, 1, 2], "priority": "HIGH"}
    )
    assert response.status_code == 400


def test_ingest_valid_request():
    response = client.post(
        "/ingest",
        json={"ids": [1, 2, 3, 4, 5], "priority": "HIGH"}
    )
    assert response.status_code == 200
    data = response.json()
    assert "ingestion_id" in data


def test_status_endpoint():
    response = client.post(
        "/ingest",
        json={"ids": [1, 2, 3, 4, 5], "priority": "HIGH"}
    )
    ingestion_id = response.json()["ingestion_id"]

    response = client.get(f"/status/{ingestion_id}")
    assert response.status_code == 200
    data = response.json()
    assert data["ingestion_id"] == ingestion_id
    assert "status" in data
    assert "batches" in data
    assert len(data["batches"]) == 2  # 5 IDs → 2 batches (3 + 2)


def test_rate_limiting():
    response1 = client.post("/ingest", json={"ids": [1, 2, 3], "priority": "MEDIUM"})
    ingestion_id1 = response1.json()["ingestion_id"]

    time.sleep(2)

    response2 = client.post("/ingest", json={"ids": [4, 5, 6], "priority": "HIGH"})
    ingestion_id2 = response2.json()["ingestion_id"]

    assert wait_for_status(ingestion_id2, 0, BatchStatus.TRIGGERED.value)
    assert wait_for_status(ingestion_id2, 0, BatchStatus.COMPLETED.value)
    assert wait_for_status(ingestion_id1, 0, BatchStatus.COMPLETED.value)


def test_priority_processing():
    response1 = client.post("/ingest", json={"ids": [1, 2, 3], "priority": "LOW"})
    ingestion_id1 = response1.json()["ingestion_id"]

    time.sleep(1)
    response2 = client.post("/ingest", json={"ids": [4, 5, 6], "priority": "HIGH"})
    ingestion_id2 = response2.json()["ingestion_id"]

    time.sleep(1)
    response3 = client.post("/ingest", json={"ids": [7, 8, 9], "priority": "MEDIUM"})
    ingestion_id3 = response3.json()["ingestion_id"]

    assert wait_for_status(ingestion_id2, 0, BatchStatus.TRIGGERED.value)
    assert wait_for_status(ingestion_id2, 0, BatchStatus.COMPLETED.value)
    assert wait_for_status(ingestion_id3, 0, BatchStatus.COMPLETED.value)
    assert wait_for_status(ingestion_id1, 0, BatchStatus.COMPLETED.value)


def test_batch_size_limit():
    response = client.post(
        "/ingest",
        json={"ids": [1, 2, 3, 4, 5, 6, 7], "priority": "HIGH"}
    )
    ingestion_id = response.json()["ingestion_id"]

    data = client.get(f"/status/{ingestion_id}").json()
    assert len(data["batches"]) == 3  # 3 + 3 + 1
    assert len(data["batches"][0]["ids"]) == 3
    assert len(data["batches"][1]["ids"]) == 3
    assert len(data["batches"][2]["ids"]) == 1


def test_invalid_ingestion_id():
    response = client.get("/status/nonexistent")
    assert response.status_code == 404


def test_invalid_priority():
    response = client.post(
        "/ingest",
        json={"ids": [1, 2, 3], "priority": "INVALID"}
    )
    assert response.status_code == 422  # FastAPI validation error


if __name__ == "__main__":
    pytest.main(["-v", "test_main.py"])
