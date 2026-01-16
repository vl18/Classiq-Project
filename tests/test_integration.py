import time
import pytest


QASM3_BELL = """OPENQASM 3;
include "stdgates.inc";
qubit[2] q;
bit[2] c;
h q[0];
cx q[0], q[1];
c[0] = measure q[0];
c[1] = measure q[1];
"""

QASM3_INVALID = "OPENQASM 3; qubit[1] q; BAD"


def _poll_status(client, task_id: str, target: str, timeout_seconds: int = 30) -> dict:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        gr = client.get(f"/tasks/{task_id}")
        if gr.status_code == 200:
            data = gr.json()
            if data["status"] == target:
                return data
        time.sleep(0.5)
    raise AssertionError(f"task did not reach {target} in time")


@pytest.mark.integration
def test_submit_and_poll(require_integration, http_client):
    r = http_client.post("/tasks", json={"qc": QASM3_BELL, "shots": 256})
    r.raise_for_status()
    task_id = r.json()["task_id"]

    data = _poll_status(http_client, task_id, "completed")
    assert isinstance(data["result"], dict)


@pytest.mark.integration
def test_not_found(require_integration, http_client):
    r = http_client.get("/tasks/00000000-0000-0000-0000-000000000000")
    assert r.status_code == 200
    assert r.json() == {"status": "error", "message": "Task not found."}


@pytest.mark.integration
def test_invalid_qc_reaches_error(require_integration, http_client):
    r = http_client.post("/tasks", json={"qc": QASM3_INVALID, "shots": 32})
    r.raise_for_status()
    task_id = r.json()["task_id"]

    _poll_status(http_client, task_id, "error", timeout_seconds=45)


@pytest.mark.integration
def test_validation_errors(require_integration, http_client):
    r = http_client.post("/tasks", json={"qc": "", "shots": 1})
    assert r.status_code == 422

    r = http_client.post("/tasks", json={"qc": QASM3_BELL, "shots": 0})
    assert r.status_code == 422


@pytest.mark.integration
def test_qc_too_large(require_integration, http_client):
    huge_qc = "OPENQASM 3;\n" + ("x" * 2_000_001)
    r = http_client.post("/tasks", json={"qc": huge_qc, "shots": 1})
    assert r.status_code == 413
