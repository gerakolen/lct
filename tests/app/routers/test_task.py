import uuid
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app.db import get_session
from app.main import app
from app.model import NewTaskRequest, create_mock_response
from app.routers.task import sort_queries_by_runquantity
from app.schema import Task, TaskStatus
from app.security import require_basic_auth

TEST_TASK_ID = "9d8edbee-5f4a-4259-bd5e-151dfa9d7742"


@pytest.fixture
def client():
    test_client = TestClient(app)
    try:
        yield test_client
    finally:
        app.dependency_overrides.clear()


@pytest.fixture
def router_session_mock():
    return MagicMock(name="Session")


################# utils  #################
def test_sort_queries_by_runquantity():
    input_data = {
        "url": "jdbc:postgresql://localhost:5432/mydb?login=admin&password=secret",
        "ddl": [],
        "queries": [
            {
                "queryid": uuid.UUID("9d8edbee-5f4a-4259-bd5e-111111111111"),
                "query": "SELECT * FROM aaaa",
                "runquantity": 10,
                "executiontime": 12,
            },
            {
                "queryid": uuid.UUID("9d8edbee-5f4a-4259-bd5e-222222222222"),
                "query": "SELECT * FROM bcde",
                "runquantity": 200,
                "executiontime": 13,
            },
            {
                "queryid": uuid.UUID("9d8edbee-5f4a-4259-bd5e-333333333333"),
                "query": "SELECT * FROM dddd",
                "runquantity": 50,
                "executiontime": 14,
            },
        ],
    }
    expected_order = [
        uuid.UUID("9d8edbee-5f4a-4259-bd5e-222222222222"),
        uuid.UUID("9d8edbee-5f4a-4259-bd5e-333333333333"),
        uuid.UUID("9d8edbee-5f4a-4259-bd5e-111111111111"),
    ]

    result = sort_queries_by_runquantity(NewTaskRequest(**input_data))
    sorted_queryids = [q.queryid for q in result.queries]
    assert sorted_queryids == expected_order


################# /status  #################
def test_task_status_unauthorized(client, router_session_mock, monkeypatch):
    resp = client.get("/status", params={"task_id": TEST_TASK_ID})
    assert resp.status_code == 401


def test_task_status_pending(client, router_session_mock, monkeypatch):
    mock_task = MagicMock(name="Task")
    mock_task.get.return_value = Task(id=TEST_TASK_ID, status=TaskStatus.PENDING)
    app.dependency_overrides[get_session] = lambda: mock_task
    app.dependency_overrides[require_basic_auth] = lambda: "test-user"

    resp = client.get("/status", params={"task_id": TEST_TASK_ID})
    assert resp.status_code == 200
    assert resp.json() == {"status": "PENDING"}


def test_task_status_not_found(client, router_session_mock, monkeypatch):
    mock_task = MagicMock(name="Task")
    mock_task.get.return_value = None
    app.dependency_overrides[get_session] = lambda: mock_task
    app.dependency_overrides[require_basic_auth] = lambda: "test-user"

    resp = client.get("/status", params={"task_id": TEST_TASK_ID})
    assert resp.status_code == 404
    assert resp.json() == {"detail": "Task not found"}


################# /getresult  #################


def test_get_result_unauthorized(client, router_session_mock, monkeypatch):
    resp = client.get("/getresult", params={"task_id": TEST_TASK_ID})
    assert resp.status_code == 401


def test_get_result_unsuccessful(client, router_session_mock, monkeypatch):
    mock_task = MagicMock(name="Task")
    mock_task.get.side_effect = [
        None,
        Task(status=TaskStatus.PENDING),
        Task(status=TaskStatus.RUNNING),
        Task(status=TaskStatus.FAILED, error="error text"),
    ]
    app.dependency_overrides[get_session] = lambda: mock_task
    app.dependency_overrides[require_basic_auth] = lambda: "test-user"

    # First call (Not Found)
    resp = client.get("/getresult", params={"task_id": TEST_TASK_ID})
    assert resp.status_code == 404
    assert resp.json() == {"detail": "Task not found"}

    # Second call (Pending)
    resp1 = client.get("/getresult", params={"task_id": TEST_TASK_ID})
    assert resp1.status_code == 400
    assert resp1.json() == {"detail": {"status": "PENDING"}}

    # Third call ( Running)
    resp2 = client.get("/getresult", params={"task_id": TEST_TASK_ID})
    assert resp2.status_code == 400
    assert resp2.json() == {"detail": {"status": "RUNNING"}}

    # Fourth call ( Failed)
    resp3 = client.get("/getresult", params={"task_id": TEST_TASK_ID})
    assert resp3.status_code == 400
    assert resp3.json() == {"detail": {"status": "FAILED", "error": "error text"}}


def test_get_result_successful(client, router_session_mock, monkeypatch):
    mock_task = MagicMock(name="Task")
    mock_task.get.return_value = Task(
        id=TEST_TASK_ID, status=TaskStatus.COMPLETE, result={"info": "success"}
    )
    app.dependency_overrides[get_session] = lambda: mock_task
    app.dependency_overrides[require_basic_auth] = lambda: "test-user"

    resp = client.get("/getresult", params={"task_id": TEST_TASK_ID})
    assert resp.status_code == 200
    assert resp.json() == create_mock_response().model_dump(mode="json")


################# /new  #################


def test_new_unauthorized(client, router_session_mock, monkeypatch):
    resp = client.post("/new", json={})
    assert resp.status_code == 401


def test_new(client, router_session_mock, monkeypatch):
    fixed_uuid = uuid.UUID(TEST_TASK_ID)

    with patch("uuid.uuid4", return_value=fixed_uuid):
        with patch("app.worker_task.process_task.delay") as mock_delay:
            request_data = {
                "url": "jdbc:postgresql://localhost:5432/mydb?login=admin&password=secret",
                "ddl": [
                    {
                        "statement": "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))"
                    },
                    {
                        "statement": "CREATE TABLE orders (order_id INT PRIMARY KEY, user_id INT, amount DECIMAL)"
                    },
                ],
                "queries": [
                    {
                        "queryid": "0197a0b2-2284-7af8-9012-fcb21e1a9785",
                        "query": "SELECT u.id, u.name, COUNT(o.order_id) FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.id",
                        "runquantity": 123,
                        "executiontime": 12,
                    },
                    {
                        "queryid": "c8ed3309-1acb-439a-b32b-f802ba41db3e",
                        "query": "WITH active_users AS (SELECT id FROM users WHERE active = true) SELECT * FROM orders WHERE user_id IN (SELECT id FROM active_users)",
                        "runquantity": 112233,
                        "executiontime": 1222,
                    },
                ],
            }

            mock_task = MagicMock(name="Task")
            app.dependency_overrides[get_session] = lambda: mock_task
            app.dependency_overrides[require_basic_auth] = lambda: "test-user"

            resp = client.post("/new", json=request_data)
            assert resp.status_code == 200
            assert resp.json() == {"taskid": TEST_TASK_ID}

            mock_delay.assert_called_once()
            called_task_id, called_payload = mock_delay.call_args[0]
            assert called_task_id == TEST_TASK_ID
