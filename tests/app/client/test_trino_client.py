import unittest

from pydantic_core._pydantic_core import ValidationError

from app.client.trino_client import extract_connection_details


class TestExtractConnectionDetails(unittest.TestCase):
    def test_trino_jdbc_url(self):
        url = "jdbc:trino://trino.czxqx2r9.data.bizmrg.com:443?user=hackuser&password=dovq(ozaq8ngt)oS"
        expected = {
            "host": "trino.czxqx2r9.data.bizmrg.com",
            "port": 443,
            "username": "hackuser",
            "password": "dovq(ozaq8ngt)oS",
        }
        result = extract_connection_details(url)
        self.assertEqual(result.model_dump(), expected)

    def test_missing_user_password(self):
        url = "jdbc:trino://host.example.com:1234"
        with self.assertRaises(ValidationError):
            extract_connection_details(url)

    def test_different_order_query_params(self):
        url = "jdbc:trino://host.example.com:1234?password=abc123&user=testuser"
        expected = {
            "host": "host.example.com",
            "port": 1234,
            "username": "testuser",
            "password": "abc123",
        }
        result = extract_connection_details(url)
        self.assertEqual(result.model_dump(), expected)
