from chalice.test import Client
from app import app


def test_index():
    with Client(app) as client:
        response = client.http.get('/test-new')
        assert response.json_body['code'] == 200
