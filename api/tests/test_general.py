import pytest


@pytest.mark.integration
def test_root_endpoint_is_up(client):
    response = client.get('/')
    assert response.status_code == 200
    assert response.headers['content-type'] == 'text/html; charset=utf-8'


@pytest.mark.integration
def test_nonexistent_path_returns_404(client):
    response = client.get('/this-path-does-not-exist')
    assert response.status_code == 404
    payload = response.json()
    assert 'detail' in payload
    assert payload['detail'] == 'Not Found'


@pytest.mark.integration
def test_openapi_schema_available(client):
    response = client.get('/openapi.json')
    assert response.status_code == 200
    schema = response.json()
    assert 'openapi' in schema
    assert isinstance(schema['paths'], dict)


@pytest.mark.integration
def test_swagger_ui_docs_page(client):
    response = client.get('/')
    assert response.status_code == 200
    html = response.text
    assert '<title>Swagger UI</title>' in html or 'swagger-ui' in html.lower()
