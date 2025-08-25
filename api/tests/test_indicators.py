import pytest


@pytest.mark.integration
@pytest.mark.parametrize(
    'path',
    [
        '/indicators/polg',
        '/indicators/bezk',
        '/indicators/kant',
    ],
)
def test_valid_response_indicators(client, path):
    response = client.get(path)
    assert response.status_code == 200
    assert response.headers['content-type'] == 'application/json'
