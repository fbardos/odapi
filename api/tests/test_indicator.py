import pytest

import odapi


@pytest.mark.integration
@pytest.mark.parametrize(
    'path',
    [
        '/indicator/polg',
        '/indicator/bezk',
        '/indicator/kant',
    ],
)
def test_valid_response_indicator_geo_code(client, path):
    response = client.get(f'{path}/1?limit=100')
    assert response.status_code == 200
    assert response.headers['content-type'] == odapi.GeoJsonResponse.media_type


@pytest.mark.integration
@pytest.mark.parametrize('filetype', ('/csv', '/xlsx', '/parquet', ''))
def test_valid_response_indicator_duplicate_request(client, filetype):
    for _ in range(4):
        response = client.get(f'/indicator/polg/22{filetype}?limit=10')
        assert response.status_code == 200
        match filetype:
            case 'csv/':
                assert response.headers['content-type'] == odapi.CsvResponse.media_type
            case 'xlsx/':
                assert response.headers['content-type'] == odapi.XlsxResponse.media_type
            case 'parquet/':
                assert (
                    response.headers['content-type']
                    == odapi.GeoparquetResponse.media_type
                )
            case '':
                assert (
                    response.headers['content-type'] == odapi.GeoJsonResponse.media_type
                )


@pytest.mark.integration
@pytest.mark.slow
@pytest.mark.parametrize('indicator_id', range(1, 100))
@pytest.mark.parametrize('filetype', ('/csv', '/xlsx', '/parquet', ''))
def test_valid_response_indicator_indicator_id(client, indicator_id, filetype):
    response = client.get(f'/indicator/polg/{indicator_id}{filetype}?limit=10')
    assert response.status_code == 200
    match filetype:
        case 'csv/':
            assert response.headers['content-type'] == odapi.CsvResponse.media_type
        case 'xlsx/':
            assert response.headers['content-type'] == odapi.XlsxResponse.media_type
        case 'parquet/':
            assert (
                response.headers['content-type'] == odapi.GeoparquetResponse.media_type
            )
        case '':
            assert response.headers['content-type'] == odapi.GeoJsonResponse.media_type


@pytest.mark.integration
@pytest.mark.parametrize(
    'path',
    [
        '/indicator/polg',
        '/indicator/bezk',
        '/indicator/kant',
    ],
)
def test_invalid_indicator_id(client, path):
    _invalid_indicator_id = 99_999
    response = client.get(f'{path}/{_invalid_indicator_id}?limit=100')
    assert response.status_code == 200
    assert response.headers['content-type'] == odapi.GeoJsonResponse.media_type
