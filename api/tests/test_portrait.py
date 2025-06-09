import pytest

import odapi


@pytest.mark.integration
@pytest.mark.parametrize(
    'path',
    [
        '/portrait/polg',
        '/portrait/bezk',
        '/portrait/kant',
    ],
)
def test_valid_response_portrait_geo_code(client, path):
    response = client.get(f'{path}/230?limit=10')
    assert response.status_code == 200
    assert response.headers['content-type'] == odapi.GeoJsonResponse.media_type


@pytest.mark.integration
@pytest.mark.parametrize('geo_value', (230, 261))
@pytest.mark.parametrize('filetype', ('/csv', '/xlsx', '/parquet', ''))
def test_valid_response_portrait_geo_value(client, geo_value, filetype):
    response = client.get(f'/portrait/polg/{geo_value}{filetype}?limit=10')
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
@pytest.mark.parametrize('filetype', ('/csv', '/xlsx', '/parquet', ''))
def test_valid_response_portrait_duplicate_request(client, filetype):
    for _ in range(4):
        response = client.get(f'/portrait/polg/230{filetype}?limit=10')
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
