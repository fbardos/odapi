import datetime as dt
import io

import geopandas as gpd
import pandas as pd
import pytest

import odapi


@pytest.mark.integration
@pytest.mark.slow
@pytest.mark.parametrize('year', range(1850, dt.datetime.now().year - 1))
@pytest.mark.parametrize('dimension', ('municipalities', 'districts', 'cantons'))
@pytest.mark.parametrize('filetype', ('/csv', '/xlsx', '/parquet', ''))
def test_valid_response_dimensions(client, year, filetype, dimension):
    response = client.get(f'/{dimension}/{year}{filetype}?limit=10')
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
    'year',
    (1850, 1900, 1945, 1980, 1999, 2003, 2011, 2015, 2016, dt.datetime.now().year - 1),
)
@pytest.mark.parametrize('dimension', ('municipalities', 'districts', 'cantons'))
@pytest.mark.parametrize('filetype', ('/csv', '/xlsx', '/parquet', ''))
def test_data_rows_dimensions(client, year, filetype, dimension):
    response = client.get(f'/{dimension}/{year}{filetype}?limit=100')
    match filetype:
        case 'csv/':
            assert response.headers['content-type'] == odapi.CsvResponse.media_type
            df = pd.read_csv(io.StringIO(response.text))
            if dimension == 'cantons':
                assert len(df) > 20 & len(df) < 30
            else:
                assert len(df) == 100
        case 'xlsx/':
            assert response.headers['content-type'] == odapi.XlsxResponse.media_type
            df = pd.read_excel(io.BytesIO(response.content))
            if dimension == 'cantons':
                assert len(df) > 20 & len(df) < 30
            else:
                assert len(df) == 100
        case 'parquet/':
            assert (
                response.headers['content-type'] == odapi.GeoparquetResponse.media_type
            )
            df = pd.read_parquet(io.BytesIO(response.content))
            if dimension == 'cantons':
                assert len(df) > 20 & len(df) < 30
            else:
                assert len(df) == 100
        case '':
            assert response.headers['content-type'] == odapi.GeoJsonResponse.media_type
            gdf = gpd.read_file(io.StringIO(response.text))
            if dimension == 'cantons':
                assert len(gdf) > 20 & len(gdf) < 30
            else:
                assert len(gdf) == 100
