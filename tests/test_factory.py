"""Test factory functions"""

import pytest
from titiler.cmr.dependencies import (
    RasterioParams,
    InterpolatedXarrayParams,
    ReaderParams,
)
from titiler.cmr.factory import parse_reader_options
from titiler.cmr.reader import MultiFilesBandsReader
from rio_tiler.io import rasterio


class TestParseReaderOptions:
    """Test parse_reader_options function assertions about bands and indexes"""

    def test_bands_regex_without_bands_fails(self):
        """Test that providing bands_regex without bands raises an assertion error"""
        rasterio_params = RasterioParams(
            bands_regex="B0[1-3]", bands=None, indexes=None
        )
        xarray_io_params = InterpolatedXarrayParams()
        xarray_ds_params = InterpolatedXarrayParams()
        reader_params = ReaderParams(backend="rasterio")

        with pytest.raises(
            AssertionError,
            match="`bands=` option must be provided when using multi-band data",
        ):
            parse_reader_options(
                rasterio_params=rasterio_params,
                xarray_io_params=xarray_io_params,
                xarray_ds_params=xarray_ds_params,
                reader_params=reader_params,
            )

    def test_no_bands_regex_no_bands_no_indexes_fails(self):
        """Test that providing neither bands_regex, bands, nor indexes raises an assertion error"""
        rasterio_params = RasterioParams(
            bands_regex=None,
            bands=None,
            indexes=None,  # This should cause the assertion to fail
        )
        xarray_io_params = InterpolatedXarrayParams()
        xarray_ds_params = InterpolatedXarrayParams()
        reader_params = ReaderParams(backend="rasterio")

        with pytest.raises(
            AssertionError,
            match="you must provide `indexes` if not providing `bands_regex` and `bands`",
        ):
            parse_reader_options(
                rasterio_params=rasterio_params,
                xarray_io_params=xarray_io_params,
                xarray_ds_params=xarray_ds_params,
                reader_params=reader_params,
            )

    def test_bands_regex_with_bands_succeeds(self):
        """Test that providing bands_regex with bands succeeds and returns MultiFilesBandsReader"""
        rasterio_params = RasterioParams(
            bands_regex="B0[1-3]", bands=["B01", "B02", "B03"], indexes=None
        )
        xarray_io_params = InterpolatedXarrayParams()
        xarray_ds_params = InterpolatedXarrayParams()
        reader_params = ReaderParams(backend="rasterio")

        reader, read_options, reader_options = parse_reader_options(
            rasterio_params=rasterio_params,
            xarray_io_params=xarray_io_params,
            xarray_ds_params=xarray_ds_params,
            reader_params=reader_params,
        )

        assert reader == MultiFilesBandsReader
        assert read_options["bands"] == ["B01", "B02", "B03"]
        assert read_options["bands_regex"] == "B0[1-3]"
        assert reader_options == {}

    def test_indexes_without_bands_regex_succeeds(self):
        """Test that providing indexes without bands_regex succeeds and returns rasterio.Reader"""
        rasterio_params = RasterioParams(
            bands_regex=None, bands=None, indexes=[1, 2, 3]
        )
        xarray_io_params = InterpolatedXarrayParams()
        xarray_ds_params = InterpolatedXarrayParams()
        reader_params = ReaderParams(backend="rasterio")

        reader, read_options, reader_options = parse_reader_options(
            rasterio_params=rasterio_params,
            xarray_io_params=xarray_io_params,
            xarray_ds_params=xarray_ds_params,
            reader_params=reader_params,
        )

        assert reader == rasterio.Reader
        assert read_options["indexes"] == [1, 2, 3]
        assert reader_options == {}

    def test_xarray_backend_ignores_rasterio_params(self):
        """Test that xarray backend ignores rasterio_params and doesn't trigger assertions"""
        rasterio_params = RasterioParams(bands_regex=None, bands=None, indexes=None)
        xarray_io_params = InterpolatedXarrayParams()
        xarray_ds_params = InterpolatedXarrayParams()
        reader_params = ReaderParams(backend="xarray")

        reader, read_options, reader_options = parse_reader_options(
            rasterio_params=rasterio_params,
            xarray_io_params=xarray_io_params,
            xarray_ds_params=xarray_ds_params,
            reader_params=reader_params,
        )

        from titiler.xarray.io import Reader as XarrayReader

        assert reader == XarrayReader
