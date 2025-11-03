"""Test titiler.cmr.reader module."""

from unittest.mock import patch


from titiler.cmr.reader import AWSSessionsReader, MultiFilesBandsReader


class TestAWSSessionsReader:
    """Test AWSSessionsReader class."""

    @patch("rio_tiler.io.rasterio.Reader.__attrs_post_init__")
    def test_aws_session_reader_context_manager(self, mock_post_init):
        """Test AWSSessionsReader context manager properly sets up AWS session.

        This test verifies that AWSSessionsReader:
        1. Creates a real AWSSession with the provided credentials
        2. Creates a rasterio.Env context with that session
        3. Properly enters and exits those contexts
        """
        from rasterio.session import AWSSession
        from rasterio import Env as RasterioEnv

        # Skip the parent __attrs_post_init__ to avoid opening files
        mock_post_init.return_value = None

        s3_credentials = {
            "accessKeyId": "test_key",
            "secretAccessKey": "test_secret",
            "sessionToken": "test_token",
        }

        test_file = "s3://test-bucket/test-file.tif"

        # Create reader
        reader = AWSSessionsReader(test_file, s3_credentials=s3_credentials)

        # Before entering context, session should be None
        assert isinstance(reader.aws_session, AWSSession)
        assert reader.env_ctx is None

        # Enter context
        reader.__enter__()

        # After entering, Env context should be created
        assert isinstance(reader.env_ctx, RasterioEnv)

        # Clean up
        reader.__exit__(None, None, None)


class TestMultiFilesBandsReader:
    """Test MultiFilesBandsReader class."""

    def test_multifiles_bands_reader_with_aws_session_reader(self):
        """Test that MultiFilesBandsReader passes s3_credentials to AWSSessionsReader instances.

        When MultiFilesBandsReader is initialized with reader=AWSSessionsReader and
        s3_credentials in reader_options, those credentials should be passed to each
        AWSSessionsReader instance created for reading individual bands.
        """
        from rasterio.session import AWSSession

        # Mock S3 credentials
        s3_credentials = {
            "accessKeyId": "test_key",
            "secretAccessKey": "test_secret",
            "sessionToken": "test_token",
        }

        # Create input dictionary mapping band names to URLs
        input_files = {
            "red": "s3://test-bucket/red.tif",
        }

        # Create reader options that include s3_credentials
        reader_options = {"s3_credentials": s3_credentials}

        # Initialize MultiFilesBandsReader with explicit AWSSessionsReader
        multi_reader = MultiFilesBandsReader(
            input=input_files,
            reader=AWSSessionsReader,
            reader_options=reader_options,
        )

        # Verify initialization
        assert multi_reader.input == input_files
        assert multi_reader.reader == AWSSessionsReader
        assert multi_reader.reader_options == reader_options

        # Now test that when we create a reader instance (as MultiBandReader.tile does),
        # the AWSSessionsReader gets the s3_credentials
        url = multi_reader._get_band_url("red")

        # Mock the parent __attrs_post_init__ to avoid opening files
        with patch("rio_tiler.io.rasterio.Reader.__attrs_post_init__"):
            # Create a reader instance the same way MultiBandReader.tile does
            reader_instance = multi_reader.reader(
                url, tms=multi_reader.tms, **multi_reader.reader_options
            )

            # Verify the reader instance has the s3_credentials
            assert isinstance(reader_instance, AWSSessionsReader)
            assert isinstance(reader_instance.aws_session, AWSSession)
