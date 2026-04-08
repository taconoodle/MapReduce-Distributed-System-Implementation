from Manager.storage import S3Storage, S3ConnectionError

import pytest
from unittest.mock import patch, MagicMock, ANY
from botocore.exceptions import ClientError, EndpointConnectionError

class TestS3Storage:

    @patch('Manager.storage.RUSTFS_URL', 'http://test-url:9000')
    @patch('Manager.storage.RUSTFS_USERNAME', 'admin')
    @patch('Manager.storage.RUSTFS_PASSWORD', 'admin')
    @patch('Manager.storage.boto3.client')
    def test_init_s3_calls_boto3_with_correct_args(self, mock_boto_client):
        mock_conn = MagicMock()
        mock_boto_client.return_value = mock_conn

        s3 = S3Storage()
        assert s3.conn is None # No conn should exist in creation

        result = s3.init_s3()

        mock_boto_client.assert_called_once_with(
            's3',
            endpoint_url='http://test-url:9000',
            aws_access_key_id='admin',
            aws_secret_access_key='admin',
            config=ANY,
            region_name='us-east-1'
        )

        assert s3.conn == mock_conn
        assert result is mock_conn

    @patch('Manager.storage.boto3.client')
    def test_init_s3_raises_on_invalid_username(self, mock_boto_client):
        mock_boto_client.side_effect = ClientError(
            error_response={'Error': {'Code': 'InvalidAccessKeyId'}},
            operation_name='TestCreateConn'
        )

        s3 = S3Storage()
        with pytest.raises(S3ConnectionError) as except_info:
            s3.init_s3()

        assert "Invalid username" in str(except_info.value)

    @patch('Manager.storage.boto3.client')
    def test_init_s3_raises_on_invalid_password(self, mock_boto_client):
        mock_boto_client.side_effect = ClientError(
            error_response={'Error': {'Code': 'SignatureDoesNotMatch'}},
            operation_name='TestCreateConn'
        )

        s3 = S3Storage()
        with pytest.raises(S3ConnectionError) as except_info:
            s3.init_s3()

        assert "Invalid password" in str(except_info.value)

    @patch('Manager.storage.boto3.client')
    def test_init_s3_raises_on_invalid_endpoint_url(self, mock_boto_client):
        mock_boto_client.side_effect = EndpointConnectionError(
            endpoint_url='http://test-url:9000'
        )

        s3 = S3Storage()
        with pytest.raises(S3ConnectionError) as except_info:
            s3.init_s3()

        assert "Could not reach S3 endpoint" in str(except_info.value)

    @patch('Manager.storage.boto3.client')
    def test_init_s3_raises_unknown_error(self, mock_boto_client):
        mock_boto_client.side_effect = ClientError(
            error_response={'Error': {'Code': 'UnknownError'}},
            operation_name='TestCreateConn'
        )

        s3 = S3Storage()
        with pytest.raises(S3ConnectionError) as except_info:
            s3.init_s3()

        assert f"Unknown error" in str(except_info.value)