from Manager.storage import S3Storage, S3ConnectionError

import pytest
from unittest.mock import patch, MagicMock, ANY
from botocore.exceptions import *

TEST_BUCKET_NAME = 'test-bucket'
TEST_KEY_NAME = 'test-key'
TEST_FILE_NAME = 'test-file'
TEST_BODY = 'test-body'
TEST_ENDPOINT_URL = 'http://test-url:9000'
TEST_USERNAME = 'admin'
TEST_PASSWORD = 'admin'
TEST_UPLOAD_ID = 'test-upload-id'
TEST_BYTE_AMOUNT_MB = 100
TEST_BYTE_OFFSET = 10

class TestS3Storage:

    @patch('Manager.storage.RUSTFS_URL', TEST_ENDPOINT_URL)
    @patch('Manager.storage.RUSTFS_USERNAME', TEST_USERNAME)
    @patch('Manager.storage.RUSTFS_PASSWORD', TEST_PASSWORD)
    @patch('Manager.storage.boto3.client')
    def test_init_s3_calls_boto3_with_correct_args(self, mock_boto_client):
        mock_conn = MagicMock()
        mock_boto_client.return_value = mock_conn

        s3 = S3Storage()
        assert s3.conn is None # No conn should exist in creation

        result = s3.init_s3()

        mock_boto_client.assert_called_once_with(
            's3',
            endpoint_url=TEST_ENDPOINT_URL,
            aws_access_key_id=TEST_USERNAME,
            aws_secret_access_key=TEST_PASSWORD,
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
            endpoint_url=TEST_ENDPOINT_URL
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

        assert "Unknown error" in str(except_info.value)

    @pytest.fixture
    def mock_conn(self):
        return MagicMock()

    @pytest.fixture
    def s3(self, mock_conn):
        with patch('Manager.storage.boto3.client') as mock_boto:
            mock_boto.return_value = mock_conn
            s3 = S3Storage()
            s3.init_s3()
            yield s3

    def test_create_bucket_success(self, s3, mock_conn):
        mock_conn.create_bucket.return_value = {
            'Location': f'/{TEST_BUCKET_NAME}'
        }
        result = s3.create_bucket(TEST_BUCKET_NAME)

        mock_conn.create_bucket.assert_called_once_with(Bucket=TEST_BUCKET_NAME)
        assert result['Location'] == f'/{TEST_BUCKET_NAME}'

    def test_create_bucket_already_exists(self, s3, mock_conn):
        mock_conn.create_bucket.side_effect = ClientError(
            error_response={'Error': {'Code': 'BucketAlreadyExists'}},
            operation_name='TestBucketCreate'
        )

        with pytest.raises(S3ConnectionError) as except_info:
            s3.create_bucket(TEST_BUCKET_NAME)
        assert 'Bucket already exists' in str(except_info.value)

    def test_create_bucket_invalid_name(self, s3, mock_conn):
        mock_conn.create_bucket.side_effect = ClientError(
            error_response={'Error': {'Code': 'InvalidBucketName'}},
            operation_name='TestBucketCreate'
        )

        with pytest.raises(S3ConnectionError) as except_info:
            s3.create_bucket(TEST_BUCKET_NAME)
        assert 'Invalid bucket name' in str(except_info.value)

    def test_create_bucket_when_too_many_exist(self, s3, mock_conn):
        mock_conn.create_bucket.side_effect = ClientError(
            error_response={'Error': {'Code': 'TooManyBuckets'}},
            operation_name='TestBucketCreate'
        )

        with pytest.raises(S3ConnectionError) as except_info:
            s3.create_bucket(TEST_BUCKET_NAME)
        assert 'Too many buckets' in str(except_info.value)

    def test_create_bucket_raises_unknown_error(self, s3, mock_conn):
        mock_conn.create_bucket.side_effect = ClientError(
            error_response={'Error': {'Code': 'UnknownError'}},
            operation_name='TestBucketCreate'
        )

        with pytest.raises(S3ConnectionError) as except_info:
            s3.create_bucket(TEST_BUCKET_NAME)
        assert 'Unknown error' in str(except_info.value)

    def test_create_object_success(self, s3, mock_conn):
        mock_conn.put_object.return_value = {
            'ETag': 'fake-etag'
        }
        result = s3.create_object(TEST_BUCKET_NAME, TEST_KEY_NAME, TEST_BODY)

        mock_conn.put_object.assert_called_once_with(
            Bucket=TEST_BUCKET_NAME,
            Key=TEST_KEY_NAME,
            Body=TEST_BODY
        )
        assert result['ETag'] == 'fake-etag'

    def test_create_object_bucket_does_not_exist(self, s3, mock_conn):
        mock_conn.put_object.side_effect = ClientError(
            error_response={'Error': {'Code': 'NoSuchBucket'}},
            operation_name='TestCreateObject'
        )

        with pytest.raises(S3ConnectionError) as except_info:
            s3.create_object(TEST_BUCKET_NAME, TEST_KEY_NAME, TEST_BODY)
        assert 'Bucket does not exist' in str(except_info.value)

    def test_create_object_invalid_bucket_name(self, s3, mock_conn):
        mock_conn.put_object.side_effect = ClientError(
            error_response={'Error': {'Code': 'InvalidBucketName'}},
            operation_name='TestCreateObject'
        )

        with pytest.raises(S3ConnectionError) as except_info:
            s3.create_object(TEST_BUCKET_NAME, TEST_KEY_NAME, TEST_BODY)
        assert 'Invalid bucket name' in str(except_info.value)

    def test_create_object_endpoint_failure(self, s3, mock_conn):
        mock_conn.put_object.side_effect = EndpointConnectionError(
            endpoint_url='http://test-url:9000'
        )

        with pytest.raises(S3ConnectionError) as except_info:
            s3.create_object(TEST_BUCKET_NAME, TEST_KEY_NAME, TEST_BODY)
        assert 'Could not reach S3 endpoint' in str(except_info.value)

    def test_create_object_unknown_error(self, s3, mock_conn):
        mock_conn.put_object.side_effect = ClientError(
            error_response={'Error': {'Code': 'UnknownError'}},
            operation_name='TestCreateObject'
        )

        with pytest.raises(S3ConnectionError) as except_info:
            s3.create_object(TEST_BUCKET_NAME, TEST_KEY_NAME, TEST_BODY)
        assert 'Unknown error' in str(except_info.value)

    def test_upload_to_bucket_success(self, s3, mock_conn):
        s3.upload_to_bucket(TEST_BUCKET_NAME, TEST_FILE_NAME, TEST_KEY_NAME)

        mock_conn.upload_file.assert_called_once_with(
            Bucket=TEST_BUCKET_NAME,
            Key=TEST_KEY_NAME,
            Filename=TEST_FILE_NAME
        )

    def test_upload_to_bucket_with_no_keyname(self, s3, mock_conn):
        s3.upload_to_bucket(TEST_BUCKET_NAME, TEST_FILE_NAME)

        mock_conn.upload_file.assert_called_once_with(
            Bucket=TEST_BUCKET_NAME,
            Key=TEST_FILE_NAME,
            Filename=TEST_FILE_NAME
        )

    def test_upload_to_bucket_does_not_exist(self, s3, mock_conn):
        mock_conn.upload_file.side_effect = ClientError(
            error_response={'Error': {'Code': 'NoSuchBucket'}},
            operation_name='TestUploadToBucket'
        )

        with pytest.raises(S3ConnectionError) as except_info:
            s3.upload_to_bucket(TEST_BUCKET_NAME, TEST_FILE_NAME, TEST_KEY_NAME)
        assert 'Bucket does not exist' in str(except_info.value)

    def test_upload_to_bucket_invalid_bucket_name(self, s3, mock_conn):
        mock_conn.upload_file.side_effect = ClientError(
            error_response={'Error': {'Code': 'InvalidBucketName'}},
            operation_name='TestUploadToBucket'
        )

        with pytest.raises(S3ConnectionError) as except_info:
            s3.upload_to_bucket(TEST_BUCKET_NAME, TEST_FILE_NAME, TEST_KEY_NAME)
        assert 'Invalid bucket name' in str(except_info.value)

    def test_upload_to_bucket_endpoint_failure(self, s3, mock_conn):
        mock_conn.upload_file.side_effect = EndpointConnectionError(
            endpoint_url='http://test-url:9000'
        )

        with pytest.raises(S3ConnectionError) as except_info:
            s3.upload_to_bucket(TEST_BUCKET_NAME, TEST_FILE_NAME, TEST_KEY_NAME)
        assert 'Could not reach S3 endpoint' in str(except_info.value)

    def test_upload_to_bucket_unknown_error(self, s3, mock_conn):
        mock_conn.upload_file.side_effect = ClientError(
            error_response={'Error': {'Code': 'UnknownError'}},
            operation_name='TestUploadToBucket'
        )

        with pytest.raises(S3ConnectionError) as except_info:
            s3.upload_to_bucket(TEST_BUCKET_NAME, TEST_FILE_NAME, TEST_KEY_NAME)
        assert 'Unknown error' in str(except_info.value)

    def test_download_from_bucket_success(self, s3, mock_conn):
        s3.download_from_bucket(TEST_BUCKET_NAME, TEST_KEY_NAME, TEST_FILE_NAME)

        mock_conn.download_file.assert_called_once_with(
            Key=TEST_KEY_NAME,
            Bucket=TEST_BUCKET_NAME,
            Filename=TEST_FILE_NAME
        )

    def test_download_from_bucket_with_no_filename(self, s3, mock_conn):
        s3.download_from_bucket(TEST_BUCKET_NAME, TEST_KEY_NAME)

        mock_conn.download_file.assert_called_once_with(
            Key=TEST_KEY_NAME,
            Bucket=TEST_BUCKET_NAME,
            Filename=TEST_KEY_NAME
        )

    def test_download_from_bucket_bucket_does_not_exist(self, s3, mock_conn):
        mock_conn.download_file.side_effect = ClientError(
            error_response={'Error': {'Code': 'NoSuchBucket'}},
            operation_name='TestDownloadFromBucket'
        )
        with pytest.raises(S3ConnectionError) as except_info:
            s3.download_from_bucket(TEST_BUCKET_NAME, TEST_KEY_NAME)
        assert 'Bucket does not exist' in str(except_info.value)

    def test_download_from_bucket_key_does_not_exist(self, s3, mock_conn):
        mock_conn.download_file.side_effect = ClientError(
            error_response={'Error': {'Code': 'NoSuchKey'}},
            operation_name='TestDownloadFromBucket'
        )

        with pytest.raises(S3ConnectionError) as except_info:
            s3.download_from_bucket(TEST_BUCKET_NAME, TEST_KEY_NAME)
        assert 'Key does not exist' in str(except_info.value)

    def test_download_from_bucket_invalid_bucket_name(self, s3, mock_conn):
        mock_conn.download_file.side_effect = ClientError(
            error_response={'Error': {'Code': 'InvalidBucketName'}},
            operation_name='TestDownloadFromBucket'
        )
        with pytest.raises(S3ConnectionError) as except_info:
            s3.download_from_bucket(TEST_BUCKET_NAME, TEST_KEY_NAME)
        assert 'Invalid bucket name' in str(except_info.value)

    def test_download_from_bucket_endpoint_failure(self, s3, mock_conn):
        mock_conn.download_file.side_effect = EndpointConnectionError(
            endpoint_url=TEST_ENDPOINT_URL
        )
        with pytest.raises(S3ConnectionError) as except_info:
            s3.download_from_bucket(TEST_BUCKET_NAME, TEST_KEY_NAME)
        assert 'Could not reach S3 endpoint' in str(except_info.value)

    def test_download_from_bucket_unknown_error(self, s3, mock_conn):
        mock_conn.download_file.side_effect = ClientError(
            error_response={'Error': {'Code': 'UnknownError'}},
            operation_name='TestDownloadFromBucket'
        )
        with pytest.raises(S3ConnectionError) as except_info:
            s3.download_from_bucket(TEST_BUCKET_NAME, TEST_KEY_NAME)
        assert 'Unknown error' in str(except_info.value)

    # I'LL STOP WRITING TESTS FOR EVERYTHING FROM NOW ON, OUR HANDLER WORKS
    # I'LL WRITE HAPPY PATH AND MORE SPECIFIC TESTS

    def test_delete_from_bucket_success(self, s3, mock_conn):
        mock_conn.delete_object.return_value = {'ResponseMetadata': {}}
        result = s3.delete_from_bucket(TEST_BUCKET_NAME, TEST_KEY_NAME)

        mock_conn.delete_object.assert_called_once_with(
            Bucket=TEST_BUCKET_NAME,
            Key=TEST_KEY_NAME
        )
        assert result is not None

    def test_close_success(self, s3, mock_conn):
        s3.close()
        s3.conn = None
        s3.close()

    def test_copy_file_part_success(self, s3, mock_conn):
        mock_conn.head_object.return_value = {'ContentLength': 200 * (1024 ** 2)}
        mock_conn.upload_part_copy.return_value = {
            'CopyPartResult': {
                'ETag': 'test-etag'
            }
        }
        test_byte_range = f'bytes={TEST_BYTE_OFFSET}-{TEST_BYTE_AMOUNT_MB * (1024 ** 2) + TEST_BYTE_OFFSET - 1}'
        result = s3.copy_file_part(
            TEST_UPLOAD_ID,
            'test-src-bucket',
            'test-src-key',
            'test-dst-bucket',
            'test-dst-key',
            TEST_BYTE_AMOUNT_MB,
            TEST_BYTE_OFFSET,
            2
        )

        mock_conn.upload_part_copy.assert_called_once_with(
            UploadId=TEST_UPLOAD_ID,
            Bucket='test-dst-bucket',
            Key='test-dst-key',
            PartNumber=2,
            CopySource={'Bucket': 'test-src-bucket', 'Key': 'test-src-key'},
            CopySourceRange=test_byte_range
        )
        assert result == {
            'CopyPartResult': {
                'ETag': 'test-etag'
            }
        }

    def test_copy_file_part_without_optional_args(self, s3, mock_conn):
        mock_conn.head_object.return_value = {'ContentLength': 200 * (1024 ** 2)}
        mock_conn.upload_part_copy.return_value = {
            'CopyPartResult': {
                'ETag': 'test-etag'
            }
        }
        test_byte_range = f'bytes={0}-{64 * (1024 ** 2) - 1}'
        result = s3.copy_file_part(
            TEST_UPLOAD_ID,
            'test-src-bucket',
            'test-src-key',
            'test-dst-bucket',
            'test-dst-key',
        )

        mock_conn.upload_part_copy.assert_called_once_with(
            UploadId=TEST_UPLOAD_ID,
            Bucket='test-dst-bucket',
            Key='test-dst-key',
            PartNumber=1,
            CopySource={'Bucket': 'test-src-bucket', 'Key': 'test-src-key'},
            CopySourceRange=test_byte_range
        )
        assert result == {
            'CopyPartResult': {
                'ETag': 'test-etag'
            }
        }

    def test_copy_file_part_range_smaller_than_file(self, s3, mock_conn):
        mock_conn.head_object.return_value = {'ContentLength': 10 * (1024 ** 2)}
        mock_conn.upload_part_copy.return_value = {
            'CopyPartResult': {
                'ETag': 'test-etag'
            }
        }
        test_byte_range = f'bytes={TEST_BYTE_OFFSET}-{10 * (1024 ** 2) - 1}'
        result = s3.copy_file_part(
            TEST_UPLOAD_ID,
            'test-src-bucket',
            'test-src-key',
            'test-dst-bucket',
            'test-dst-key',
            TEST_BYTE_AMOUNT_MB,
            TEST_BYTE_OFFSET,
            2
        )

        mock_conn.upload_part_copy.assert_called_once_with(
            UploadId=TEST_UPLOAD_ID,
            Bucket='test-dst-bucket',
            Key='test-dst-key',
            PartNumber=2,
            CopySource={'Bucket': 'test-src-bucket', 'Key': 'test-src-key'},
            CopySourceRange=test_byte_range
        )
        assert result == {
            'CopyPartResult': {
                'ETag': 'test-etag'
            }
        }

    def test_copy_file_part_invalid_args(self, s3, mock_conn):
        with pytest.raises(S3ConnectionError) as except_info:
            s3.copy_file_part(
                TEST_UPLOAD_ID,
                'test-src-bucket',
                'test-src-key',
                'test-dst-bucket',
                'test-dst-key',
                0,
                TEST_BYTE_OFFSET,
                2
            )
        assert 'Byte amount must be at least 0' in str(except_info.value)

        with pytest.raises(S3ConnectionError) as except_info:
            s3.copy_file_part(
                TEST_UPLOAD_ID,
                'test-src-bucket',
                'test-src-key',
                'test-dst-bucket',
                'test-dst-key',
                TEST_BYTE_AMOUNT_MB,
                -1,
                2
            )
        assert 'Byte offset must be greater than 0' in str(except_info.value)

        with pytest.raises(S3ConnectionError) as except_info:
            s3.copy_file_part(
                TEST_UPLOAD_ID,
                'test-src-bucket',
                'test-src-key',
                'test-dst-bucket',
                'test-dst-key',
                TEST_BYTE_AMOUNT_MB,
                TEST_BYTE_OFFSET,
                20000
            )
        assert 'Invalid part number. Part number must be between 1 and 10000' in str(except_info.value)

        with pytest.raises(S3ConnectionError) as except_info:
            s3.copy_file_part(
                TEST_UPLOAD_ID,
                'test-src-bucket',
                'test-src-key',
                'test-dst-bucket',
                'test-dst-key',
                TEST_BYTE_AMOUNT_MB,
                TEST_BYTE_OFFSET,
                -1
            )
        assert 'Invalid part number. Part number must be between 1 and 10000' in str(except_info.value)

