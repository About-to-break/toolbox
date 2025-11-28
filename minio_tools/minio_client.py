import logging

from minio import Minio

class MinioClient:
    def __init__(self, logger: logging.Logger, endpoint: str, access_key: str, secret_key: str, bucket_name: str, secure: str):
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.bucket_name = bucket_name
        self.secure = secure
        self.l = logger
        self.client = None

        self._init_client()


    def _init_client(self):
        try:
            self.client = Minio(
                endpoint=self.endpoint,
                access_key=self.access_key,
                secret_key=self.secret_key,
                secure=bool(self.secure),
                region="us-east-1"
            )
            self.l.info('Initialized Minio client')

        except Exception as e:
            self.l.error(f'Minio client initialization failed {e}')

    def download_file(self, file_path: str):
        try:
            response = self.client.get_object(
                bucket_name=self.bucket_name,
                object_name=file_path,
            )
            data = response.read()

            response.close()
            response.release_conn()

            self.l.debug(f'Downloaded file {file_path} from Minio server')

            return data

        except Exception as e:
            self.l.error(f'Minio client download file {file_path} failed {e}')

            return None

    def upload_file(self, file_path: str):
        try:
            self.client.fput_object(
                bucket_name=self.bucket_name,
                object_name=file_path,
            )
            self.l.debug(f'Uploaded file {file_path} to Minio server')
        except Exception as e:
            self.l.error(f'Minio client upload file {file_path} failed {e}')





