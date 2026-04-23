"""Async S3/MinIO client used by the image downloader."""

import aioboto3


class S3ImageStore:
    """Async context manager wrapping an aioboto3 S3 client.

    Usage::

        async with S3ImageStore(endpoint_url=..., access_key=..., ...) as store:
            await store.upload(bucket, key, body, content_type)
    """

    def __init__(
        self,
        *,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        region: str,
    ) -> None:
        self._endpoint_url = endpoint_url
        self._access_key = access_key
        self._secret_key = secret_key
        self._region = region
        self._session = aioboto3.Session()
        self._client_ctx = None
        self._client = None

    async def __aenter__(self) -> "S3ImageStore":
        self._client_ctx = self._session.client(
            "s3",
            endpoint_url=self._endpoint_url,
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret_key,
            region_name=self._region,
        )
        self._client = await self._client_ctx.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        assert self._client_ctx is not None
        await self._client_ctx.__aexit__(exc_type, exc, tb)
        self._client_ctx = None
        self._client = None

    async def upload(self, bucket: str, key: str, body: bytes, content_type: str) -> None:
        await self._client.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type)

    async def exists(self, bucket: str, key: str) -> bool:
        try:
            await self._client.head_object(Bucket=bucket, Key=key)
            return True
        except Exception:
            return False
