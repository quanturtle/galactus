import aioboto3


class S3BlobStore:
    """BlobStore implementation backed by aioboto3.

    Concrete put/get use S3's standard idempotent put-by-key semantics.
    """

    def __init__(self, *, bucket: str, region: str | None = None, prefix: str = "") -> None:
        self.bucket = bucket
        self.region = region
        self.prefix = prefix.rstrip("/")
        self._session = aioboto3.Session()

    def _key(self, key: str) -> str:
        return f"{self.prefix}/{key}" if self.prefix else key

    async def put(self, key: str, body: bytes, content_type: str) -> str:
        full = self._key(key)
        async with self._session.client("s3", region_name=self.region) as s3:
            await s3.put_object(Bucket=self.bucket, Key=full, Body=body, ContentType=content_type)
        return f"s3://{self.bucket}/{full}"

    async def get(self, key: str) -> bytes:
        full = self._key(key)
        async with self._session.client("s3", region_name=self.region) as s3:
            response = await s3.get_object(Bucket=self.bucket, Key=full)
            async with response["Body"] as stream:
                return await stream.read()
