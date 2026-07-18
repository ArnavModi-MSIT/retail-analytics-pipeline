"""
Storage abstraction for pipeline I/O.
LocalBackend is the working implementation; S3Backend is a stub for later.
Callers depend only on the StorageBackend interface, never on the concrete class.
"""

from abc import ABC, abstractmethod
from pathlib import Path


class StorageBackend(ABC):
    @abstractmethod
    def write(self, df, path: str, fmt: str = "parquet", mode: str = "overwrite") -> None:
        ...

    @abstractmethod
    def read(self, spark, path: str, fmt: str = "parquet"):
        ...

    @abstractmethod
    def resolve_path(self, path: str) -> str:
        ...


class LocalBackend(StorageBackend):
    def __init__(self, root: str = "data"):
        self.root = Path(root)

    def resolve_path(self, path: str) -> str:
        return str(self.root / path)

    def write(self, df, path: str, fmt: str = "parquet", mode: str = "overwrite") -> None:
        full_path = self.resolve_path(path)
        Path(full_path).parent.mkdir(parents=True, exist_ok=True)
        df.write.mode(mode).format(fmt).save(full_path)

    def read(self, spark, path: str, fmt: str = "parquet"):
        return spark.read.format(fmt).load(self.resolve_path(path))


class S3Backend(StorageBackend):
    """
    Stub — not implemented. Swap in once AWS setup is ready.
    Same interface as LocalBackend, so no caller code changes when activated.
    """

    def __init__(self, bucket: str, prefix: str = ""):
        self.bucket = bucket
        self.prefix = prefix

    def resolve_path(self, path: str) -> str:
        raise NotImplementedError("S3Backend not yet implemented")

    def write(self, df, path: str, fmt: str = "parquet", mode: str = "overwrite") -> None:
        raise NotImplementedError("S3Backend not yet implemented")

    def read(self, spark, path: str, fmt: str = "parquet"):
        raise NotImplementedError("S3Backend not yet implemented")


def get_backend(config: dict | None = None) -> StorageBackend:
    """
    Config-driven backend selection.
    config = {"backend": "local", "root": "data"}
    config = {"backend": "s3", "bucket": "...", "prefix": "..."}
    """
    config = config or {"backend": "local", "root": "data"}

    if config["backend"] == "local":
        return LocalBackend(root=config.get("root", "data"))
    elif config["backend"] == "s3":
        return S3Backend(bucket=config["bucket"], prefix=config.get("prefix", ""))
    else:
        raise ValueError(f"Unknown backend: {config['backend']}")
