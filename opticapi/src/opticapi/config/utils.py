from __future__ import annotations

from pydantic import BaseModel
from niizarr import ZarrConfig


def with_positions(cls: type[BaseModel]) -> type[BaseModel]:
    cls.model_rebuild()
    for i, (name, field) in enumerate(cls.model_fields.items()):
        extra = dict(field.json_schema_extra or {})
        extra["position"] = i
        field.json_schema_extra = extra
    return cls


def default_zarr_config() -> ZarrConfig:
    return ZarrConfig(
        shard=(1024,),
        zarr_version=3,
        ome_zarr_version="0.5",
        overwrite=True,
        driver="tensorstore",
    )
