from __future__ import annotations

from niizarr import ZarrConfig


def default_zarr_config() -> ZarrConfig:
    return ZarrConfig(
        shard=(1024,),
        zarr_version=3,
        ome_zarr_version="0.5",
        overwrite=True,
        driver="tensorstore",
    )
