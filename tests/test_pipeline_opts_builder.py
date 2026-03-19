"""Tests for PipelineOpts construction from PS-OCT scan config."""

import math

import pytest
from niizarr import ZarrConfig

from opticstream.config.pipeline_opts_builder import build_pipeline_opts
from opticstream.config.psoct_scan_config import (
    PSOCTAcquisitionParams,
    PSOCTProcessingParams,
    PSOCTScanConfigModel,
    TileSavingType,
)


def _minimal_config(**kwargs) -> PSOCTScanConfigModel:
    base = dict(
        project_name="t",
        project_base_path="/tmp",
        acquisition=PSOCTAcquisitionParams(
            grid_size_x_normal=1,
            grid_size_x_tilted=2,
            grid_size_y=3,
        ),
        zarr_config=ZarrConfig(),
    )
    base.update(kwargs)
    return PSOCTScanConfigModel(**base)


def test_build_pipeline_opts_spectral_and_pixel_scale() -> None:
    cfg = _minimal_config(
        acquisition=PSOCTAcquisitionParams(
            grid_size_x_normal=1,
            grid_size_x_tilted=2,
            grid_size_y=3,
            tile_size_x_normal=100,
            tile_size_x_tilted=50,
            tile_size_y=200,
            scan_resolution_3d=[0.02, 0.02, 0.005],
            tile_saving_type=TileSavingType.SPECTRAL,
            wavelength_um=1.31,
            slice_thickness_um=400.0,
        ),
    )
    po = build_pipeline_opts(cfg, illumination="normal")
    assert po.spectral is not None
    assert po.spectral.aline_size == 100
    assert po.spectral.bline_size == 200
    assert po.spectral.is_raw_format is True
    po_t = build_pipeline_opts(cfg, illumination="tilted")
    assert po_t.spectral.aline_size == 50

    assert po.acquisition is not None
    assert po.acquisition.pixel_dimensions_um == [20.0, 20.0, 5.0]
    assert po.acquisition.wavelength_um == 1.31
    assert po.acquisition.slice_thickness_um == 400.0


def test_phase_offset_deg_to_radians() -> None:
    cfg = _minimal_config(
        processing=PSOCTProcessingParams(phase_offset_deg=180.0),
    )
    po = build_pipeline_opts(cfg)
    assert po.volume is not None
    assert po.volume.phase_offset == pytest.approx(math.pi)


def test_enface_compute_from_modalities() -> None:
    cfg = _minimal_config(enface_modalities=["ret", "mip"])
    po = build_pipeline_opts(cfg)
    assert po.enface is not None
    c = po.enface.compute
    assert c.ret is True
    assert c.mip is True
    assert c.ori is False
    assert c.biref is False
