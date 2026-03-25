"""Tests for PSOCT flow helper utilities."""

from opticstream.flows.psoct.utils import (
    oct_batch_ident,
    processed_output_prefix,
    slice_id_for_mosaic_id,
)


def test_processed_output_prefix_matches_fiji_stem() -> None:
    assert processed_output_prefix(1, 3) == "mosaic_001_image_0003"


def test_oct_batch_ident_matches_slice_derivation() -> None:
    ident = oct_batch_ident("proj-a", mosaic_id=5, batch_id=2)
    assert ident.project_name == "proj-a"
    assert ident.mosaic_id == 5
    assert ident.batch_id == 2
    assert ident.slice_id == slice_id_for_mosaic_id(5)
