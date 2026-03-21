"""Tests for PSOCT flow helper utilities."""

from opticstream.flows.psoct.utils import oct_batch_ident, slice_id_for_mosaic_id


def test_oct_batch_ident_matches_slice_derivation() -> None:
    ident = oct_batch_ident("proj-a", mosaic_id=5, batch_id=2)
    assert ident.project_name == "proj-a"
    assert ident.mosaic_id == 5
    assert ident.batch_id == 2
    assert ident.slice_id == slice_id_for_mosaic_id(5)
