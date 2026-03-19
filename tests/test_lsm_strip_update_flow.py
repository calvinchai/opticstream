from opticstream.flows.lsm.update_artifacts import (build_project_strip_summary_rows,
                                                    build_slice_strip_matrix_rows)
from opticstream.state.lsm_project_state import (
    LSMChannelStateView,
    LSMProjectStateView,
    LSMSliceStateView,
    LSMStripStateView,
)
from opticstream.state.project_state_core import ProcessingState


def test_build_project_strip_summary_rows_counts_by_slice_and_channel():
    state = LSMProjectStateView(
        slices={
            1: LSMSliceStateView(
                slice_id=1,
                channels={
                    1: LSMChannelStateView(
                        slice_id=1,
                        channel_id=1,
                        strips={
                            1: LSMStripStateView(
                                slice_id=1,
                                channel_id=1,
                                strip_id=1,
                                compressed=True,
                                archived=False,
                                uploaded=True,
                                processing_state=ProcessingState.COMPLETED,
                            ),
                            2: LSMStripStateView(
                                slice_id=1,
                                channel_id=1,
                                strip_id=2,
                                compressed=True,
                                archived=True,
                                uploaded=False,
                                processing_state=ProcessingState.RUNNING,
                            ),
                        },
                    ),
                    2: LSMChannelStateView(
                        slice_id=1,
                        channel_id=2,
                        strips={
                            1: LSMStripStateView(
                                slice_id=1,
                                channel_id=2,
                                strip_id=1,
                                compressed=False,
                                archived=False,
                                uploaded=False,
                                processing_state=ProcessingState.PENDING,
                            )
                        },
                    ),
                },
            )
        }
    )

    rows, total_completed, total_slots = build_project_strip_summary_rows(
        state=state,
        strips_per_slice=3,
    )

    assert rows == [
        {
            "Slice": 1,
            "Channel": 1,
            "Total Strips": 3,
            "Compressed": 2,
            "Archived": 1,
            "Uploaded": 1,
            "Completed": 1,
            "Progress": "33.3%",
        },
        {
            "Slice": 1,
            "Channel": 2,
            "Total Strips": 3,
            "Compressed": 0,
            "Archived": 0,
            "Uploaded": 0,
            "Completed": 0,
            "Progress": "0.0%",
        },
    ]
    assert total_completed == 1
    assert total_slots == 6


def test_build_slice_strip_matrix_rows_includes_missing_strips_and_channels():
    slice_view = LSMSliceStateView(
        slice_id=2,
        channels={
            1: LSMChannelStateView(
                slice_id=2,
                channel_id=1,
                strips={
                    1: LSMStripStateView(
                        slice_id=2,
                        channel_id=1,
                        strip_id=1,
                        compressed=True,
                        archived=True,
                        uploaded=False,
                        processing_state=ProcessingState.RUNNING,
                    )
                },
            ),
            3: LSMChannelStateView(
                slice_id=2,
                channel_id=3,
                strips={
                    2: LSMStripStateView(
                        slice_id=2,
                        channel_id=3,
                        strip_id=2,
                        compressed=True,
                        archived=True,
                        uploaded=True,
                        processing_state=ProcessingState.COMPLETED,
                    )
                },
            ),
        },
    )

    rows = build_slice_strip_matrix_rows(slice_view=slice_view, strips_per_slice=3)

    assert rows == [
        {
            "Strip": 1,
            "Ch 1": "running | cY aY uN",
            "Ch 3": "pending | cN aN uN",
        },
        {
            "Strip": 2,
            "Ch 1": "pending | cN aN uN",
            "Ch 3": "completed | cY aY uY",
        },
        {
            "Strip": 3,
            "Ch 1": "pending | cN aN uN",
            "Ch 3": "pending | cN aN uN",
        },
    ]
