# OCT Pipeline Workflow

Prefect-based workflow orchestration system for processing tiled OCT (Optical Coherence Tomography) scan data.

## Overview

This workflow processes spectral raw data through multiple stages:
1. **Tile Processing**: Spectral raw → Complex → 3D volumes (dBI, O3D, R3D) → Enface images
2. **Mosaic Processing**: Coordinate determination → Stitching of tiles
3. **Slice Processing**: Registration of normal and tilted illuminations
4. **Multi-Slice Processing**: Stacking of 2D images and 3D volumes

## Architecture

The workflow follows a hierarchical flow structure:

```
process_experiment_flow (main)
├── process_slice_flow (per slice)
│   ├── process_mosaic_flow (per mosaic, 2 per slice)
│   │   ├── process_tile_flow (per tile)
│   │   ├── determine_mosaic_coordinates_flow
│   │   └── stitch_mosaic_flow
│   └── register_slice_flow
└── stack_all_slices_flow
```

## Installation

1. Install Prefect:
```bash
pip install prefect
```

2. Install additional dependencies (if needed):
```bash
pip install pyyaml  # For YAML config support
```

3. Set up Prefect server or connect to Prefect Cloud:
```bash
# For local development
prefect server start

# Or connect to Prefect Cloud
prefect cloud login
```

## Configuration

1. Copy the example configuration file:
```bash
cp config.yaml.example config.yaml
```

2. Edit `config.yaml` with your settings:
- Data paths (data_root, output_base, compressed_base)
- Processing parameters (surface_method, depth, overlap, mask_threshold)
- Cloud upload settings (if using cloud storage)
- Slack notification settings (if using Slack)

See `config.yaml.example` for all available options.

## Usage

### Running a Workflow

#### Command Line

```bash
# Run workflow with config file
python -m workflow.main --config config.yaml

# Override paths from command line
python -m workflow.main --config config.yaml --data-root /path/to/data --output-base /path/to/output

# Process specific slices
python -m workflow.main --config config.yaml --slices 1 2 3

# Dry run (validate config without running)
python -m workflow.main --config config.yaml --dry-run
```

#### Python API

```python
from workflow import process_experiment_flow, load_config, get_slack_config_dict
from workflow.upload_queue import get_upload_queue_manager

# Load configuration
config = load_config("config.yaml")

# Initialize upload queue (if using cloud storage)
upload_queue = get_upload_queue_manager(
    max_concurrent=config.cloud.upload["max_concurrent"],
    cli_tool=config.cloud.upload["cli_tool"],
    cli_base_args=config.cloud.upload["cli_base_args"]
)

# Get Slack config
slack_config = get_slack_config_dict(config)

# Run workflow
result = process_experiment_flow(
    data_root_path=config.paths.data_root,
    output_base_path=config.paths.output_base,
    compressed_base_path=config.paths.compressed_base,
    surface_method=config.processing.surface_method,
    depth=config.processing.depth,
    mask_threshold=config.processing.mask_threshold,
    overlap=config.processing.overlap,
    upload_queue=upload_queue,
    slack_config=slack_config
)
```

### Creating a Deployment

To create a Prefect deployment for scheduled or API-triggered runs:

```bash
python -m workflow.main --config config.yaml --deploy --deployment-name oct-pipeline --work-pool default
```

With a schedule:
```bash
python -m workflow.main --config config.yaml --deploy --schedule "0 0 * * *"  # Daily at midnight
```

## Workflow Components

### Flows

- **`process_experiment_flow`**: Main entry point that orchestrates entire experiment
- **`process_slice_flow`**: Processes a single slice (both normal and tilted mosaics)
- **`process_mosaic_flow`**: Processes all tiles in a mosaic and stitches them
- **`process_tile_flow`**: Processes a single tile from spectral raw to volumes/enface
- **`determine_mosaic_coordinates_flow`**: Determines stitching coordinates
- **`stitch_mosaic_flow`**: Stitches tiles into a mosaic
- **`register_slice_flow`**: Registers normal and tilted illuminations
- **`stack_all_slices_flow`**: Stacks all processed slices

### Tasks

All processing tasks are defined in `tasks.py`. Key task categories:

- **Synchronous Processing Tasks**: Data conversion, volume generation, enface creation
- **Async Tasks**: Compression, cloud uploads, Slack notifications (non-blocking)
- **Utility Tasks**: File I/O, coordinate management, data collection

### Upload Queue Manager

The `UploadQueueManager` handles cloud uploads with concurrency control:
- Maximum 5 concurrent uploads (configurable)
- Uses CLI tools (aws s3 cp, gsutil, azcopy)
- Non-blocking queue-based architecture

## Data Organization

The workflow expects the following data structure:

```
Data Structure:
├── Slice N
│   ├── Mosaic 2N-1 (normal illumination)
│   │   └── Tile files (spectral raw)
│   └── Mosaic 2N (tilted illumination)
│       └── Tile files (spectral raw)
```

**Naming Convention**:
- Slice `n` has mosaics `mosaic_2n-1` (normal) and `mosaic_2n` (tilted)
- Example: Slice 1 → `mosaic_001` (normal), `mosaic_002` (tilted)

## Output Structure

Processed data is organized as follows:

```
{output_base_path}/
├── processed/          # Individual tile outputs
│   ├── {mosaic_id}_tile_{tile_index}_dBI.nii
│   ├── {mosaic_id}_tile_{tile_index}_aip.nii
│   └── ...
├── coordinates/        # Stitching coordinates
│   └── {mosaic_id}_coordinates.yaml
├── stitched/          # Stitched mosaics
│   ├── {mosaic_id}_dBI.nii
│   ├── {mosaic_id}_aip.nii
│   └── ...
├── registered/        # Registered slice data
│   ├── slice_{n}_orientation.nii
│   └── slice_{n}_3daxis.jpg
└── stacked/           # Final stacked outputs
    ├── all_slices_dBI.nii
    └── all_slices_aip.nii
```

Compressed spectral data is saved to `{compressed_base_path}/` (separate directory/disk).

## Async Task Design

The workflow uses async tasks for non-blocking operations:

1. **Compression**: Spectral raw files compressed to separate directory (fire-and-forget)
2. **Cloud Uploads**: Files queued for upload with concurrency control
3. **Slack Notifications**: Progress milestones and completion notifications

All async tasks use `allow_failure=True` to prevent workflow failures if they error.

## Monitoring

### Prefect UI

Monitor workflow runs in the Prefect UI:
- View flow run status
- Inspect task execution
- View logs and errors
- Track processing times

### Slack Notifications

If enabled, the workflow sends notifications at:
- 25%, 50%, 75%, 100% tile completion milestones
- Mosaic stitching completion (with stitched image)
- Individual tile completion (optional, can be disabled for high-volume)

## Implementation Notes

### Placeholder Functions

The workflow includes placeholder implementations for external data processing functions. You need to implement:

- Spectral raw loading and conversion
- Complex to volume conversion
- Surface finding algorithms
- Enface image generation
- Coordinate determination
- Stitching algorithms
- Registration algorithms
- Stacking algorithms

See `tasks.py` for TODO comments indicating where to add actual implementations.

### Error Handling

- Failed tiles: Logged and workflow continues with other tiles
- Failed mosaics: Alerted and slice processing may pause
- Failed slices: Alerted and workflow continues with other slices
- Failed stacking: Alerted and requires manual intervention

### Retry Configuration

Configure retries in Prefect task decorators:
- Tile processing: 3 retries with exponential backoff
- Stitching: 2 retries
- Registration: 2 retries
- Cloud upload: 5 retries (network issues)

## Development

### Project Structure

```
workflow/
├── __init__.py          # Package initialization
├── main.py              # CLI entry point
├── config.py            # Configuration management
├── tasks.py             # Task definitions
├── flows.py             # Flow definitions
├── upload_queue.py      # Upload queue manager
├── config.yaml.example  # Example configuration
└── README.md            # This file
```

### Adding New Tasks

1. Define task function in `tasks.py` with `@task` decorator
2. Add task to appropriate flow in `flows.py`
3. Update configuration if needed

### Testing

```bash
# Dry run to validate configuration
python -m workflow.main --config config.yaml --dry-run

# Test with small dataset
python -m workflow.main --config config.yaml --slices 1
```

## Troubleshooting

### Common Issues

1. **Config file not found**: Ensure `config.yaml` exists or use `--config` flag
2. **Path errors**: Check that all paths in config are valid and accessible
3. **Prefect connection**: Ensure Prefect server is running or connected to Prefect Cloud
4. **Upload failures**: Check cloud credentials and CLI tool availability
5. **Slack notifications not working**: Verify Slack webhook/token and channel settings

### Logging

Set log level for more detailed output:
```bash
python -m workflow.main --config config.yaml --log-level DEBUG
```

## License

See main project license.

## References

- [Prefect Documentation](https://docs.prefect.io/)
- [Design Document](DESIGN.md) - Detailed design specifications

