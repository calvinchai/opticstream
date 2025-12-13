# Tile Batch Event-Driven Flows Setup Guide

This guide explains how to set up event-driven flows for tile batch processing using Prefect 3.0's `DeploymentEventTrigger`.

Based on Prefect 3.0 documentation:
- [Passing event payloads to flows](https://docs-3.prefect.io/v3/how-to-guides/automations/passing-event-payloads-to-flows)
- [Chaining deployments with events](https://docs-3.prefect.io/v3/how-to-guides/automations/chaining-deployments-with-events)

## Overview

The tile batch processing workflow uses Prefect events to trigger downstream flows:

1. **process_tile_batch_flow** - Main flow that:
   - Runs archive and spectral2complex tasks in parallel
   - Emits events to trigger downstream flows

2. **complex_to_processed_batch_event_flow** - Triggered by `tile_batch.complex2processed.ready` event:
   - Runs complex_to_processed_batch_task
   - Checks if all batches in mosaic are processed
   - Emits `mosaic.processed` event if all batches complete

3. **upload_to_linc_batch_event_flow** - Triggered by `tile_batch.upload_to_linc.ready` event:
   - Runs upload_to_linc_batch_task

## Events

### Emitted Events

- `tile_batch.complex2processed.ready` - Emitted after archive and spectral2complex complete
  - Payload: `{project_name, project_base_path, mosaic_id, batch_id}`
  
- `tile_batch.upload_to_linc.ready` - Emitted after archive and spectral2complex complete
  - Payload: `{project_name, project_base_path, mosaic_id, batch_id}`
  
- `mosaic.processed` - Emitted when all batches in a mosaic are processed
  - Payload: `{project_name, project_base_path, mosaic_id, total_batches}`

## Deployment

### Step 1: Create Deployments with DeploymentEventTrigger

The deployments are configured with `DeploymentEventTrigger` directly in Python, which automatically sets up event-driven triggering. No manual automation setup in the UI is required.

Run the deployment script:

```bash
python deploy_tile_batch_flows.py
```

Or use the apply method (one-time deployment):

```bash
python deploy_tile_batch_flows.py --method apply
```

This creates two deployments with built-in event triggers:
- `complex_to_processed_batch_event_flow/complex_to_processed_batch`
  - Automatically triggers on `tile_batch.complex2processed.ready` event
  - Parameters are automatically extracted from event payload using Jinja templates
  
- `upload_to_linc_batch_event_flow/upload_to_linc_batch`
  - Automatically triggers on `tile_batch.upload_to_linc.ready` event
  - Parameters are automatically extracted from event payload using Jinja templates

### How It Works

The deployments use `DeploymentEventTrigger` with Jinja template parameter mapping:

```python
DeploymentEventTrigger(
    expect={"tile_batch.complex2processed.ready"},
    parameters={
        "project_name": {
            "__prefect_kind": "jinja",
            "template": "{{ event.payload.project_name }}",
        },
        "mosaic_id": {
            "__prefect_kind": "jinja",
            "template": "{{ event.payload.mosaic_id }}",
        },
        # ... other parameters
    },
)
```

This automatically:
1. Listens for the specified event
2. Extracts parameters from the event payload
3. Triggers the deployment with those parameters

### Step 2: Optional - Mosaic Processing Flow

If you have a mosaic-level processing flow, you can create a deployment with a trigger for the `mosaic.processed` event:

```python
from prefect.events import DeploymentEventTrigger

mosaic_deployment = your_mosaic_flow.to_deployment(
    name="process_mosaic",
    triggers=[
        DeploymentEventTrigger(
            expect={"mosaic.processed"},
            parameters={
                "project_name": {
                    "__prefect_kind": "jinja",
                    "template": "{{ event.payload.project_name }}",
                },
                "mosaic_id": {
                    "__prefect_kind": "jinja",
                    "template": "{{ event.payload.mosaic_id }}",
                },
                # ... other parameters
            },
        )
    ],
)
```

## State Files

The flows use state files to track batch progress:

- `batch-{batch_id}.started` - Batch processing started
- `batch-{batch_id}.archived` - Batch archived
- `batch-{batch_id}.complex` - Batch converted to complex
- `batch-{batch_id}.processed` - Batch processed
- `batch-{batch_id}.uploaded` - Batch uploaded

State files are located at: `{project_base_path}/mosaic-{mosaic_id}/state/`

## Testing

To test the event-driven flows:

1. Start the deployment serve process:
   ```bash
   python deploy_tile_batch_flows.py
   ```
   Note the deployment IDs from the output.

2. Run `process_tile_batch_flow` with test parameters, which will emit events.

3. Alternatively, test by emitting events directly:
   ```bash
   prefect event emit tile_batch.complex2processed.ready \
     --resource-id batch:test-project:mosaic-1:batch-1 \
     --payload '{
       "project_name": "test-project",
       "project_base_path": "/path/to/project",
       "mosaic_id": 1,
       "batch_id": 1
     }'
   ```

4. Check that events are emitted in Prefect UI → Events

5. Verify that deployments automatically trigger (check Flow Runs in UI)

6. Check state files to confirm batch progress

## Event Targeting (Advanced)

When using `DeploymentEventTrigger`, events can optionally target the deployment resource ID for more precise matching. To target a specific deployment:

1. Get the deployment ID from the serve output or with:
   ```bash
   prefect deployment ls
   ```

2. When emitting events, target the deployment:
   ```python
   emit_event(
       event="tile_batch.complex2processed.ready",
       resource={
           "prefect.resource.id": f"prefect.deployment.{deployment_id}",
       },
       payload={...}
   )
   ```

However, matching on event name (as configured) works without targeting specific deployments.

## Troubleshooting

### Events not triggering flows

- Verify deployments are running (check serve process or deployment status)
- Check event names match exactly (case-sensitive) - should be `tile_batch.complex2processed.ready`
- Verify parameter types match (e.g., `mosaic_id` and `batch_id` should be integers in payload)
- Check Prefect logs for errors
- View events in Prefect UI → Events to verify they're being emitted correctly

### All batches not detected

- Ensure all batches have `batch-{batch_id}.started` files
- Check that state directory exists and is accessible
- Verify batch IDs are consistent

### Duplicate processing

- State files prevent duplicate processing
- If needed, delete state files to reprocess

### Parameter type errors

- Ensure `mosaic_id` and `batch_id` in event payload are integers, not strings
- The Jinja templates will pass them as-is, so the payload type matters
