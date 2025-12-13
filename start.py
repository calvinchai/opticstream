#! /autofs/space/aspasia_002/users/code/miniforge3/envs/prefect/bin/python

import prefect
from workflow.flows.tile_flow import process_tile_flow

process_tile_flow.serve(name="process_tile")
# process_tile_flow.deploy(name="process_tile", work_pool_name="local")
