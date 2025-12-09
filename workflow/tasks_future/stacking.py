"""
Tasks for stacking slices.
"""

import logging
from pathlib import Path
from typing import Dict, List, Any
from prefect import task
from prefect.results import MaterializationResult

logger = logging.getLogger(__name__)


@task(name="collect_slice_data")
def collect_slice_data_task(
    slice_numbers: List[int],
    output_base_path: str
) -> Dict[str, List[str]]:
    """
    Collect all slice data paths.
    
    Parameters
    ----------
    slice_numbers : List[int]
        List of slice numbers
    output_base_path : str
        Base path for output files
    
    Returns
    -------
    Dict[str, List[str]]
        Dictionary with lists of paths for each modality
    """
    logger.info(f"Collecting slice data for {len(slice_numbers)} slices")
    
    output_dir = Path(output_base_path) / "registered"
    slice_data = {
        "orientation": [],
        "3daxis": []
    }
    
    for slice_num in slice_numbers:
        orientation_path = output_dir / f"slice_{slice_num}_orientation.nii"
        axis_path = output_dir / f"slice_{slice_num}_3daxis.jpg"
        
        if orientation_path.exists():
            slice_data["orientation"].append(str(orientation_path))
        if axis_path.exists():
            slice_data["3daxis"].append(str(axis_path))
    
    return slice_data


@task(name="stack_2d_images")
def stack_2d_images_task(
    slice_data: Dict[str, List[str]]
) -> Dict[str, Any]:
    """
    Stack all 2D enface images.
    
    Parameters
    ----------
    slice_data : Dict[str, List[str]]
        Dictionary with lists of paths for each modality
    
    Returns
    -------
    Dict[str, Any]
        Dictionary with stacked 2D images
    """
    logger.info("Stacking 2D images")
    # TODO: Implement actual stacking
    # from oct_pipe.stack import stack_2d
    # return stack_2d(slice_data)
    return {}  # Placeholder


@task(name="stack_3d_volumes")
def stack_3d_volumes_task(
    slice_data: Dict[str, List[str]]
) -> Dict[str, Any]:
    """
    Stack all 3D volumes.
    
    Parameters
    ----------
    slice_data : Dict[str, List[str]]
        Dictionary with lists of paths for each modality
    
    Returns
    -------
    Dict[str, Any]
        Dictionary with stacked 3D volumes
    """
    logger.info("Stacking 3D volumes")
    # TODO: Implement actual stacking
    # from oct_pipe.stack import stack_3d
    # return stack_3d(slice_data)
    return {}  # Placeholder


@task(name="save_stacked_data")
def save_stacked_data_task(
    stacked_2d: Dict[str, Any],
    stacked_3d: Dict[str, Any],
    output_base_path: str
) -> Dict[str, MaterializationResult]:
    """
    Save stacked outputs as Prefect assets.
    
    Parameters
    ----------
    stacked_2d : Dict[str, Any]
        Stacked 2D images
    stacked_3d : Dict[str, Any]
        Stacked 3D volumes
    output_base_path : str
        Base path for output files
    
    Returns
    -------
    Dict[str, MaterializationResult]
        Dictionary with MaterializationResult assets for stacked files
    """
    logger.info("Saving stacked data")
    
    output_dir = Path(output_base_path) / "stacked"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # TODO: Implement actual saving
    stacked_assets = {}
    for modality in ["aip", "mip", "orientation", "retardance", "birefringence"]:
        output_path = output_dir / f"all_slices_{modality}.nii"
        asset_key = f"all_slices_{modality}"
        asset = MaterializationResult(
            asset_key=asset_key,
            description=f"Stacked {modality.upper()} enface image across all slices",
            metadata={"path": str(output_path), "modality": modality, "type": "stacked"}
        )
        stacked_assets[modality] = asset
    
    for modality in ["dBI", "O3D", "R3D"]:
        output_path = output_dir / f"all_slices_{modality}.nii"
        asset_key = f"all_slices_{modality.lower()}"
        asset = MaterializationResult(
            asset_key=asset_key,
            description=f"Stacked {modality} volume across all slices",
            metadata={"path": str(output_path), "modality": modality, "type": "stacked"}
        )
        stacked_assets[modality] = asset
    
    return stacked_assets

