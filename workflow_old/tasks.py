"""
Prefect task definitions for OCT pipeline workflow.

All data processing tasks are placeholders that should be implemented
with actual processing functions.
"""

import os
import gzip
import shutil
import logging
from pathlib import Path
from typing import Dict, List, Optional, Union, Tuple, Any
from prefect import task
from prefect.tasks import task_input_hash
from prefect.results import MaterializationResult

logger = logging.getLogger(__name__)


# ============================================================================
# Helper Functions
# ============================================================================

def extract_path_from_asset(asset: MaterializationResult) -> str:
    """
    Extract file path from MaterializationResult asset.
    
    Parameters
    ----------
    asset : MaterializationResult
        Asset containing file metadata
    
    Returns
    -------
    str
        File path from asset metadata
    """
    return asset.metadata.get("path", "")


def extract_paths_from_assets(assets: Dict[str, MaterializationResult]) -> Dict[str, str]:
    """
    Extract file paths from dictionary of MaterializationResult assets.
    
    Parameters
    ----------
    assets : Dict[str, MaterializationResult]
        Dictionary of assets
    
    Returns
    -------
    Dict[str, str]
        Dictionary of file paths
    """
    return {k: extract_path_from_asset(v) for k, v in assets.items()}


# ============================================================================
# Synchronous Data Processing Tasks
# ============================================================================

@task(name="load_spectral_raw", cache_key_fn=task_input_hash)
def load_spectral_raw_task(tile_path: str) -> Any:
    """
    Load spectral raw data file.
    
    Parameters
    ----------
    tile_path : str
        Path to spectral raw tile file
    
    Returns
    -------
    Any
        Spectral raw data (placeholder - implement with actual loader)
    """
    logger.info(f"Loading spectral raw from {tile_path}")
    # TODO: Implement actual spectral raw loading
    # from oct_pipe.spectral_raw import load_spectral_file
    # return load_spectral_file(tile_path)
    return {"path": tile_path, "data": None}  # Placeholder


@task(name="spectral_to_complex")
def spectral_to_complex_task(spectral_data: Any) -> Any:
    """
    Convert spectral raw to complex data (in-memory).
    
    Parameters
    ----------
    spectral_data : Any
        Spectral raw data
    
    Returns
    -------
    Any
        Complex data (placeholder - implement with actual conversion)
    """
    logger.info("Converting spectral raw to complex data")
    # TODO: Implement actual conversion
    # from oct_pipe.spectral_raw.spectral2complex import spectral2complex
    # return spectral2complex(spectral_data)
    return {"complex": None}  # Placeholder


@task(name="complex_to_volumes")
def complex_to_volumes_task(complex_data: Any) -> Dict[str, Any]:
    """
    Convert complex to 3D volumes (dBI, O3D, R3D).
    
    Parameters
    ----------
    complex_data : Any
        Complex data
    
    Returns
    -------
    Dict[str, Any]
        Dictionary with keys: dBI, O3D, R3D
    """
    logger.info("Converting complex to 3D volumes")
    # TODO: Implement actual conversion
    # from oct_pipe.volume_3d.complex2vol import process_complex3d
    # dBI, O3D, R3D = process_complex3d(complex_data)
    # return {"dBI": dBI, "O3D": O3D, "R3D": R3D}
    return {"dBI": None, "O3D": None, "R3D": None}  # Placeholder


@task(name="find_surface")
def find_surface_task(
    volumes: Dict[str, Any],
    method: Union[str, int, str] = "find"
) -> Any:
    """
    Surface finding algorithm for enface conversion.
    
    Parameters
    ----------
    volumes : Dict[str, Any]
        Dictionary with volume data (dBI, O3D, R3D)
    method : Union[str, int, str]
        Surface finding method ("find", constant, or file path)
    
    Returns
    -------
    Any
        Surface data (placeholder - implement with actual algorithm)
    """
    logger.info(f"Finding surface using method: {method}")
    # TODO: Implement actual surface finding
    # from oct_pipe.enface import find_surface
    # return find_surface(volumes["dBI"], method=method)
    return {"surface": None}  # Placeholder


@task(name="volumes_to_enface")
def volumes_to_enface_task(
    volumes: Dict[str, Any],
    surface: Any,
    depth: int = 80
) -> Dict[str, Any]:
    """
    Generate enface images from volumes.
    
    Parameters
    ----------
    volumes : Dict[str, Any]
        Dictionary with volume data (dBI, O3D, R3D)
    surface : Any
        Surface data
    depth : int
        Depth below surface for enface window
    
    Returns
    -------
    Dict[str, Any]
        Dictionary with keys: aip, mip, orientation, retardance, birefringence
    """
    logger.info(f"Generating enface images with depth={depth}")
    # TODO: Implement actual enface generation
    # from oct_pipe.enface.vol2enface import EnfaceVolume
    # enface = EnfaceVolume(
    #     volumes["dBI"],
    #     volumes["R3D"],
    #     volumes["O3D"],
    #     surface=surface,
    #     depth=depth
    # )
    # return {
    #     "aip": enface.aip,
    #     "mip": enface.mip,
    #     "orientation": enface.orientation,
    #     "retardance": enface.retardance,
    #     "birefringence": enface.birefringence
    # }
    return {
        "aip": None,
        "mip": None,
        "orientation": None,
        "retardance": None,
        "birefringence": None
    }  # Placeholder


@task(name="save_volumes")
def save_volumes_task(
    volumes: Dict[str, Any],
    output_base_path: str,
    mosaic_id: str,
    tile_index: int
) -> Dict[str, MaterializationResult]:
    """
    Save 3D volumes to disk as Prefect assets.
    
    Parameters
    ----------
    volumes : Dict[str, Any]
        Dictionary with volume data (dBI, O3D, R3D)
    output_base_path : str
        Base path for output files
    mosaic_id : str
        Mosaic identifier
    tile_index : int
        Tile index within mosaic
    
    Returns
    -------
    Dict[str, MaterializationResult]
        Dictionary with MaterializationResult assets for each volume file
    """
    logger.info(f"Saving volumes for {mosaic_id} tile {tile_index}")
    
    output_dir = Path(output_base_path) / "processed"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    volume_assets = {}
    for modality in ["dBI", "O3D", "R3D"]:
        output_path = output_dir / f"{mosaic_id}_tile_{tile_index}_{modality}.nii"
        # TODO: Implement actual saving
        # import nibabel as nib
        # nib.save(nib.Nifti1Image(volumes[modality], affine), output_path)
        
        # Create asset with unique key
        asset_key = f"{mosaic_id}_tile_{tile_index}_{modality.lower()}"
        asset = MaterializationResult(
            asset_key=asset_key,
            description=f"{modality} volume for {mosaic_id} tile {tile_index}",
            metadata={"path": str(output_path), "modality": modality, "mosaic_id": mosaic_id, "tile_index": tile_index}
        )
        volume_assets[modality] = asset
        logger.debug(f"Created asset {asset_key} for {output_path}")
    
    return volume_assets


@task(name="save_enface")
def save_enface_task(
    enface_images: Dict[str, Any],
    output_base_path: str,
    mosaic_id: str,
    tile_index: int
) -> Dict[str, MaterializationResult]:
    """
    Save enface images to disk as Prefect assets.
    
    Parameters
    ----------
    enface_images : Dict[str, Any]
        Dictionary with enface images (aip, mip, orientation, retardance, birefringence)
    output_base_path : str
        Base path for output files
    mosaic_id : str
        Mosaic identifier
    tile_index : int
        Tile index within mosaic
    
    Returns
    -------
    Dict[str, MaterializationResult]
        Dictionary with MaterializationResult assets for each enface file
    """
    logger.info(f"Saving enface images for {mosaic_id} tile {tile_index}")
    
    output_dir = Path(output_base_path) / "processed"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    enface_assets = {}
    for modality in ["aip", "mip", "orientation", "retardance", "birefringence"]:
        output_path = output_dir / f"{mosaic_id}_tile_{tile_index}_{modality}.nii"
        # TODO: Implement actual saving
        # import nibabel as nib
        # nib.save(nib.Nifti1Image(enface_images[modality], affine), output_path)
        
        # Create asset with unique key
        asset_key = f"{mosaic_id}_tile_{tile_index}_{modality}"
        asset = MaterializationResult(
            asset_key=asset_key,
            description=f"{modality.upper()} enface image for {mosaic_id} tile {tile_index}",
            metadata={"path": str(output_path), "modality": modality, "mosaic_id": mosaic_id, "tile_index": tile_index}
        )
        enface_assets[modality] = asset
        logger.debug(f"Created asset {asset_key} for {output_path}")
    
    return enface_assets


# ============================================================================
# Mosaic Coordinate Determination Tasks
# ============================================================================

@task(name="collect_tile_aip_images")
def collect_tile_aip_images_task(
    mosaic_id: str,
    tile_paths: List[str],
    output_base_path: str
) -> List[str]:
    """
    Collect all AIP enface images for the mosaic.
    
    Parameters
    ----------
    mosaic_id : str
        Mosaic identifier
    tile_paths : List[str]
        List of processed tile paths
    output_base_path : str
        Base path for output files
    
    Returns
    -------
    List[str]
        List of paths to AIP images
    """
    logger.info(f"Collecting AIP images for {mosaic_id}")
    
    output_dir = Path(output_base_path) / "processed"
    aip_paths = []
    
    for i, tile_path in enumerate(tile_paths):
        aip_path = output_dir / f"{mosaic_id}_tile_{i}_aip.nii"
        if aip_path.exists():
            aip_paths.append(str(aip_path))
        else:
            logger.warning(f"AIP image not found: {aip_path}")
    
    return aip_paths


@task(name="determine_tile_coordinates")
def determine_tile_coordinates_task(
    mosaic_id: str,
    aip_paths: List[str],
    ideal_coord_file: Optional[str] = None
) -> Dict[str, Tuple[int, int]]:
    """
    Run coordinate determination script using AIP images.
    
    Parameters
    ----------
    mosaic_id : str
        Mosaic identifier
    aip_paths : List[str]
        List of paths to AIP images
    ideal_coord_file : str, optional
        Path to ideal coordinate file (if available)
    
    Returns
    -------
    Dict[str, Tuple[int, int]]
        Dictionary mapping tile paths to coordinates
    """
    logger.info(f"Determining coordinates for {mosaic_id}")
    # TODO: Implement actual coordinate determination
    # from oct_pipe.stitch.process_tile_coord import determine_coordinates
    # return determine_coordinates(aip_paths, ideal_coord_file)
    return {}  # Placeholder


@task(name="save_coordinates")
def save_coordinates_task(
    coordinates: Dict[str, Tuple[int, int]],
    output_coord_file: str,
    mosaic_id: str
) -> MaterializationResult:
    """
    Save coordinates to YAML/JSON file as Prefect asset.
    
    Parameters
    ----------
    coordinates : Dict[str, Tuple[int, int]]
        Dictionary mapping tile paths to coordinates
    output_coord_file : str
        Path to save coordinates file
    mosaic_id : str
        Mosaic identifier
    
    Returns
    -------
    MaterializationResult
        Asset for the coordinates file
    """
    logger.info(f"Saving coordinates to {output_coord_file}")
    
    output_path = Path(output_coord_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # TODO: Implement actual saving
    # import yaml
    # with open(output_path, 'w') as f:
    #     yaml.dump(coordinates, f)
    
    # Create asset
    asset_key = f"{mosaic_id}_coordinates"
    asset = MaterializationResult(
        asset_key=asset_key,
        description=f"Stitching coordinates for {mosaic_id}",
        metadata={"path": str(output_path), "mosaic_id": mosaic_id, "num_tiles": len(coordinates)}
    )
    logger.debug(f"Created asset {asset_key} for {output_path}")
    
    return asset


# ============================================================================
# Mosaic Stitching Tasks
# ============================================================================

@task(name="load_coordinates")
def load_coordinates_task(coordinate_file: str) -> Dict[str, Tuple[int, int]]:
    """
    Load coordinate file for mosaic.
    
    Parameters
    ----------
    coordinate_file : str
        Path to coordinate file
    
    Returns
    -------
    Dict[str, Tuple[int, int]]
        Dictionary mapping tile paths to coordinates
    """
    logger.info(f"Loading coordinates from {coordinate_file}")
    # TODO: Implement actual loading
    # import yaml
    # with open(coordinate_file, 'r') as f:
    #     return yaml.safe_load(f)
    return {}  # Placeholder


@task(name="create_mask_from_aip")
def create_mask_from_aip_task(
    stitched_aip: Any,
    mask_threshold: int = 55
) -> Any:
    """
    Create mask from stitched AIP (threshold-based).
    
    Parameters
    ----------
    stitched_aip : Any
        Stitched AIP image
    mask_threshold : int
        Threshold for mask creation
    
    Returns
    -------
    Any
        Mask array
    """
    logger.info(f"Creating mask with threshold={mask_threshold}")
    # TODO: Implement actual mask creation
    # mask = stitched_aip > mask_threshold
    # return mask
    return None  # Placeholder


@task(name="stitch_enface_images")
def stitch_enface_images_task(
    tile_paths: List[str],
    coordinates: Dict[str, Tuple[int, int]],
    overlap: int = 50
) -> Dict[str, Any]:
    """
    Stitch all enface modalities.
    
    Parameters
    ----------
    tile_paths : List[str]
        List of processed tile paths
    coordinates : Dict[str, Tuple[int, int]]
        Dictionary mapping tile paths to coordinates
    overlap : int
        Overlap between tiles in pixels
    
    Returns
    -------
    Dict[str, Any]
        Dictionary with stitched enface images
    """
    logger.info(f"Stitching enface images with overlap={overlap}")
    # TODO: Implement actual stitching
    # from oct_pipe.stitch.stitch2d import stitch_2d
    # stitched = {}
    # for modality in ["aip", "mip", "orientation", "retardance", "birefringence"]:
    #     modality_tiles = [load_modality_tile(t, modality) for t in tile_paths]
    #     stitched[modality] = stitch_2d(modality_tiles, overlap)
    return {}  # Placeholder


@task(name="stitch_3d_volumes")
def stitch_3d_volumes_task(
    tile_paths: List[str],
    coordinates: Dict[str, Tuple[int, int]],
    overlap: int = 50
) -> Dict[str, Any]:
    """
    Stitch 3D volumes.
    
    Parameters
    ----------
    tile_paths : List[str]
        List of processed tile paths
    coordinates : Dict[str, Tuple[int, int]]
        Dictionary mapping tile paths to coordinates
    overlap : int
        Overlap between tiles in pixels
    
    Returns
    -------
    Dict[str, Any]
        Dictionary with stitched 3D volumes
    """
    logger.info(f"Stitching 3D volumes with overlap={overlap}")
    # TODO: Implement actual stitching
    # from oct_pipe.stitch import stitch_3d
    # stitched = {}
    # for modality in ["dBI", "O3D", "R3D"]:
    #     modality_tiles = [load_modality_tile(t, modality) for t in tile_paths]
    #     stitched[modality] = stitch_3d(modality_tiles, overlap)
    return {}  # Placeholder


@task(name="apply_mask")
def apply_mask_task(
    data: Union[Dict[str, Any], Any],
    mask: Any
) -> Union[Dict[str, Any], Any]:
    """
    Apply mask to data.
    
    Parameters
    ----------
    data : Union[Dict[str, Any], Any]
        Data to mask (can be dict of modalities or single array)
    mask : Any
        Mask array
    
    Returns
    -------
    Union[Dict[str, Any], Any]
        Masked data
    """
    logger.info("Applying mask to data")
    # TODO: Implement actual masking
    # if isinstance(data, dict):
    #     return {k: v * mask for k, v in data.items()}
    # else:
    #     return data * mask
    return data  # Placeholder


@task(name="save_stitched_enface")
def save_stitched_enface_task(
    stitched_enface: Dict[str, Any],
    output_base_path: str,
    mosaic_id: str
) -> Dict[str, MaterializationResult]:
    """
    Save stitched enface images as Prefect assets.
    
    Parameters
    ----------
    stitched_enface : Dict[str, Any]
        Dictionary with stitched enface images
    output_base_path : str
        Base path for output files
    mosaic_id : str
        Mosaic identifier
    
    Returns
    -------
    Dict[str, MaterializationResult]
        Dictionary with MaterializationResult assets for each stitched enface file
    """
    logger.info(f"Saving stitched enface images for {mosaic_id}")
    
    output_dir = Path(output_base_path) / "stitched"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    enface_assets = {}
    for modality in ["aip", "mip", "orientation", "retardance", "birefringence"]:
        output_path = output_dir / f"{mosaic_id}_{modality}.nii"
        # TODO: Implement actual saving
        
        # Create asset
        asset_key = f"{mosaic_id}_stitched_{modality}"
        asset = MaterializationResult(
            asset_key=asset_key,
            description=f"Stitched {modality.upper()} enface image for {mosaic_id}",
            metadata={"path": str(output_path), "modality": modality, "mosaic_id": mosaic_id, "type": "stitched"}
        )
        enface_assets[modality] = asset
        logger.debug(f"Created asset {asset_key} for {output_path}")
    
    return enface_assets


@task(name="save_stitched_volumes")
def save_stitched_volumes_task(
    stitched_volumes: Dict[str, Any],
    output_base_path: str,
    mosaic_id: str
) -> Dict[str, MaterializationResult]:
    """
    Save stitched 3D volumes as Prefect assets.
    
    Parameters
    ----------
    stitched_volumes : Dict[str, Any]
        Dictionary with stitched 3D volumes
    output_base_path : str
        Base path for output files
    mosaic_id : str
        Mosaic identifier
    
    Returns
    -------
    Dict[str, MaterializationResult]
        Dictionary with MaterializationResult assets for each stitched volume file
    """
    logger.info(f"Saving stitched volumes for {mosaic_id}")
    
    output_dir = Path(output_base_path) / "stitched"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    volume_assets = {}
    for modality in ["dBI", "O3D", "R3D"]:
        output_path = output_dir / f"{mosaic_id}_{modality}.nii"
        # TODO: Implement actual saving
        
        # Create asset
        asset_key = f"{mosaic_id}_stitched_{modality.lower()}"
        asset = MaterializationResult(
            asset_key=asset_key,
            description=f"Stitched {modality} volume for {mosaic_id}",
            metadata={"path": str(output_path), "modality": modality, "mosaic_id": mosaic_id, "type": "stitched"}
        )
        volume_assets[modality] = asset
        logger.debug(f"Created asset {asset_key} for {output_path}")
    
    return volume_assets


# ============================================================================
# Slice Registration Tasks
# ============================================================================

@task(name="load_normal_mosaic")
def load_normal_mosaic_task(
    normal_mosaic_id: str,
    output_base_path: str
) -> Dict[str, Any]:
    """
    Load normal illumination mosaic data.
    
    Parameters
    ----------
    normal_mosaic_id : str
        Normal mosaic ID
    output_base_path : str
        Base path for output files
    
    Returns
    -------
    Dict[str, Any]
        Normal mosaic data
    """
    logger.info(f"Loading normal mosaic {normal_mosaic_id}")
    # TODO: Implement actual loading
    return {}  # Placeholder


@task(name="load_tilted_mosaic")
def load_tilted_mosaic_task(
    tilted_mosaic_id: str,
    output_base_path: str
) -> Dict[str, Any]:
    """
    Load tilted illumination mosaic data.
    
    Parameters
    ----------
    tilted_mosaic_id : str
        Tilted mosaic ID
    output_base_path : str
        Base path for output files
    
    Returns
    -------
    Dict[str, Any]
        Tilted mosaic data
    """
    logger.info(f"Loading tilted mosaic {tilted_mosaic_id}")
    # TODO: Implement actual loading
    return {}  # Placeholder


@task(name="register_orientations")
def register_orientations_task(
    normal_mosaic: Dict[str, Any],
    tilted_mosaic: Dict[str, Any],
    gamma: float,
    mask_file: Optional[str] = None,
    mask_threshold: int = 55
) -> Dict[str, Any]:
    """
    Perform registration to align orientations.
    
    Parameters
    ----------
    normal_mosaic : Dict[str, Any]
        Normal illumination mosaic data
    tilted_mosaic : Dict[str, Any]
        Tilted illumination mosaic data
    gamma : float
        Tilt angle parameter
    mask_file : str, optional
        Optional mask file for registration
    mask_threshold : int
        Threshold for mask
    
    Returns
    -------
    Dict[str, Any]
        Registered orientation data
    """
    logger.info(f"Registering orientations with gamma={gamma}")
    # TODO: Implement actual registration
    # from oct_pipe.registration import register_orientations
    # return register_orientations(normal_mosaic, tilted_mosaic, gamma, mask_file)
    return {}  # Placeholder


@task(name="compute_3d_orientation")
def compute_3d_orientation_task(
    registered_data: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Compute 3D orientation (thru-plane, in-plane, 3D axis).
    
    Parameters
    ----------
    registered_data : Dict[str, Any]
        Registered orientation data
    
    Returns
    -------
    Dict[str, Any]
        Dictionary with 3D orientation data
    """
    logger.info("Computing 3D orientation")
    # TODO: Implement actual computation
    # from oct_pipe.registration import compute_3d_orientation
    # return compute_3d_orientation(registered_data)
    return {}  # Placeholder


@task(name="save_registered_data")
def save_registered_data_task(
    registered_data: Dict[str, Any],
    orientation_data: Dict[str, Any],
    output_base_path: str,
    slice_number: int
) -> Dict[str, MaterializationResult]:
    """
    Save registered orientation data as Prefect assets.
    
    Parameters
    ----------
    registered_data : Dict[str, Any]
        Registered orientation data
    orientation_data : Dict[str, Any]
        3D orientation data
    output_base_path : str
        Base path for output files
    slice_number : int
        Slice number
    
    Returns
    -------
    Dict[str, MaterializationResult]
        Dictionary with MaterializationResult assets for registered files
    """
    logger.info(f"Saving registered data for slice {slice_number}")
    
    output_dir = Path(output_base_path) / "registered"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # TODO: Implement actual saving
    orientation_path = output_dir / f"slice_{slice_number}_orientation.nii"
    axis_path = output_dir / f"slice_{slice_number}_3daxis.jpg"
    
    # Create assets
    orientation_asset = MaterializationResult(
        asset_key=f"slice_{slice_number}_orientation",
        description=f"Registered orientation for slice {slice_number}",
        metadata={"path": str(orientation_path), "slice_number": slice_number, "type": "registered"}
    )
    
    axis_asset = MaterializationResult(
        asset_key=f"slice_{slice_number}_3daxis",
        description=f"3D axis visualization for slice {slice_number}",
        metadata={"path": str(axis_path), "slice_number": slice_number, "type": "visualization"}
    )
    
    return {
        "orientation": orientation_asset,
        "3daxis": axis_asset
    }


# ============================================================================
# Final Stacking Tasks
# ============================================================================

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


# ============================================================================
# Async Tasks (Compression, Upload, Notifications)
# ============================================================================

@task(name="compress_spectral", allow_failure=True)
def compress_spectral_task(
    tile_path: str,
    compressed_base_path: str,
    mosaic_id: str,
    tile_index: int
) -> MaterializationResult:
    """
    Compress spectral raw file to separate directory as Prefect asset (async, fire-and-forget).
    
    Parameters
    ----------
    tile_path : str
        Path to spectral raw tile file
    compressed_base_path : str
        Base path for compressed files (separate directory/disk)
    mosaic_id : str
        Mosaic identifier
    tile_index : int
        Tile index within mosaic
    
    Returns
    -------
    MaterializationResult
        Asset for the compressed file
    """
    logger.info(f"Compressing spectral raw for {mosaic_id} tile {tile_index}")
    
    # Create output directory if it doesn't exist
    os.makedirs(compressed_base_path, exist_ok=True)
    
    # Output path
    output_filename = f"{mosaic_id}_tile_{tile_index}_spectral.nii.gz"
    output_path = os.path.join(compressed_base_path, output_filename)
    
    # Compress file
    try:
        with open(tile_path, 'rb') as f_in:
            with gzip.open(output_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        logger.info(f"Compressed to {output_path}")
        
        # Create asset
        asset_key = f"{mosaic_id}_tile_{tile_index}_spectral_compressed"
        asset = MaterializationResult(
            asset_key=asset_key,
            description=f"Compressed spectral raw for {mosaic_id} tile {tile_index}",
            metadata={"path": output_path, "mosaic_id": mosaic_id, "tile_index": tile_index, "type": "compressed"}
        )
        return asset
    except Exception as e:
        logger.error(f"Compression failed: {e}")
        raise


@task(name="queue_upload_spectral", allow_failure=True)
def queue_upload_spectral_task(
    compressed_file_path: str,
    destination: str,
    upload_queue: Any
) -> None:
    """
    Queue compressed file for cloud upload (async, non-blocking).
    
    Parameters
    ----------
    compressed_file_path : str
        Path to compressed file
    destination : str
        Destination path/URL for upload
    upload_queue : Any
        Upload queue manager instance
    """
    logger.info(f"Queueing upload: {compressed_file_path} -> {destination}")
    upload_queue.enqueue(compressed_file_path, destination)


@task(name="queue_upload_stitched_volumes", allow_failure=True)
def queue_upload_stitched_volumes_task(
    volume_paths: Dict[str, str],
    destination_base: str,
    upload_queue: Any
) -> None:
    """
    Queue stitched volumes for cloud upload (async, non-blocking).
    
    Parameters
    ----------
    volume_paths : Dict[str, str]
        Dictionary with paths to stitched volume files
    destination_base : str
        Base destination path/URL
    upload_queue : Any
        Upload queue manager instance
    """
    logger.info(f"Queueing uploads for {len(volume_paths)} volumes")
    for modality, path in volume_paths.items():
        destination = f"{destination_base}/{os.path.basename(path)}"
        upload_queue.enqueue(path, destination)


@task(name="notify_slack", allow_failure=True)
def notify_slack_task(
    message: str,
    image_path: Optional[str] = None,
    slack_config: Optional[Dict] = None
) -> bool:
    """
    Send notification to Slack channel (async, non-blocking).
    
    Parameters
    ----------
    message : str
        Message to send
    image_path : str, optional
        Path to image file to upload
    slack_config : dict, optional
        Slack configuration (webhook_url, bot_token, channel)
    
    Returns
    -------
    bool
        True if notification sent successfully
    """
    if not slack_config or not slack_config.get("enabled", False):
        return False
    
    logger.info(f"Sending Slack notification: {message}")
    
    try:
        # TODO: Implement actual Slack integration
        # Option 1: Webhook
        # if slack_config.get("webhook_url"):
        #     from slack_sdk.webhook import WebhookClient
        #     webhook = WebhookClient(slack_config["webhook_url"])
        #     if image_path:
        #         with open(image_path, "rb") as f:
        #             response = webhook.send(text=message, files=[("image", f)])
        #     else:
        #         response = webhook.send(text=message)
        #     return response.status_code == 200
        
        # Option 2: Bot API
        # if slack_config.get("bot_token"):
        #     from slack_sdk import WebClient
        #     client = WebClient(token=slack_config["bot_token"])
        #     channel = slack_config.get("channel", "#oct-processing")
        #     if image_path:
        #         response = client.files_upload_v2(
        #             channel=channel,
        #             file=image_path,
        #             initial_comment=message
        #         )
        #     else:
        #         response = client.chat_postMessage(
        #             channel=channel,
        #             text=message
        #         )
        #     return response["ok"]
        
        logger.debug(f"Slack notification (placeholder): {message}")
        return True
    except Exception as e:
        logger.error(f"Slack notification failed: {e}")
        return False


@task(name="notify_tile_complete", allow_failure=True)
def notify_tile_complete_task(
    mosaic_id: str,
    tile_index: int,
    slack_config: Optional[Dict] = None
) -> bool:
    """
    Send tile completion notification (async, non-blocking).
    
    Parameters
    ----------
    mosaic_id : str
        Mosaic identifier
    tile_index : int
        Tile index
    slack_config : dict, optional
        Slack configuration
    
    Returns
    -------
    bool
        True if notification sent successfully
    """
    message = f"✅ Tile {tile_index} in {mosaic_id} completed"
    return notify_slack_task(message, slack_config=slack_config)


@task(name="notify_stitched_complete", allow_failure=True)
def notify_stitched_complete_task(
    mosaic_id: str,
    aip_path: str,
    slack_config: Optional[Dict] = None
) -> bool:
    """
    Send stitched image to Slack channel (async, non-blocking).
    
    Parameters
    ----------
    mosaic_id : str
        Mosaic identifier
    aip_path : str
        Path to stitched AIP image
    slack_config : dict, optional
        Slack configuration
    
    Returns
    -------
    bool
        True if notification sent successfully
    """
    message = f"🎨 Stitched mosaic {mosaic_id} completed"
    return notify_slack_task(message, image_path=aip_path, slack_config=slack_config)


@task(name="monitor_tile_progress")
def monitor_tile_progress_task(
    mosaic_id: str,
    total_tiles: int,
    completed_tiles: List[str],
    slack_config: Optional[Dict] = None
) -> None:
    """
    Monitor tile progress and send milestone notifications (async, background).
    
    Parameters
    ----------
    mosaic_id : str
        Mosaic identifier
    total_tiles : int
        Total number of tiles
    completed_tiles : List[str]
        List of completed tile indices
    slack_config : dict, optional
        Slack configuration
    """
    completed_count = len(completed_tiles)
    progress = completed_count / total_tiles if total_tiles > 0 else 0.0
    
    milestones = [0.25, 0.50, 0.75, 1.0]
    # TODO: Track milestones per mosaic (could use Redis for distributed)
    # For now, this is a simple implementation that sends all milestones
    # In production, you'd want to track which milestones have been sent
    
    for milestone in milestones:
        if progress >= milestone:
            message = (
                f"🎯 Mosaic {mosaic_id}: {milestone*100:.0f}% complete "
                f"({completed_count}/{total_tiles} tiles)"
            )
            notify_slack_task.submit(message, slack_config=slack_config)
            # Note: In production, you'd want to track sent milestones
            # to avoid sending duplicates


# ============================================================================
# Utility Tasks
# ============================================================================

@task(name="discover_slices")
def discover_slices_task(
    data_root_path: str,
    slice_numbers: Optional[List[int]] = None
) -> List[int]:
    """
    Discover available slices from data directory.
    
    Parameters
    ----------
    data_root_path : str
        Root path to data directory
    slice_numbers : List[int], optional
        Optional list of slice numbers (if None, auto-discover)
    
    Returns
    -------
    List[int]
        List of slice numbers
    """
    if slice_numbers is not None:
        return slice_numbers
    
    logger.info(f"Discovering slices in {data_root_path}")
    
    data_path = Path(data_root_path)
    slices = []
    
    # TODO: Implement actual discovery logic
    # This would scan the directory structure to find available slices
    # For now, return empty list as placeholder
    
    return slices

