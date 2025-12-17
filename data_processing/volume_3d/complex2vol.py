import dask.array as da
from numpy.typing import ArrayLike


def complex2volume(
    complex3d: ArrayLike, flip_orientation: bool = False, offset: float = 100
    ) -> tuple[da.Array, da.Array, da.Array]:
    if complex3d.shape[0] % 4 != 0:
        raise ValueError("First dimension size must be multiple of 4.")
    raw_tile_width = complex3d.shape[0] // 4
    comp = complex3d.reshape(
        (4, raw_tile_width, complex3d.shape[1], complex3d.shape[2]))
    j1r, j1i, j2r, j2i = comp[0], comp[1], comp[2], comp[3]

    j1 = j1r + 1j * j1i
    j2 = j2r + 1j * j2i
    mag1 = da.abs(j1)
    mag2 = da.abs(j2)

    dBI3D = da.flip(10 * da.log10(mag1 ** 2 + mag2 ** 2), axis=2)
    R3D = da.flip(
        da.arctan(mag1 / mag2) / da.pi * 180,
        axis=2
    )
    offset = da.deg2rad(offset)
    if not flip_orientation:
        phi = da.angle(j1) - da.angle(j2) + offset * 2
    else:
        phi = da.angle(j2) - da.angle(j1) + offset * 2
    # wrap into [-π, π]
    phi = da.where(phi > da.pi, phi - 2 * da.pi, phi)
    phi = da.where(phi < -da.pi, phi + 2 * da.pi, phi)
    O3D = da.flip(phi / (2 * da.pi) * 180, axis=2)

    return dBI3D, R3D, O3D
