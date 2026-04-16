"""
LazyHDF5ArrayAdapter — project-local replacement for Tiled's HDF5ArrayAdapter.

The stock `tiled.adapters.hdf5.HDF5ArrayAdapter` wraps h5py Datasets in
`dask.delayed → from_delayed → rechunk`. On every catalog read this forces
`__array__` → `h5py.read_direct` of the ENTIRE registered dataset before
the user slice is applied (see debug/MODE_B_ROOT_CAUSE.md §4).

This adapter is a drop-in replacement that honors the `parameters["dataset"]`
and `parameters["slice"]` kwargs that the broker already stores on each
data_source, and does direct `h5py.Dataset[base_index][user_slice]` indexing.
That hits h5py's native per-chunk path and reads only the bytes the client
asked for.

Dispatched via `adapters_by_mimetype` on the private mimetype
`application/x-hdf5-broker`, so it coexists with stock `application/x-hdf5`.
"""

import copy
from typing import Any, List, Optional, Tuple, Union

import h5py
import numpy
from numpy.typing import NDArray

from tiled.adapters.core import Adapter
from tiled.catalog.orm import Node
from tiled.ndslice import NDBlock, NDSlice
from tiled.structures.array import ArrayStructure
from tiled.structures.core import Spec, StructureFamily
from tiled.structures.data_source import DataSource
from tiled.type_aliases import JSON
from tiled.utils import path_from_uri


class LazyHDF5ArrayAdapter(Adapter[ArrayStructure]):
    """Array adapter for single-slice HDF5 entities.

    `base_index` is the slice index into the leading axis of the underlying
    HDF5 dataset (or None if the registered entity is the full dataset).
    The registered array structure therefore has shape equal to
    `ds.shape[1:]` when `base_index is not None`.
    """

    structure_family = StructureFamily.array

    def __init__(
        self,
        file_path: str,
        dataset_path: str,
        base_index: Optional[int],
        structure: ArrayStructure,
        *,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
    ) -> None:
        self._file_path = file_path
        self._dataset_path = dataset_path
        self._base_index = base_index
        super().__init__(structure, metadata=metadata, specs=specs)

    @classmethod
    def from_catalog(
        cls,
        data_source: DataSource[ArrayStructure],
        node: Node,
        /,
        dataset: Optional[str] = None,
        slice: Optional[Union[str, int]] = None,
        **_ignored: Any,
    ) -> "LazyHDF5ArrayAdapter":
        """Build adapter from a catalog row.

        The broker stores `parameters = {"dataset": "<path>", "slice": "<int>"}`
        on the data source row. Tiled's catalog adapter unpacks those into
        kwargs when invoking this classmethod.
        """
        assets = data_source.assets
        data_uris = [
            ast.data_uri for ast in assets if ast.parameter == "data_uris"
        ] or [assets[0].data_uri]
        file_path = path_from_uri(data_uris[0])

        if dataset is None:
            raise ValueError(
                "LazyHDF5ArrayAdapter requires parameters['dataset'] "
                "(the HDF5 path of the source dataset)."
            )

        base_index: Optional[int]
        if slice is None or slice == "":
            base_index = None
        elif isinstance(slice, str):
            base_index = int(slice)
        else:
            base_index = int(slice)

        # Validate on-disk shape/dtype against the registered structure
        # (matches stock HDF5 adapter behavior; saves debugging time when
        # catalog goes stale).
        with h5py.File(file_path, "r", locking=False) as f:
            ds = f[dataset]
            full_shape = tuple(ds.shape)
            ds_dtype = ds.dtype

        expected_shape = (
            full_shape[1:] if base_index is not None else full_shape
        )
        registered_shape = tuple(data_source.structure.shape)
        if expected_shape != registered_shape:
            raise ValueError(
                f"Shape mismatch for {file_path}:{dataset}[{base_index}]: "
                f"registered={registered_shape}, on_disk={expected_shape}"
            )
        registered_dtype = data_source.structure.data_type.to_numpy_dtype()
        if ds_dtype != registered_dtype:
            raise ValueError(
                f"Dtype mismatch for {file_path}:{dataset}: "
                f"registered={registered_dtype}, on_disk={ds_dtype}"
            )

        return cls(
            file_path,
            dataset,
            base_index,
            data_source.structure,
            metadata=copy.deepcopy(node.metadata_),
            specs=node.specs,
        )

    def _open_and_select(self) -> Tuple[h5py.File, h5py.Dataset]:
        f = h5py.File(self._file_path, "r", locking=False)
        return f, f[self._dataset_path]

    def read(
        self,
        slice: NDSlice = NDSlice(...),
    ) -> NDArray[Any]:
        """Read user-specified slice of the registered entity.

        Opens h5py per request (cheap on this Lustre view: ~0.5 ms, see
        debug/bench_open_cost.py). Avoids cross-thread file-handle sharing.
        """
        with h5py.File(self._file_path, "r", locking=False) as f:
            ds = f[self._dataset_path]
            if self._base_index is not None:
                # h5py reads exactly the bytes of the one chunk at base_index.
                row = ds[self._base_index]
                arr = row[tuple(slice)] if slice else row
            else:
                arr = ds[tuple(slice)] if slice else ds[...]
            return numpy.asarray(arr)

    def read_block(
        self,
        block: NDBlock,
        slice: NDSlice = NDSlice(...),
    ) -> NDArray[Any]:
        """Read a dask-style block of the registered entity."""
        block_slice = block.slice_from_chunks(self._structure.chunks)
        with h5py.File(self._file_path, "r", locking=False) as f:
            ds = f[self._dataset_path]
            if self._base_index is not None:
                row = ds[self._base_index]
                arr = row[tuple(block_slice)]
            else:
                arr = ds[tuple(block_slice)]
            if slice:
                arr = arr[tuple(slice)]
            return numpy.asarray(arr)
