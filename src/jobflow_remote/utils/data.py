from __future__ import annotations

import os
from collections.abc import Mapping, MutableMapping
from copy import deepcopy
from uuid import UUID


def deep_merge_dict(
    d1: MutableMapping,
    d2: Mapping,
    path: list[str] | None = None,
    raise_on_conflicts: bool = True,
    inplace: bool = True,
) -> MutableMapping:
    """
    Merge a dictionary d2 into a dictionary d1 recursively.

    Parameters
    ----------
    d1
    d2
    path
    raise_on_conflicts
    inplace

    Returns
    -------

    """
    if not inplace:
        d1 = deepcopy(d1)
    if path is None:
        path = []
    for key in d2:
        if key in d1:
            if isinstance(d1[key], Mapping) and isinstance(d2[key], Mapping):
                deep_merge_dict(d1[key], d2[key], path + [str(key)])
            elif d1[key] == d2[key]:
                pass  # same leaf value
            elif raise_on_conflicts:
                raise ValueError("Conflict at %s" % ".".join(path + [str(key)]))
            else:
                d1[key] = d2[key]
        else:
            d1[key] = d2[key]
    return d1


def uuid_to_path(uuid: str, num_subdirs: int = 3, subdir_len: int = 2):
    u = UUID(uuid)
    u_hex = u.hex

    # Split the digest into groups of "subdir_len" characters
    subdirs = [
        u_hex[i : i + subdir_len]
        for i in range(0, num_subdirs * subdir_len, subdir_len)
    ]

    # Combine root directory and subdirectories to form the final path
    return os.path.join(*subdirs, uuid)
