from __future__ import annotations

import os
from collections.abc import Mapping, MutableMapping
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import maggma.stores  # required to enable subclass searching
from maggma.core.store import Store
from monty.json import MontyDecoder


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


def remove_none(obj):
    if isinstance(obj, (list, tuple, set)):
        return type(obj)(remove_none(x) for x in obj if x is not None)
    elif isinstance(obj, dict):
        return type(obj)(
            (remove_none(k), remove_none(v))
            for k, v in obj.items()
            if k is not None and v is not None
        )
    else:
        return obj


def check_dict_keywords(obj: Any, keywords: list[str]) -> bool:
    if isinstance(obj, (list, tuple, set)):
        return any(check_dict_keywords(x, keywords) for x in obj)
    elif isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(k, str) and any(k.startswith(kw) for kw in keywords):
                return True
            if check_dict_keywords(v, keywords):
                return True
    return False


def uuid_to_path(
    uuid: str, index: int | None = 1, num_subdirs: int = 3, subdir_len: int = 2
):
    u = UUID(uuid)
    u_hex = u.hex

    # Split the digest into groups of "subdir_len" characters
    subdirs = [
        u_hex[i : i + subdir_len]
        for i in range(0, num_subdirs * subdir_len, subdir_len)
    ]

    # add the index to the final dir name
    dir_name = f"{uuid}"
    if index is not None:
        dir_name += f"_{index}"

    # Combine root directory and subdirectories to form the final path
    return os.path.join(*subdirs, dir_name)


def store_from_dict(store_dict: dict) -> Store:
    if "@class" in store_dict and "@module" in store_dict:
        store = MontyDecoder().process_decoded(store_dict)
        if not isinstance(store, Store):
            raise ValueError(
                f"The converted object {store} is not an instance of a maggma Store"
            )
        return store
    else:

        def all_subclasses(cl):
            return set(cl.__subclasses__()).union(
                [s for c in cl.__subclasses__() for s in all_subclasses(c)]
            )

        all_stores = {s.__name__: s for s in all_subclasses(maggma.stores.Store)}
        return convert_store(store_dict, all_stores)


def convert_store(spec_dict: dict, valid_stores) -> Store:
    """
    Build a store based on the dict spec configuration from JobFlow
    TODO expose the methods from jobflow and don't duplicate the code
    """

    _spec_dict = dict(spec_dict)
    store_type = _spec_dict.pop("type")
    for k, v in _spec_dict.items():
        if isinstance(v, dict) and "type" in v:
            _spec_dict[k] = convert_store(v, valid_stores)
    return valid_stores[store_type](**_spec_dict)


def convert_utc_time(datetime_value: datetime) -> datetime:
    """
    Convert a time in UTC (used in the DB) to the time zone of the
    system where the code is being executed.

    Parameters
    ----------
    datetime_value
        a datetime object in UTC
    Returns
    -------
        The datetime in the zone of the current system
    """
    return datetime_value.replace(tzinfo=timezone.utc).astimezone(tz=None)
