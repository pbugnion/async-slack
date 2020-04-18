import dpath.util  # type: ignore

from typing import Dict, Any


def safe_get(obj: Dict[str, Any], path: str):
    try:
        return dpath.util.get(obj, path)
    except KeyError:
        return None


def map_dictionary(mapping: Dict[str, str], obj: Dict[str, Any]):
    return {key: safe_get(obj, key) for key, path in mapping.items()}
