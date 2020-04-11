import dpath.util

def safe_get(obj, path):
    try:
        return dpath.util.get(obj, path)
    except KeyError:
        return None


def map_dictionary(mapping, d):
    return {key: safe_get(d, key) for key, path in mapping.items()}
