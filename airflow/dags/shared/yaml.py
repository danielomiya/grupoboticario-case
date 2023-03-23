import os
import re
import typing as t
from functools import reduce
from pathlib import Path

import yaml

ENV_VAR_PATTERN = re.compile(r".*?({{\s*(\w+)\s*}}).*?")


def env_var_constructor(loader: yaml.BaseLoader, node: yaml.Node) -> str:
    """
    Replaces a pattern present in string values with the value of an
    environment variable

    :param loader: a YAML loader
    :param node: a node that needs to be loaded
    :return: a string with the resolved env var value
    """
    value = loader.construct_scalar(node)
    match = ENV_VAR_PATTERN.findall(value)

    if not match:
        return value

    return reduce(
        lambda acc, next: acc.replace(next[0], os.getenv(next[1], "")),
        match,
        value,
    )


yaml.add_implicit_resolver("!envvar", ENV_VAR_PATTERN, Loader=yaml.SafeLoader)
yaml.add_constructor("!envvar", env_var_constructor, Loader=yaml.SafeLoader)


def safe_load(
    path_or_buf: t.Union[str, os.PathLike, t.IO[str], t.IO[bytes]]
) -> t.Any:
    """
    Safely loads a YAML document from a path or a buffer

    :param path_or_buf: path or buffer to be deserialized
    :return: deserialized YAML data
    """
    if isinstance(path_or_buf, os.PathLike):
        path = path_or_buf.__fspath__()
    elif isinstance(path_or_buf, str) and Path(path_or_buf).is_file():
        path = path_or_buf
    else:
        path = None

    if path:
        with open(path) as f:
            return yaml.safe_load(f)

    return yaml.safe_load(path_or_buf)
