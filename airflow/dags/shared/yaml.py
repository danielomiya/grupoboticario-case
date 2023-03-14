import os
import re
from functools import reduce

import yaml

ENV_VAR_PATTERN = re.compile(r".*?({{\s*(\w+)\s*}}).*?")


def env_var_constructor(loader: yaml.BaseLoader, node: yaml.Node) -> str:
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


safe_load = yaml.safe_load
