from typing import Any
from typing import Mapping
from typing import Union

from pydantic import BaseModel


def get_attribute(result: Union[Mapping, BaseModel], attribute_name: str) -> Any:
    """Convenience function to get attribute both from mapping and pydantic models"""
    if isinstance(result, Mapping):
        return result[attribute_name]
    return result.__getattribute__(attribute_name)
