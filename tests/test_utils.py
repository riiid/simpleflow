from pydantic import BaseModel

from simpleflow.utils import get_attribute


class MyModel(BaseModel):
    x: int


def test_get_attribute():
    my_model = MyModel(x=1)
    my_dict = {"x": 1}
    assert get_attribute(my_model, "x") == 1
    assert get_attribute(my_dict, "x") == 1
