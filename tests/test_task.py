import asyncio
from typing import Dict

from pydantic import BaseModel

from ..task import Node
from ..decorator import task


@task
def add_1(x: int) -> int:
    return x + 1


@task
def add(x: int, y: int) -> int:
    return x + y


@task
async def async_task(x: int) -> int:
    await asyncio.sleep(0.001)
    return x + 1


@task
def dict_returner() -> Dict[str, int]:
    return {"dict_key": 1}


class MyModel(BaseModel):
    model_key: int


@task
def model_returner() -> MyModel:
    return MyModel(model_key=1)


async def test_simple_task():
    t = add_1(Node())
    assert await t.run(0) == 1


async def test_multiple_input_task():
    t = add(Node(), Node())
    assert await t.run(1, 2) == 3


async def test_async_task():
    t = async_task(Node())
    assert await t.run(0) == 1


def test_task_function_argument_label():
    node1 = Node()
    node2 = dict_returner()
    node3 = model_returner()

    t0 = add(node1, node2.dict_key)
    t1 = add(node2, node2.dict_key)
    t2 = add(t1, node3.model_key)

    assert t0.task_function_argument_label == {node1: "x", node2: "dict_key"}
    assert t1.task_function_argument_label == {node2: "x, dict_key"}
    assert t2.task_function_argument_label == {t1: "x", node3: "model_key"}
