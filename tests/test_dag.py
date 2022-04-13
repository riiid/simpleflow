import pytest
from pydantic import BaseModel

from ..decorator import dag
from ..decorator import task
from ..task import Node


@task
def add(x, y):
    return x + y


@task
def exception_raising_task(x: int):
    raise NotImplementedError()


async def test_simple_dag():
    class TestDataModel(BaseModel):
        x: int
        y: int

    @dag()
    def my_dag(source_node: Node[TestDataModel]) -> Node[int]:
        return add(source_node.x, source_node.y)

    result = await my_dag.run(TestDataModel(x=1, y=2))
    assert result == 3


async def test_task_execution_exception():
    """Test error propagation"""

    class TestDataModel(BaseModel):
        x: int
        y: int

    @dag()
    def my_dag(source_node: Node[TestDataModel]) -> Node[int]:
        t1 = add(source_node.x, source_node.y)
        return exception_raising_task(t1)

    with pytest.raises(NotImplementedError):
        await my_dag.run(TestDataModel(x=1, y=2))
