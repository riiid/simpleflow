import inspect
from asyncio import iscoroutinefunction
from collections import defaultdict
from functools import cached_property
from typing import TYPE_CHECKING
from typing import Any
from typing import Awaitable
from typing import Callable
from typing import Dict
from typing import Generic
from typing import Optional
from typing import Set
from typing import Tuple
from typing import TypeVar
from typing import Union

from ddtrace import tracer


if TYPE_CHECKING:
    from dag import DAG

NodeResult = TypeVar("NodeResult")


class Node(Generic[NodeResult]):
    """Parent class of all node classes to be run in DAG"""

    parent_nodes: Set["Node"] = set()
    name: str

    # this is a global property
    current_dag: Optional["DAG"] = None

    def __init__(self, parent_nodes: Set["Node"] = None, name: str = ""):
        """Register to a dag if context is provided"""
        if parent_nodes is None:
            parent_nodes = set()
        self.parent_nodes = parent_nodes
        self.name = name
        if Node.current_dag:
            Node.current_dag.add(self)

    def __getattr__(self, name: str) -> Tuple["Node", str]:
        """Represents attribute lookup"""
        return self, name

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} {self.name}>"


class TaskNode(Node[NodeResult]):
    """
    TaskNode is a minimal unit of execution on this framework.
    It wraps a function and act as a node in a directed acyclic graph.
    """

    # Function that's going to be run
    task_function: Callable[..., Union[NodeResult, Awaitable[NodeResult]]]
    # Represents arguments going into task function when run
    args: Tuple[Union[Node, Tuple[Node, str]], ...]

    def __init__(
        self,
        task_function: Callable[..., Union[NodeResult, Awaitable[NodeResult]]],
        *args: Union[Node, Tuple[Node, str]],
    ):
        self.args = args
        self.task_function = task_function  # type: ignore  # See https://github.com/python/mypy/issues/708

        parent_nodes = set(arg[0] if isinstance(arg, tuple) else arg for arg in args)
        name = task_function.__name__

        super().__init__(parent_nodes, name)

    @cached_property
    def task_function_argument_label(self) -> Dict[Node, str]:
        """Labels each parameter in the task function for documentation purpose"""
        parameters = inspect.signature(self.task_function).parameters

        labels = defaultdict(list)
        for arg, param_name in zip(self.args, parameters):
            # Label with attribute name if attribute is specified, but use function param name if not.
            # This is to choose a more descriptive name.
            if isinstance(arg, tuple):
                parent_node, attribute_name = arg
                labels[parent_node].append(attribute_name)
            else:
                labels[arg].append(param_name)

        return {key: ", ".join(value) for key, value in labels.items()}

    async def run(self, *args: Any) -> NodeResult:
        """Run the task function"""

        with tracer.trace(name=self.name):
            if iscoroutinefunction(self.task_function):
                return await self.task_function(*args)  # type: ignore
            return self.task_function(*args)  # type: ignore
