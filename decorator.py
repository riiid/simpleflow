from functools import wraps
from typing import Callable
from typing import Optional
from typing import Tuple
from typing import Union
from typing import get_args
from typing import get_type_hints

from .dag import DAG
from .task import Node
from .task import NodeResult
from .task import TaskNode


def task(task_function):
    """Decorator to define `TaskNode` for convenience"""

    @wraps(task_function)
    def wrapper(*parent_nodes: Union[Node, Tuple[Node, str]]) -> TaskNode[NodeResult]:
        return TaskNode(task_function, *parent_nodes)

    return wrapper


def dag(name: Optional[str] = None, description: Optional[str] = None):
    """Create a DAG and add all tasks defined in this context to it."""

    def actual_decorator(dag_function: Callable[[Node], Node]) -> DAG:
        nonlocal description, name
        if Node.current_dag:
            raise RuntimeError("Already in context of another dag!")

        if name is None:
            name = dag_function.__name__
        if description is None:
            description = name

        # Getting `NodeResult` from `Generic[NodeResult]` type object
        type_hints = get_type_hints(dag_function)
        if len(type_hints) != 2 or "return" not in type_hints:
            raise DAGTypeAnnotationError(name)

        input_model_tuple, response_model_tuple = [get_args(hint) for hint in type_hints.values()]
        if len(input_model_tuple) != 1 or len(response_model_tuple) != 1:
            raise DAGTypeAnnotationError(name)

        input_model = input_model_tuple[0]
        response_model = response_model_tuple[0]

        new_dag = DAG(name, description, input_model, response_model)
        Node.current_dag = new_dag

        # All task init and registration happen in the line below
        new_dag.sink_node = dag_function(new_dag.source_node)

        Node.current_dag = None
        return new_dag

    return actual_decorator


class DAGTypeAnnotationError(RuntimeError):
    """Error for when dag functions don't have proper annotations"""

    def __init__(self, dag_name: str):
        message = (
            f"DAG {dag_name} does not have a proper type annotation. "
            "It requires input of `Node[NodeResult]` and return value of `Node[ResponseModel]`. "
            "Ex: `def my_dag(source_node: Node[NodeResult]) -> Node[ResponseModel]:`"
        )
        super().__init__(message)
