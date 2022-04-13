import asyncio
from base64 import b64encode
from typing import TYPE_CHECKING
from typing import Any
from typing import Dict
from typing import Optional

import networkx as nx
from pydantic import BaseModel

from simpleflow.task import Node
from simpleflow.task import NodeResult
from simpleflow.task import TaskNode
from simpleflow.utils import get_attribute


class DAG:
    def __init__(self, name: str, description: str, input_model: BaseModel, response_model: BaseModel):
        self.graph = nx.DiGraph()
        self.source_node: Node = Node(set(), "request")
        self.input_model = input_model
        self.response_model = response_model
        self.sink_node: Optional[Node] = None
        self.nodes: Dict[str, Node] = {}
        self.name = name
        self.description = description

        self.add(self.source_node)

    def add(self, node: Node):
        """Add a node to the graph."""
        if node.name in self.nodes:
            raise RuntimeError(f"TaskNode {node.name} already in DAG!")

        self.nodes[node.name] = node
        self.graph.add_node(node)

        for prev_node in node.parent_nodes:
            self.graph.add_edge(prev_node, node)

    async def run(self, input_: NodeResult) -> Any:
        """Run the DAG with the given keyword arguments"""
        if TYPE_CHECKING:
            # Non-null is guaranteed after dag is built
            assert self.sink_node

        # Dictionary to store all results
        results: Dict[Node, Any] = {self.source_node: input_}

        running_tasks = set()
        remaining_graph = self.graph.copy()
        remaining_graph.remove_node(self.source_node)

        while remaining_graph:
            # Run nodes with in_degree of 0
            for node, in_degree in remaining_graph.in_degree:
                if in_degree:
                    continue

                args = []
                for arg in node.args:
                    if isinstance(arg, tuple):
                        parent_node, attribute_name = arg
                        args.append(get_attribute(results[parent_node], attribute_name))
                    else:
                        args.append(results[arg])

                # Note that initializing `asyncio.Task` also registers it to the event loop
                aio_task = asyncio.Task(node.run(*args), name=node.name)
                running_tasks.add(aio_task)

            done_tasks, running_tasks = await asyncio.wait(running_tasks, return_when=asyncio.FIRST_COMPLETED)

            # don't confuse; this is an asyncio task, not a simpleflow task.
            for done_task in done_tasks:
                done_node = self.nodes[done_task.get_name()]
                results[done_node] = done_task.result()  # this raises exception if task errored
                remaining_graph.remove_node(done_node)

        return results[self.sink_node]

    def endpoint_description(self) -> str:
        """Description of endpoint for openapi documentation"""

        base64_img = b64encode(self.visualize()).decode()

        docstr = ""
        if self.name:
            docstr += f"<br><b>{self.name}</b>"
        if self.description:
            docstr += f"<br>{self.description}"
        docstr += f'<br><img src="data:image/png;base64, {base64_img}"><br>'

        return docstr

    def visualize(self) -> bytes:
        """Draw png image of graph as bytes"""
        if TYPE_CHECKING:
            assert self.sink_node

        # Build graph for visualization.
        # Only node names are copied, and `GetterNode`s are omitted.
        graph = nx.DiGraph()

        for start_node, end_node in self.graph.edges:
            if isinstance(end_node, TaskNode):
                label = end_node.task_function_argument_label[start_node]
                # Note that spacing is added
                graph.add_edge(start_node.name, end_node.name, label=" " + label)
            else:
                raise NotImplementedError("Visualization currently only supported for `TaskNode`s!")

        graph.add_edge(self.sink_node.name, "response")

        # set default node shape to rectangle (process node in flowchart)
        graph.graph["node"] = {"shape": "record"}

        # draw rounded rectangle (terminal node in flowchart)
        graph.nodes["request"]["style"] = "rounded"
        graph.nodes["response"]["style"] = "rounded"

        agraph = nx.nx_agraph.to_agraph(graph)
        return agraph.draw(format="png", prog="dot")
