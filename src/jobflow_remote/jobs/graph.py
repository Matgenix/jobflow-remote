from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from networkx import DiGraph

    from jobflow_remote.jobs.data import FlowInfo


def get_graph(flow: FlowInfo, label: str = "name") -> DiGraph:
    import networkx as nx

    graph = nx.DiGraph()

    ids_mapping = flow.ids_mapping

    # Add nodes
    for job_prop in flow.iter_job_prop():
        db_id = job_prop["db_id"]
        job_prop["label"] = job_prop[label]
        # change this as the "name" is used in jobflow's graph plotting util
        job_prop["job_name"] = job_prop.pop("name")
        graph.add_node(db_id, **job_prop)

    # Add edges based on parents
    for child_node, parents in zip(flow.db_ids, flow.parents):
        for parent_uuid in parents:
            for parent_node in ids_mapping[parent_uuid].values():
                graph.add_edge(parent_node, child_node)

    return graph
