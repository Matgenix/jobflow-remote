from __future__ import annotations

from typing import TYPE_CHECKING

from jobflow_remote.jobs.state import JobState

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


def get_graph_elements(flow: FlowInfo):
    ids_mapping = flow.ids_mapping

    nodes = {}
    for job_prop in flow.iter_job_prop():
        db_id = job_prop["db_id"]
        nodes[db_id] = job_prop

    # edges based on parents
    edges = []
    for child_node, parents in zip(flow.db_ids, flow.parents):
        for parent_uuid in parents:
            for parent_node in ids_mapping[parent_uuid].values():
                edges.append((parent_node, child_node))

    # group of nodes based on hosts
    # from collections import defaultdict
    # groups = defaultdict(list)
    hosts = {}
    # for job_prop in flow.iter_job_prop():
    #     for host in job_prop["hosts"]:
    #         groups[host].append(job_prop["db_id"])
    for job_prop in flow.iter_job_prop():
        hosts[job_prop["db_id"]] = job_prop["hosts"]

    return nodes, edges, hosts


def plot_dash(flow: FlowInfo):
    nodes, edges, hosts = get_graph_elements(flow)

    import dash_cytoscape as cyto
    from dash import Dash, Input, Output, callback, html

    app = Dash(f"{flow.name} - {flow.flow_id}")

    elements = []

    # parent elements
    hosts_hierarchy = {}
    jobs_inner_hosts = {}
    hosts_set = set()
    for db_id, job_hosts in hosts.items():
        job_hosts = list(reversed(job_hosts))
        if len(job_hosts) < 2:
            continue
        for i, host in enumerate(job_hosts[1:-1], 1):
            hosts_hierarchy[job_hosts[i + 1]] = host

        hosts_set.update(job_hosts[1:])
        jobs_inner_hosts[db_id] = job_hosts[-1]

    for host in hosts_set:
        elements.append({"data": {"id": host, "parent": hosts_hierarchy.get(host)}})

    for db_id, node_info in nodes.items():
        node_info["id"] = str(db_id)
        node_info["label"] = node_info["name"]
        node_info["parent"] = jobs_inner_hosts.get(db_id)
        elements.append(
            {
                "data": node_info,
            }
        )

    for edge in edges:
        elements.append({"data": {"source": str(edge[0]), "target": str(edge[1])}})

    stylesheet: list[dict] = [
        {
            "selector": f'[state = "{state}"]',
            "style": {
                "background-color": color,
            },
        }
        for state, color in COLOR_MAPPING.items()
    ]
    stylesheet.append(
        {
            "selector": "node",
            "style": {
                "label": "data(name)",
            },
        }
    )
    stylesheet.append(
        {
            "selector": "node:parent",
            "style": {
                "background-opacity": 0.2,
                "background-color": "#2B65EC",
                "border-color": "#2B65EC",
            },
        }
    )

    app.layout = html.Div(
        [
            cyto.Cytoscape(
                id="flow-graph",
                layout={"name": "breadthfirst", "directed": True},
                # layout={'name': 'cose'},
                style={"width": "100%", "height": "500px"},
                elements=elements,
                stylesheet=stylesheet,
            ),
            html.P(id="job-info-output"),
        ]
    )

    @callback(
        Output("job-info-output", "children"), Input("flow-graph", "mouseoverNodeData")
    )
    def displayTapNodeData(data):
        if data:
            return str(data)

    app.run(debug=True)


BLUE_COLOR = "#5E6BFF"
RED_COLOR = "#fC3737"
COLOR_MAPPING = {
    JobState.WAITING.value: "grey",
    JobState.READY.value: "#DAF7A6",
    JobState.CHECKED_OUT.value: BLUE_COLOR,
    JobState.UPLOADED.value: BLUE_COLOR,
    JobState.SUBMITTED.value: BLUE_COLOR,
    JobState.RUNNING.value: BLUE_COLOR,
    JobState.TERMINATED.value: BLUE_COLOR,
    JobState.DOWNLOADED.value: BLUE_COLOR,
    JobState.REMOTE_ERROR.value: RED_COLOR,
    JobState.COMPLETED.value: "#47bf00",
    JobState.FAILED.value: RED_COLOR,
    JobState.PAUSED.value: "#EAE200",
    JobState.STOPPED.value: RED_COLOR,
    JobState.CANCELLED.value: RED_COLOR,
    JobState.BATCH_SUBMITTED.value: BLUE_COLOR,
    JobState.BATCH_RUNNING.value: BLUE_COLOR,
}
