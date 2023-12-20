from typing import Annotated, Optional

import typer
from jobflow.utils.graph import draw_graph
from rich.prompt import Confirm
from rich.text import Text

from jobflow_remote import SETTINGS
from jobflow_remote.cli.formatting import format_flow_info, get_flow_info_table
from jobflow_remote.cli.jf import app
from jobflow_remote.cli.jfr_typer import JFRTyper
from jobflow_remote.cli.types import (
    days_opt,
    db_ids_opt,
    end_date_opt,
    flow_db_id_arg,
    flow_ids_opt,
    flow_state_opt,
    force_opt,
    hours_opt,
    job_flow_id_flag_opt,
    job_ids_opt,
    max_results_opt,
    name_opt,
    reverse_sort_flag_opt,
    sort_opt,
    start_date_opt,
    verbosity_opt,
)
from jobflow_remote.cli.utils import (
    SortOption,
    check_incompatible_opt,
    exit_with_error_msg,
    exit_with_warning_msg,
    get_job_controller,
    get_job_db_ids,
    get_start_date,
    loading_spinner,
    out_console,
)
from jobflow_remote.jobs.graph import get_graph, plot_dash

app_flow = JFRTyper(
    name="flow", help="Commands for managing the flows", no_args_is_help=True
)
app.add_typer(app_flow)


@app_flow.command(name="list")
def flows_list(
    job_id: job_ids_opt = None,
    db_id: db_ids_opt = None,
    flow_id: flow_ids_opt = None,
    state: flow_state_opt = None,
    start_date: start_date_opt = None,
    end_date: end_date_opt = None,
    name: name_opt = None,
    days: days_opt = None,
    hours: hours_opt = None,
    verbosity: verbosity_opt = 0,
    max_results: max_results_opt = 100,
    sort: sort_opt = SortOption.UPDATED_ON.value,
    reverse_sort: reverse_sort_flag_opt = False,
):
    """
    Get the list of Jobs in the database
    """
    check_incompatible_opt({"start_date": start_date, "days": days, "hours": hours})
    check_incompatible_opt({"end_date": end_date, "days": days, "hours": hours})

    jc = get_job_controller()

    start_date = get_start_date(start_date, days, hours)

    sort = [(sort.value, 1 if reverse_sort else -1)]

    with loading_spinner():
        flows_info = jc.get_flows_info(
            job_ids=job_id,
            db_ids=db_id,
            flow_ids=flow_id,
            state=state,
            start_date=start_date,
            end_date=end_date,
            name=name,
            limit=max_results,
            sort=sort,
            full=verbosity > 0,
        )

        table = get_flow_info_table(flows_info, verbosity=verbosity)

    if SETTINGS.cli_suggestions:
        if max_results and len(flows_info) == max_results:
            out_console.print(
                f"The number of Flows printed is limited by the maximum selected: {max_results}",
                style="yellow",
            )

    out_console.print(table)


@app_flow.command()
def delete(
    job_id: job_ids_opt = None,
    db_id: db_ids_opt = None,
    flow_id: flow_ids_opt = None,
    state: flow_state_opt = None,
    start_date: start_date_opt = None,
    end_date: end_date_opt = None,
    name: name_opt = None,
    days: days_opt = None,
    hours: hours_opt = None,
    force: force_opt = False,
):
    """
    Permanently delete Flows from the database
    """
    check_incompatible_opt({"start_date": start_date, "days": days, "hours": hours})
    check_incompatible_opt({"end_date": end_date, "days": days, "hours": hours})

    start_date = get_start_date(start_date, days, hours)

    jc = get_job_controller()

    with loading_spinner(False) as progress:
        progress.add_task(description="Fetching data...", total=None)
        flows_info = jc.get_flows_info(
            job_ids=job_id,
            db_ids=db_id,
            flow_ids=flow_id,
            state=state,
            start_date=start_date,
            end_date=end_date,
            name=name,
        )

    if not flows_info:
        exit_with_warning_msg("No flows matching criteria")

    if flows_info and not force:
        text = Text.from_markup(
            f"[red]This operation will [bold]delete {len(flows_info)} Flow(s)[/bold]. Proceed anyway?[/red]"
        )

        confirmed = Confirm.ask(text, default=False)
        if not confirmed:
            raise typer.Exit(0)

    to_delete = [fi.flow_id for fi in flows_info]
    with loading_spinner(False) as progress:
        progress.add_task(description="Deleting...", total=None)

        jc.delete_flows(flow_ids=to_delete)

    out_console.print(
        f"Deleted Flow(s) with id: {', '.join(str(i) for i in to_delete)}"
    )


@app_flow.command(name="info")
def flow_info(
    flow_db_id: flow_db_id_arg,
    job_id_flag: job_flow_id_flag_opt = False,
):
    """
    Provide detailed information on a Flow
    """
    db_id, jf_id = get_job_db_ids(flow_db_id, None)
    db_ids = job_ids = flow_ids = None
    if db_id is not None:
        db_ids = [db_id]
    elif job_id_flag:
        job_ids = [jf_id]
    else:
        flow_ids = [jf_id]

    with loading_spinner():
        jc = get_job_controller()

        flows_info = jc.get_flows_info(
            job_ids=job_ids,
            db_ids=db_ids,
            flow_ids=flow_ids,
            limit=1,
            full=True,
        )
    if not flows_info:
        exit_with_error_msg("No data matching the request")

    out_console.print(format_flow_info(flows_info[0]))


@app_flow.command()
def graph(
    flow_db_id: flow_db_id_arg,
    job_id_flag: job_flow_id_flag_opt = False,
    label: Annotated[
        Optional[str],
        typer.Option(
            "--label",
            "-l",
            help="The label used to identify the nodes",
        ),
    ] = "name",
    file_path: Annotated[
        Optional[str],
        typer.Option(
            "--path",
            "-p",
            help="If defined, the graph will be dumped to a file",
        ),
    ] = None,
    dash_plot: Annotated[
        bool,
        typer.Option(
            "--dash",
            "-d",
            help="Show the graph in a dash app",
        ),
    ] = False,
    print_mermaid: Annotated[
        bool,
        typer.Option(
            "--mermaid",
            "-m",
            help="Print the mermaid graph",
        ),
    ] = False,
):
    """
    Provide detailed information on a Flow
    """
    db_id, jf_id = get_job_db_ids(flow_db_id, None)
    db_ids = job_ids = flow_ids = None
    if db_id is not None:
        db_ids = [db_id]
    elif job_id_flag:
        job_ids = [jf_id]
    else:
        flow_ids = [jf_id]

    with loading_spinner():
        jc = get_job_controller()

        flows_info = jc.get_flows_info(
            job_ids=job_ids,
            db_ids=db_ids,
            flow_ids=flow_ids,
            limit=1,
            full=True,
        )
    if not flows_info:
        exit_with_error_msg("No data matching the request")

    if print_mermaid:
        from jobflow_remote.jobs.graph import get_mermaid

        print(get_mermaid(flows_info[0]))

    if dash_plot:
        plot_dash(flows_info[0])
    else:
        plt = draw_graph(get_graph(flows_info[0], label=label))
        if file_path:
            plt.savefig(file_path)
        else:
            plt.show()
