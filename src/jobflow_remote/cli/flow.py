from datetime import datetime, timedelta

import typer
from rich.prompt import Confirm
from rich.text import Text

from jobflow_remote.cli.formatting import get_flow_info_table
from jobflow_remote.cli.jf import app
from jobflow_remote.cli.types import (
    days_opt,
    db_ids_opt,
    end_date_opt,
    flow_ids_opt,
    force_opt,
    job_ids_opt,
    job_state_opt,
    max_results_opt,
    reverse_sort_flag_opt,
    sort_opt,
    start_date_opt,
    verbosity_opt,
)
from jobflow_remote.cli.utils import (
    SortOption,
    check_incompatible_opt,
    exit_with_warning_msg,
    loading_spinner,
    out_console,
)
from jobflow_remote.jobs.jobcontroller import JobController

app_flow = typer.Typer(
    name="flow", help="Commands for managing the flows", no_args_is_help=True
)
app.add_typer(app_flow)


@app_flow.command(name="list")
def flows_list(
    job_id: job_ids_opt = None,
    db_id: db_ids_opt = None,
    flow_id: flow_ids_opt = None,
    state: job_state_opt = None,
    start_date: start_date_opt = None,
    end_date: end_date_opt = None,
    days: days_opt = None,
    verbosity: verbosity_opt = 0,
    max_results: max_results_opt = 100,
    sort: sort_opt = SortOption.UPDATED_ON.value,
    reverse_sort: reverse_sort_flag_opt = False,
):
    """
    Get the list of Jobs in the database
    """
    check_incompatible_opt({"start_date": start_date, "days": days})
    check_incompatible_opt({"end_date": end_date, "days": days})

    jc = JobController()

    if days:
        start_date = datetime.now() - timedelta(days=days)

    sort = [(sort.query_field, 1 if reverse_sort else -1)]

    with loading_spinner():
        flows_info = jc.get_flows_info(
            job_ids=job_id,
            db_ids=db_id,
            flow_id=flow_id,
            state=state,
            start_date=start_date,
            end_date=end_date,
            limit=max_results,
            sort=sort,
        )

        table = get_flow_info_table(flows_info, verbosity=verbosity)

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
    state: job_state_opt = None,
    start_date: start_date_opt = None,
    end_date: end_date_opt = None,
    days: days_opt = None,
    force: force_opt = False,
):
    """
    Permanently delete Flows from the database
    """
    check_incompatible_opt({"start_date": start_date, "days": days})
    check_incompatible_opt({"end_date": end_date, "days": days})

    jc = JobController()

    with loading_spinner(False) as progress:
        progress.add_task(description="Fetching data...", total=None)
        flows_info = jc.get_flows_info(
            job_ids=job_id,
            db_ids=db_id,
            flow_id=flow_id,
            state=state,
            start_date=start_date,
            end_date=end_date,
        )

    if not flows_info:
        exit_with_warning_msg("No flows matching criteria")

    if flows_info and not force:
        text = Text()
        text.append("This operation will ", style="red")
        text.append(f"delete {len(flows_info)} Flow(s)", style="red bold")
        text.append(". Proceed anyway?", style="red")

        confirmed = Confirm.ask(text, default=False)
        if not confirmed:
            raise typer.Exit(0)

    to_delete = [fi.db_ids[0] for fi in flows_info]
    with loading_spinner(False) as progress:
        progress.add_task(description="Deleting...", total=None)

        jc.delete_flows(db_ids=to_delete)

    out_console.print(
        f"Deleted Flow(s) with db_id: {', '.join(str(i) for i in to_delete)}"
    )
