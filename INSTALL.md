# Installation

Clone this repository and then install with `pip` in the virtual environment of your choice.

```
git clone git@{{ repository_provider }}:{{ repository_namespace }}/{{ package_name }}
cd {{package_name}}
pip install .
```

## Development installation

You can use

```
pip install -e .[dev,tests]
```

to perform an editable installation with additional development and test dependencies.
You can then activate `pre-commit` in your local repository with `pre-commit install`.

## Simple usage

jobflow-remote requires several things to be set up before first use.

Firstly, configure a project by adding a `.yaml` file into the `$HOME/.jfremote`
directory.
This should contain connection details on the FireServer to use to manage the workflows, details on any FireWorkers
that will be executing the workflows, and details of the job queue.
You should also configure a FireWorks launchpad on your machine.
Using the unique name provided in that config, you should be able to create a
test job:

```python
from jobflow import job, Flow
from jobflow_remote.jobs.submit import submit_flow

@job
def add(a, b):
    return a + b

add_first = add(1, 5)
add_second = add(add_first.output, 5)

flow = Flow([add_first, add_second])

submit_flow(flow)
```

Any FireWorkers must also be configured to run jobs from the launchpad.
