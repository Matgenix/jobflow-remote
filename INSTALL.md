# Installation

Simple installation instructions can be found below.
For more advanced setup including configuring a database and runner, please see the [online documentation](https://matgenix.github.io/jobflow-remote/user/install.html)

Clone this repository and then install with `pip` in the virtual environment of your choice.

```
git clone https://github.com/Matgenix/jobflow-remote
cd jobflow-remote
pip install .
```

## Development installation

You can use

```
pip install -e .[dev,tests]
```

to perform an editable installation with additional development and test dependencies.
You can then activate `pre-commit` in your local repository with `pre-commit install`.
