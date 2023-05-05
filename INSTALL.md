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
