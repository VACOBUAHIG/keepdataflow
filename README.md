# keepdataflow
A python library to extendnd  data load process

## Overview
TBD

## Usage
This project is set up using poetry. To install the dependencies, run `poetry install` from the root of the project.


## Next Steps
    1. Udpate the keepit sql module to perform upsert from different systems natively 
    2. Add the Database to database module
    3. Uppdate the merge function on df_to database to dynamically chance merge type based on system 
    4. start adding annotations and documents
    5. in keepitsql merge function, update that joinn_key name to matched condtions





```shell
poetry install
```

To add a new dependency, run `poetry add <dependency>` from the root of the project.

```shell
poetry add <dependency>
```

### Pre-Commit Hooks
This project uses [pre-commit](https://pre-commit.com/) to run linting and formatting tools before each commit. To install the pre-commit hooks, run `pre-commit install` from the root of the project.

```shell
poetry run pre-commit install
```

To run the pre-commit hooks manually, run `pre-commit run --all-files` from the root of the project.

```shell
poetry run pre-commit run --all-files
```


### Testing
This project uses [pytest](https://docs.pytest.org/en/stable/) for testing. To run the tests, run `pytest` from the root of the project in the poetry shell.

```shell
poetry run pytest
```

There are sensible defaults for pytest setup in the `pyproject.toml` file. You can override these defaults by passing in command line arguments. For example, to run the tests with debug logging enabled, run `pytest --log-cli-level=DEBUG` from the root of the project.

```shell
poetry run pytest --log-cli-level=DEBUG
```

