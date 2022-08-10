import logging
from contextlib import contextmanager
from typing import Callable, Generator

from getml.engine import delete_project, set_project


@contextmanager
def project(name: str) -> Generator[None, None, None]:
    """
    A context manager that ensures proper setup and teardown of a project and prevents caching.
    """
    set_project(name)
    yield
    delete_project(name)


@contextmanager
def log_run(
    driver: Callable, ds_name: str, run: int, runs: int
) -> Generator[None, None, None]:
    """
    A context manager for logging benchmark runs.
    """
    logging.info(
        "Benchmarking %s on %s (run %i/%i)...",
        driver.__name__[6:],
        ds_name,
        run,
        runs,
    )
    yield
    logging.info("Finished.")
