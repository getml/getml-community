# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#

import re
from inspect import cleandoc
from typing import Any, Dict

from getml.exceptions import engine_exception_handler

FILE_SYSTEM_MISMATCH_ERROR_MSG_TEMPLATE = cleandoc(
    """
    Engine could not retrieve file:
    {file_name!r}

    Is the Engine running inside docker? When working with getML's database
    module, file paths are interpreted as paths inside the docker container.

    Hint: Use `docker cp <container>:/home/getml/assets` to copy the files into
    the docker container and read the data from the container's filesystem
    directly, e.g.:

    `getml.database.read_csv('table_name', '/home/getml/assets/file.csv')`
    `getml.database.connect_sqlite3("/home/getml/assets/database.db")`
    """
)


@engine_exception_handler
def handle_database_file_system_mismatch_error(msg: str, extra: Dict[str, Any]):
    if msg.endswith("unable to open database file"):
        file_name = extra.get("name", "")
        raise OSError(
            FILE_SYSTEM_MISMATCH_ERROR_MSG_TEMPLATE.format(file_name=file_name)
        )
    could_not_be_opened_msg = re.match("'(.+)' could not be opened!", msg)
    if could_not_be_opened_msg:
        file_name = could_not_be_opened_msg.group(1)
        raise OSError(
            FILE_SYSTEM_MISMATCH_ERROR_MSG_TEMPLATE.format(file_name=file_name)
        )
