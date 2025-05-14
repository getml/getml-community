# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Contains various constants for getML
"""

COMPARISON_ONLY = ", comparison only"
""""""
JOIN_KEY_SEP = "$GETML_JOIN_KEY_SEP"
""""""
MULTIPLE_JOIN_KEYS_BEGIN = "$GETML_MULTIPLE_JOIN_KEYS_BEGIN"
""""""
MULTIPLE_JOIN_KEYS_END = "$GETML_MULTIPLE_JOIN_KEYS_END"
""""""
NO_JOIN_KEY = "$GETML_NO_JOIN_KEY"
""""""
ROWID = "rowid"
""""""
TIME_STAMP = "time stamp"
""""""
TIME_FORMATS = [
    "%Y-%m-%dT%H:%M:%s%z",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d",
    "%Y-%m-%dT%H:%M:%S.%F",  # microsecond
    "%Y-%m-%dT%H:%M:%S.%i",  # millisecond
    "%Y-%m-%dT%H:%M:%S.%c",  # centisecond
    "%Y-%m-%d %H:%M:%S.%F",  # microsecond
    "%Y-%m-%d %H:%M:%S.%i",  # millisecond
    "%Y-%m-%d %H:%M:%S.%c",  # centisecond
    "%Y-%m-%d %H:%M:%s%z",
]
"""The default time stamp formats to be used.

Whenever a time stamp is parsed from a string,
the getML Engine tries different time stamp formats.

The procedure works as follows: The Engine tries to parse the string
using the first format. If that fails, it will use the second format etc.

If none of the time stamp formats work, then it tries to parse the string
as a floating point value, which it will interpret as the number of
seconds since UNIX time (January 1, 1970).

If that fails as well, the value is set to NULL.
"""

DEFAULT_BATCH_SIZE = 100000
"""
The default batch size used whenver batched IO operations are performed.
"""

ANNOTATIONS_DOCS_URL = "https://getml.com/latest/user_guide/concepts/annotating_data/"
"""
URL that points to the annotations sections in the user guide of the getML documentation.
"""

DOCKER_DOCS_URL = "https://getml.com/latest/install/packages/docker/"
"""
URL that points to the docker sections of the getML documentation.
"""

ENTERPRISE_DOCS_URL = "https://getml.com/latest/enterprise/"
"""
URL that points to the enterprise sections of the getML documentation.
"""

INSTALL_DOCS_URL = "https://getml.com/latest/install/"
"""
URL that points to the installation sections of the getML documentation.
"""

COMPOSE_FILE_URL = "https://raw.githubusercontent.com/getml/getml-community/refs/heads/main/runtime/docker-compose.yml"
"""
URL that points to the docker-compose runtime file.
"""
