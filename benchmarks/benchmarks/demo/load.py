from pathlib import Path

import getml


def load_or_query(conn, name, **kwargs):
    """
    Loads the data from disk (the project folder) if present, if not, queries it from
    the database associated with `conn`.

    `kwargs` are passed to `getml.data.DataFrame.from_db`.
    """

    if not getml.data.exists(name):
        print(f"Querying {name!r} from {conn.dbname!r}...")
        df = getml.DataFrame.from_db(name=name, table_name=name, conn=conn, **kwargs)
        df.save()
    else:
        print(f"Loading {name!r} from disk (project folder).")
        df = getml.data.load_data_frame(name)

    print()

    return df


def load_or_retrieve(csv_file, name=None, **kwargs):
    """
    Loads the data from disk (the project folder) if present, if not, retrieves the
    and reads the `csv_file`.

    `kwargs` are passed to `getml.data.DataFrame.from_csv`.

    If no name is supplied, the df's name is inferred from the filename.
    """

    if name is None:
        name = Path(csv_file).stem

    if not getml.data.exists(name):
        df = getml.DataFrame.from_csv(fnames=csv_file, name=name, **kwargs)
        df.save()
    else:
        print(f"Loading {name!r} from disk (project folder).")
        df = getml.data.load_data_frame(name)

    print()

    return df
