"""
Contains two functions: one uploads the getML build to Google Cloud bucket and
the other sends a confirmation message in Google Chat.
"""

import os
from typing import List, Tuple

import click
from google.cloud import storage  # type: ignore
from oauth2client.service_account import ServiceAccountCredentials  # type: ignore
from googleapiclient.discovery import build  # type: ignore
from httplib2 import Http  # type: ignore

BUCKET_NAME = "static.getml.com"


@click.command()
@click.argument("files", nargs=-1, type=click.Path(exists=True), required=True)
@click.option(
    "--release",
    "-r",
    type=str,
    required=True,
    help=f"Name of release folder inside '{BUCKET_NAME}/download/' where the "
    + "FILE(S) will be uploaded.",
)
@click.option(
    "--force",
    "-f",
    is_flag=True,
    default=False,
    help="Force delete the existing FILE(S).",
)
def upload(files: Tuple[str, ...], release: str, force: bool) -> None:
    """Uploads the FILE(S) of latest getML built to Google Cloud bucket.
    Once the upload completes, a confirmation message is sent in Google Chat.
    """

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"getml-infra-service-key.json"

    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)

    files_messages: List[str] = []

    for file in files:

        path_in_bucket = "download/" + release + "/" + os.path.basename(file)
        blob = bucket.blob(path_in_bucket)

        if blob.exists() and not force:
            print(
                f"The FILE '{file}' already exist inside '/{BUCKET_NAME}/"
                f"{os.path.dirname(path_in_bucket)}/' directory. "
                "Please use -f flag to replace the existing FILE(S)."
            )
            return

        print(f"Uploading {file}...")
        blob.upload_from_filename(file, timeout=600)
        print(f"{file}: successfully uploaded.")

        files_messages.append(
            f"{file}: https://storage.googleapis.com/{BUCKET_NAME}/download"
            f"/{release}/{file}" + "\n"
        )

    first_line_message: str = (
        f"getML release {release} is ready and may be accessed"
        " with the following links:" + "\n"
    )

    complete_message: str = first_line_message + "".join(files_messages)

    # TODO
    # send_chat_message(complete_message)
    print(complete_message)


def send_chat_message(message: str) -> None:
    """Sends a message to Google Chat after the successful uploading of a getML
    build FILE(S) to Google Cloud bucket.
    """

    scopes: List[str] = ["https://www.googleapis.com/auth/chat.bot"]
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        "getml-infra-service-key.json", scopes
    )

    chat_service = build("chat", "v1", http=credentials.authorize(Http()))

    spaces_list = chat_service.spaces().list().execute()  # pylint: disable=no-member

    for space in spaces_list["spaces"]:
        if space["displayName"] == "CI/CD":
            target_space_name: str = space["name"]

    chat_service.spaces().messages().create(  # pylint: disable=no-member
        parent=target_space_name, body={"text": message}
    ).execute()
