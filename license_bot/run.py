"""
This applies the license to all relevant
files of the source code.
"""

import os


def run():
    """
    This applies the license to all relevant
    files of the source code.
    """

    copyright_txt = open("copyright.txt", "r", encoding="utf-8").read()

    go_comment = _to_comment(copyright_txt, "//")
    _insert_comment("../src/getml-app/src", ".go", go_comment)

    cpp_comment = _to_comment(copyright_txt, "//")
    _insert_comment("../src/engine/include", ".hpp", cpp_comment)
    _insert_comment("../src/engine/src", ".cpp", cpp_comment)

    python_comment = _to_comment(copyright_txt, "#")
    _insert_comment("../src/python-api", ".py", python_comment)


def _insert_comment(path: str, ending: str, comment: str):
    length = len(ending)
    files = os.listdir(path)
    for file in files:
        new_path = path + "/" + file
        if os.path.isdir(new_path):
            _insert_comment(new_path, ending, comment)
        elif file[-length:] == ending:
            _insert_comment_into_file(new_path, comment)


def _insert_comment_into_file(path: str, comment: str):
    content = open(path, "r", encoding="utf-8").read()
    if comment not in content:
        content = open(path, "w", encoding="utf-8").write(comment + "\n\n" + content)
        print(path)


def _to_comment(copyright_txt: str, comment: str) -> str:
    return comment + " " + copyright_txt.replace("\n", "\n" + comment + " ")


if __name__ == "__main__":
    run()
