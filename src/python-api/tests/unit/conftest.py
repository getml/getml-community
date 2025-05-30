from __future__ import annotations

import fnmatch
import itertools as it
import struct
from typing import Iterable, List, Tuple, Type

import pytest

from getml.events.types import EventContext, EventSource


class FakePath:
    valid_paths: List[str] = []
    valid_files: List[str] = []

    def __init__(self, path: str):
        self._path = path

    def __eq__(self, other: FakePath) -> bool:
        return self._path == other._path

    def __str__(self) -> str:
        return self._path

    def __repr__(self) -> str:
        return f"FakePath({self._path})"

    def __fspath__(self) -> str:
        return self._path

    def exists(self) -> bool:
        return self._path in FakePath.valid_paths or self._path in FakePath.valid_files

    def glob(self, pattern: str) -> List[FakePath]:
        pattern_abs = f"{self._path}/{pattern}"
        return [
            FakePath(path)
            for path in it.chain(self.valid_paths, self.valid_files)
            if fnmatch.fnmatch(path, pattern_abs)
        ]

    def is_file(self) -> bool:
        return self._path in FakePath.valid_files

    def is_dir(self) -> bool:
        return self._path in FakePath.valid_paths

    @property
    def name(self) -> str:
        return next(reversed(self._path.split("/")), self._path)

    def read_text(self) -> str:
        if self._path in FakePath.valid_files:
            return "fake file content for " + self._path
        else:
            raise FileNotFoundError(f"No such file: '{self._path}'")

    def write_text(self, content) -> None:
        if self._path in FakePath.valid_files:
            print(f"Writing '{content}' to fake path {self._path}")
        else:
            FakePath.valid_files.append(self._path)
            print(f"Creating and writing '{content}' to fake path {self._path}")


@pytest.fixture
def fake_path(request) -> Type[FakePath]:
    FakePath.valid_paths = request.param.get("valid_paths", [])
    FakePath.valid_files = request.param.get("valid_files", [])
    return FakePath


class MockSocket:
    """
    A mock socket that can be used to simulate a connection to the engine.

    The socket will return a stream of messages that are encoded in the same way
    as the engine would send them (prefixing each message with its length in bytes).
    """

    def __init__(self, messages: Iterable[str]):
        self.messages = messages
        self.buffer = b""
        self._buffer_messages()

    def _buffer_messages(self):
        for message in self.messages:
            self.buffer += struct.pack(">I", len(message))
            self.buffer += message.encode()

    def recv(self, size: int) -> bytes:
        result = self.buffer[:size]
        self.buffer = self.buffer[size:]
        return result

    def getpeername(self):
        return ("localhost", 1709)


def pipeline_fit_context_messages() -> Tuple[EventContext, List[str]]:
    return EventContext(source=EventSource.ENGINE, cmd={"type_": "Pipeline.fit"}), [
        "Checking data model...",
        "log: Staging...",
        "log: Progress: 100%.",
        "log: Preprocessing...",
        "log: Progress: 100%.",
        "log: Checking...",
        "log: Progress: 12%.",
        "log: Progress: 25%.",
        "log: Progress: 37%.",
        "log: Progress: 50%.",
        "log: Progress: 66%.",
        "log: Progress: 83%.",
        "log: Progress: 100%.",
        "Success!",
        "OK.",
        "log: Staging...",
        "log: Progress: 100%.",
        "log: FastProp: Trying 548 features...",
        "log: Built 100 features. Progress: 18%.",
        "log: Built 200 features. Progress: 36%.",
        "log: Built 300 features. Progress: 54%.",
        "log: Built 400 features. Progress: 72%.",
        "log: Built 500 features. Progress: 91%.",
        "log: Built 548 features. Progress: 100%.",
        "log: FastProp: Building features...",
        "log: Built 100 rows. Progress: 100%.",
        "log: XGBoost: Training as feature selector...",
        "log: XGBoost: Trained tree 1. Progress: 1%.",
        "log: XGBoost: Trained tree 2. Progress: 2%.",
        "log: XGBoost: Trained tree 3. Progress: 3%.",
        "log: XGBoost: Trained tree 4. Progress: 4%.",
        "log: XGBoost: Trained tree 5. Progress: 5%.",
        "log: XGBoost: Trained tree 6. Progress: 6%.",
        "log: XGBoost: Trained tree 7. Progress: 7%.",
        "log: XGBoost: Trained tree 8. Progress: 8%.",
        "log: XGBoost: Trained tree 9. Progress: 9%.",
        "log: XGBoost: Trained tree 10. Progress: 10%.",
        "log: XGBoost: Trained tree 11. Progress: 11%.",
        "log: XGBoost: Trained tree 12. Progress: 12%.",
        "log: XGBoost: Trained tree 13. Progress: 13%.",
        "log: XGBoost: Trained tree 14. Progress: 14%.",
        "log: XGBoost: Trained tree 15. Progress: 15%.",
        "log: XGBoost: Trained tree 16. Progress: 16%.",
        "log: XGBoost: Trained tree 17. Progress: 17%.",
        "log: XGBoost: Trained tree 18. Progress: 18%.",
        "log: XGBoost: Trained tree 19. Progress: 19%.",
        "log: XGBoost: Trained tree 20. Progress: 20%.",
        "log: XGBoost: Trained tree 21. Progress: 21%.",
        "log: XGBoost: Trained tree 22. Progress: 22%.",
        "log: XGBoost: Trained tree 23. Progress: 23%.",
        "log: XGBoost: Trained tree 24. Progress: 24%.",
        "log: XGBoost: Trained tree 25. Progress: 25%.",
        "log: XGBoost: Trained tree 26. Progress: 26%.",
        "log: XGBoost: Trained tree 27. Progress: 27%.",
        "log: XGBoost: Trained tree 28. Progress: 28%.",
        "log: XGBoost: Trained tree 29. Progress: 29%.",
        "log: XGBoost: Trained tree 30. Progress: 30%.",
        "log: XGBoost: Trained tree 31. Progress: 31%.",
        "log: XGBoost: Trained tree 32. Progress: 32%.",
        "log: XGBoost: Trained tree 33. Progress: 33%.",
        "log: XGBoost: Trained tree 34. Progress: 34%.",
        "log: XGBoost: Trained tree 35. Progress: 35%.",
        "log: XGBoost: Trained tree 36. Progress: 36%.",
        "log: XGBoost: Trained tree 37. Progress: 37%.",
        "log: XGBoost: Trained tree 38. Progress: 38%.",
        "log: XGBoost: Trained tree 39. Progress: 39%.",
        "log: XGBoost: Trained tree 40. Progress: 40%.",
        "log: XGBoost: Trained tree 41. Progress: 41%.",
        "log: XGBoost: Trained tree 42. Progress: 42%.",
        "log: XGBoost: Trained tree 43. Progress: 43%.",
        "log: XGBoost: Trained tree 44. Progress: 44%.",
        "log: XGBoost: Trained tree 45. Progress: 45%.",
        "log: XGBoost: Trained tree 46. Progress: 46%.",
        "log: XGBoost: Trained tree 47. Progress: 47%.",
        "log: XGBoost: Trained tree 48. Progress: 48%.",
        "log: XGBoost: Trained tree 49. Progress: 49%.",
        "log: XGBoost: Trained tree 50. Progress: 50%.",
        "log: XGBoost: Trained tree 51. Progress: 51%.",
        "log: XGBoost: Trained tree 52. Progress: 52%.",
        "log: XGBoost: Trained tree 53. Progress: 53%.",
        "log: XGBoost: Trained tree 54. Progress: 54%.",
        "log: XGBoost: Trained tree 55. Progress: 55%.",
        "log: XGBoost: Trained tree 56. Progress: 56%.",
        "log: XGBoost: Trained tree 57. Progress: 57%.",
        "log: XGBoost: Trained tree 58. Progress: 58%.",
        "log: XGBoost: Trained tree 59. Progress: 59%.",
        "log: XGBoost: Trained tree 60. Progress: 60%.",
        "log: XGBoost: Trained tree 61. Progress: 61%.",
        "log: XGBoost: Trained tree 62. Progress: 62%.",
        "log: XGBoost: Trained tree 63. Progress: 63%.",
        "log: XGBoost: Trained tree 64. Progress: 64%.",
        "log: XGBoost: Trained tree 65. Progress: 65%.",
        "log: XGBoost: Trained tree 66. Progress: 66%.",
        "log: XGBoost: Trained tree 67. Progress: 67%.",
        "log: XGBoost: Trained tree 68. Progress: 68%.",
        "log: XGBoost: Trained tree 69. Progress: 69%.",
        "log: XGBoost: Trained tree 70. Progress: 70%.",
        "log: XGBoost: Trained tree 71. Progress: 71%.",
        "log: XGBoost: Trained tree 72. Progress: 72%.",
        "log: XGBoost: Trained tree 73. Progress: 73%.",
        "log: XGBoost: Trained tree 74. Progress: 74%.",
        "log: XGBoost: Trained tree 75. Progress: 75%.",
        "log: XGBoost: Trained tree 76. Progress: 76%.",
        "log: XGBoost: Trained tree 77. Progress: 77%.",
        "log: XGBoost: Trained tree 78. Progress: 78%.",
        "log: XGBoost: Trained tree 79. Progress: 79%.",
        "log: XGBoost: Trained tree 80. Progress: 80%.",
        "log: XGBoost: Trained tree 81. Progress: 81%.",
        "log: XGBoost: Trained tree 82. Progress: 82%.",
        "log: XGBoost: Trained tree 83. Progress: 83%.",
        "log: XGBoost: Trained tree 84. Progress: 84%.",
        "log: XGBoost: Trained tree 85. Progress: 85%.",
        "log: XGBoost: Trained tree 86. Progress: 86%.",
        "log: XGBoost: Trained tree 87. Progress: 87%.",
        "log: XGBoost: Trained tree 88. Progress: 88%.",
        "log: XGBoost: Trained tree 89. Progress: 89%.",
        "log: XGBoost: Trained tree 90. Progress: 90%.",
        "log: XGBoost: Trained tree 91. Progress: 91%.",
        "log: XGBoost: Trained tree 92. Progress: 92%.",
        "log: XGBoost: Trained tree 93. Progress: 93%.",
        "log: XGBoost: Trained tree 94. Progress: 94%.",
        "log: XGBoost: Trained tree 95. Progress: 95%.",
        "log: XGBoost: Trained tree 96. Progress: 96%.",
        "log: XGBoost: Trained tree 97. Progress: 97%.",
        "log: XGBoost: Trained tree 98. Progress: 98%.",
        "log: XGBoost: Trained tree 99. Progress: 99%.",
        "log: XGBoost: Trained tree 100. Progress: 100%.",
        "log: XGBoost: Training as predictor...",
        "log: XGBoost: Trained tree 1. Progress: 1%.",
        "log: XGBoost: Trained tree 2. Progress: 2%.",
        "log: XGBoost: Trained tree 3. Progress: 3%.",
        "log: XGBoost: Trained tree 4. Progress: 4%.",
        "log: XGBoost: Trained tree 5. Progress: 5%.",
        "log: XGBoost: Trained tree 6. Progress: 6%.",
        "log: XGBoost: Trained tree 7. Progress: 7%.",
        "log: XGBoost: Trained tree 8. Progress: 8%.",
        "log: XGBoost: Trained tree 9. Progress: 9%.",
        "log: XGBoost: Trained tree 10. Progress: 10%.",
        "log: XGBoost: Trained tree 11. Progress: 11%.",
        "log: XGBoost: Trained tree 12. Progress: 12%.",
        "log: XGBoost: Trained tree 13. Progress: 13%.",
        "log: XGBoost: Trained tree 14. Progress: 14%.",
        "log: XGBoost: Trained tree 15. Progress: 15%.",
        "log: XGBoost: Trained tree 16. Progress: 16%.",
        "log: XGBoost: Trained tree 17. Progress: 17%.",
        "log: XGBoost: Trained tree 18. Progress: 18%.",
        "log: XGBoost: Trained tree 19. Progress: 19%.",
        "log: XGBoost: Trained tree 20. Progress: 20%.",
        "log: XGBoost: Trained tree 21. Progress: 21%.",
        "log: XGBoost: Trained tree 22. Progress: 22%.",
        "log: XGBoost: Trained tree 23. Progress: 23%.",
        "log: XGBoost: Trained tree 24. Progress: 24%.",
        "log: XGBoost: Trained tree 25. Progress: 25%.",
        "log: XGBoost: Trained tree 26. Progress: 26%.",
        "log: XGBoost: Trained tree 27. Progress: 27%.",
        "log: XGBoost: Trained tree 28. Progress: 28%.",
        "log: XGBoost: Trained tree 29. Progress: 29%.",
        "log: XGBoost: Trained tree 30. Progress: 30%.",
        "log: XGBoost: Trained tree 31. Progress: 31%.",
        "log: XGBoost: Trained tree 32. Progress: 32%.",
        "log: XGBoost: Trained tree 33. Progress: 33%.",
        "log: XGBoost: Trained tree 34. Progress: 34%.",
        "log: XGBoost: Trained tree 35. Progress: 35%.",
        "log: XGBoost: Trained tree 36. Progress: 36%.",
        "log: XGBoost: Trained tree 37. Progress: 37%.",
        "log: XGBoost: Trained tree 38. Progress: 38%.",
        "log: XGBoost: Trained tree 39. Progress: 39%.",
        "log: XGBoost: Trained tree 40. Progress: 40%.",
        "log: XGBoost: Trained tree 41. Progress: 41%.",
        "log: XGBoost: Trained tree 42. Progress: 42%.",
        "log: XGBoost: Trained tree 43. Progress: 43%.",
        "log: XGBoost: Trained tree 44. Progress: 44%.",
        "log: XGBoost: Trained tree 45. Progress: 45%.",
        "log: XGBoost: Trained tree 46. Progress: 46%.",
        "log: XGBoost: Trained tree 47. Progress: 47%.",
        "log: XGBoost: Trained tree 48. Progress: 48%.",
        "log: XGBoost: Trained tree 49. Progress: 49%.",
        "log: XGBoost: Trained tree 50. Progress: 50%.",
        "log: XGBoost: Trained tree 51. Progress: 51%.",
        "log: XGBoost: Trained tree 52. Progress: 52%.",
        "log: XGBoost: Trained tree 53. Progress: 53%.",
        "log: XGBoost: Trained tree 54. Progress: 54%.",
        "log: XGBoost: Trained tree 55. Progress: 55%.",
        "log: XGBoost: Trained tree 56. Progress: 56%.",
        "log: XGBoost: Trained tree 57. Progress: 57%.",
        "log: XGBoost: Trained tree 58. Progress: 58%.",
        "log: XGBoost: Trained tree 59. Progress: 59%.",
        "log: XGBoost: Trained tree 60. Progress: 60%.",
        "log: XGBoost: Trained tree 61. Progress: 61%.",
        "log: XGBoost: Trained tree 62. Progress: 62%.",
        "log: XGBoost: Trained tree 63. Progress: 63%.",
        "log: XGBoost: Trained tree 64. Progress: 64%.",
        "log: XGBoost: Trained tree 65. Progress: 65%.",
        "log: XGBoost: Trained tree 66. Progress: 66%.",
        "log: XGBoost: Trained tree 67. Progress: 67%.",
        "log: XGBoost: Trained tree 68. Progress: 68%.",
        "log: XGBoost: Trained tree 69. Progress: 69%.",
        "log: XGBoost: Trained tree 70. Progress: 70%.",
        "log: XGBoost: Trained tree 71. Progress: 71%.",
        "log: XGBoost: Trained tree 72. Progress: 72%.",
        "log: XGBoost: Trained tree 73. Progress: 73%.",
        "log: XGBoost: Trained tree 74. Progress: 74%.",
        "log: XGBoost: Trained tree 75. Progress: 75%.",
        "log: XGBoost: Trained tree 76. Progress: 76%.",
        "log: XGBoost: Trained tree 77. Progress: 77%.",
        "log: XGBoost: Trained tree 78. Progress: 78%.",
        "log: XGBoost: Trained tree 79. Progress: 79%.",
        "log: XGBoost: Trained tree 80. Progress: 80%.",
        "log: XGBoost: Trained tree 81. Progress: 81%.",
        "log: XGBoost: Trained tree 82. Progress: 82%.",
        "log: XGBoost: Trained tree 83. Progress: 83%.",
        "log: XGBoost: Trained tree 84. Progress: 84%.",
        "log: XGBoost: Trained tree 85. Progress: 85%.",
        "log: XGBoost: Trained tree 86. Progress: 86%.",
        "log: XGBoost: Trained tree 87. Progress: 87%.",
        "log: XGBoost: Trained tree 88. Progress: 88%.",
        "log: XGBoost: Trained tree 89. Progress: 89%.",
        "log: XGBoost: Trained tree 90. Progress: 90%.",
        "log: XGBoost: Trained tree 91. Progress: 91%.",
        "log: XGBoost: Trained tree 92. Progress: 92%.",
        "log: XGBoost: Trained tree 93. Progress: 93%.",
        "log: XGBoost: Trained tree 94. Progress: 94%.",
        "log: XGBoost: Trained tree 95. Progress: 95%.",
        "log: XGBoost: Trained tree 96. Progress: 96%.",
        "log: XGBoost: Trained tree 97. Progress: 97%.",
        "log: XGBoost: Trained tree 98. Progress: 98%.",
        "log: XGBoost: Trained tree 99. Progress: 99%.",
        "log: XGBoost: Trained tree 100. Progress: 100%.",
        "Trained pipeline.",
        "Trained pipeline.",
        "Time taken: 0:00:01.058339.",
        "",
        "^D",
    ]


def pipeline_transform_context_messages() -> Tuple[EventContext, List[str]]:
    return EventContext(
        source=EventSource.ENGINE, cmd={"type_": "Pipeline.transform"}
    ), [
        "log: Staging...",
        "log: Progress: 100%.",
        "log: Preprocessing...",
        "log: Progress: 100%.",
        "^D",
    ]


@pytest.fixture(
    params=[pipeline_fit_context_messages(), pipeline_transform_context_messages()]
)
def pipeline_context(request):
    context, messages = request.param
    return context, MockSocket(messages)
