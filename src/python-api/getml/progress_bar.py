"""Displays progress in bar form."""

import time


class _ProgressBar:
    """Displays progress in bar form."""

    def __init__(self, description: str = ""):
        self.description = description
        self.length = 10
        self.progress = 0.0
        self.begin = time.time()

        self.show(0)

    def close(self):
        """
        Closes the progress bar so we can display
        the next thing.
        """
        self.show(100)
        print()

    def show(self, progress: float):
        """Displays an updated version of the progress bar.
        Args:
            progress (float): The progress to be shown. Must
                be between 0 and 100.
        """
        elapsed_time = time.time() - self.begin
        done = int(progress * self.length / 100.0)

        if progress <= self.progress and progress != 0.0:
            return

        self.progress = progress

        remaining = self.length - done
        filler = " " if progress < 100.0 else ""

        pbar = self.description + " "
        pbar += filler + str(int(progress)) + "%"
        pbar += " |" + "█" * done
        pbar += " " * remaining + "| "
        pbar += "[elapsed: " + _make_time_string(elapsed_time) + ","
        pbar += " remaining: " + _calc_remaining(elapsed_time, progress) + "]"
        pbar += " " * 10

        end = "\r"

        print(pbar, end=end, flush=True)


def _calc_remaining(elapsed_time: float, progress: float) -> str:
    if progress == 0.0:
        return "?"
    estimate = (100.0 - progress) * (elapsed_time / progress)
    return _make_time_string(estimate)


def _make_time_string(seconds: float) -> str:

    hours = int(seconds / 3600)
    seconds -= float(hours * 3600)

    minutes = int(seconds / 60)
    seconds -= float(minutes * 60)

    seconds = int(round(seconds))

    if hours > 0:
        return _to_str(hours) + ":" + _to_str(minutes) + ":" + _to_str(seconds)

    return _to_str(minutes) + ":" + _to_str(seconds)


def _to_str(num: int) -> str:
    if num > 9:
        return str(num)
    return "0" + str(num)
