import pytest

from getml.utilities.progress import Progress, ProgressTask


def test_exception_in_progress_bar(capsys):
    with pytest.raises(ValueError):
        with Progress() as progress:
            progress.add_task("Test")
            raise ValueError("Test")

    captured = capsys.readouterr()
    assert "Failed" in captured.out


@pytest.mark.parametrize(
    "finish_all_tasks_on_stop, expected",
    [(True, "100%"), (False, "0%")],
)
def test_finished_on_stop(finish_all_tasks_on_stop: bool, expected: str, capsys):
    with Progress(finish_all_tasks_on_stop=finish_all_tasks_on_stop) as progress:
        progress.add_task("Test")
        assert all(not task.finished for task in progress.tasks)

    captured = capsys.readouterr()
    assert expected in captured.out

    with ProgressTask("Test") as progress_task:
        assert progress_task.finished is False

    captured = capsys.readouterr()
    assert expected in captured.out


def test_progress_task_single_task():
    with ProgressTask("Test") as progress_task:
        assert len(progress_task.progress.tasks) == 1
        assert progress_task.id == 0
        assert progress_task.total == 100
        assert progress_task.completed == 0
        assert progress_task.description == "Test"
        assert progress_task.finished is False


def test_progress_task_reset_description(capsys):
    with ProgressTask("Test") as progress_task:
        progress_task.update(description="Toast")
        assert progress_task.task.description == "Toast"
        assert progress_task.description == "Test"
        captured = capsys.readouterr()
        assert "Toast" in captured.out
    assert progress_task.task.description == "Test"
    assert progress_task.description == "Test"
    captured = capsys.readouterr()
    assert "Test" in captured.out


def test_progress_advance(capsys):
    with Progress(finish_all_tasks_on_stop=False) as progress:
        task_id = progress.add_task("Test")
        progress.advance(task_id, steps=50)
        assert progress.tasks[task_id].completed == 50

    captured = capsys.readouterr()
    assert "50%" in captured.out

    with ProgressTask("Test", finish_on_stop=False) as progress_task:
        progress_task.advance(steps=50)
        assert progress_task.completed == 50

    captured = capsys.readouterr()
    assert "50%" in captured.out
