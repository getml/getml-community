import pytest

from getml.utilities.progress import Progress


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
    assert all(task.finished == finish_all_tasks_on_stop for task in progress.tasks)
    assert expected in captured.out


@pytest.mark.parametrize(
    "finish_all_tasks_on_stop, expected_final_description",
    [(True, "Test"), (False, "Toast")],
)
def test_progress_reset_description(
    capsys, finish_all_tasks_on_stop: bool, expected_final_description: str
):
    with Progress(finish_all_tasks_on_stop=finish_all_tasks_on_stop) as progress:
        task_id = progress.add_task("Test")
        progress.update(task_id, description="Toast")
        assert progress.tasks[task_id].description.lstrip().startswith("Toast")
        assert progress.descriptions[task_id].lstrip().startswith("Test")
    assert (
        progress.tasks[task_id]
        .description.lstrip()
        .startswith(expected_final_description)
    )
    captured = capsys.readouterr()
    assert expected_final_description in captured.out
