import pytest
from getml.progress_bar import _Progress


def test_exception_in_progress_bar(capsys):
    with pytest.raises(ValueError):
        with _Progress() as progress:
            progress.new("Test", 100)
            raise ValueError("Test")

    captured = capsys.readouterr()
    assert "Test Failed" in captured.out


@pytest.mark.parametrize(
    "set_finished_on_exit, expected",
    [(True, "100%"), (False, "0%")],
)
def test_finished_on_clode(set_finished_on_exit: bool, expected: str, capsys):
    with _Progress(set_finished_on_exit=set_finished_on_exit) as progress:
        progress.new("Test", 100)

    captured = capsys.readouterr()
    assert expected in captured.out


def test_complete_previous_on_new():
    with _Progress() as progress:
        task_id_0 = progress.new(
            "Test0",
        )
        task_id_1 = progress.new(
            "Test1",
        )

        assert progress._progress.tasks[task_id_0].finished
        assert not progress._progress.tasks[task_id_1].finished
