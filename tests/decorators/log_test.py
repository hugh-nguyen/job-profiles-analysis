import pytest, logging
from modules.decorators import log


def test_logging(caplog):
    @log
    def my_function():
        return "Hello, World!"

    # Enable capturing of logs
    caplog.set_level(logging.INFO)

    # Call the function
    result = my_function()

    # Get the captured logs
    logs = [record.message for record in caplog.records]
    print(logs)

    # Assert the number of logs
    assert len(logs) == 2

    # Assert the log messages
    assert logs[0] == 'Started executing my_function'
    assert logs[1] == 'Finished executing my_function'


def test_logging_with_exception(caplog):
    @log
    def my_function():
        raise ValueError("Something went wrong")

    # Enable capturing of logs
    caplog.set_level(logging.ERROR)

    # Call the function and expect an exception
    with pytest.raises(ValueError):
        my_function()

    # Get the captured logs
    logs = [record.message for record in caplog.records]

    # Assert the number of logs
    assert len(logs) == 1

    # Assert the log messages
    assert logs[0] == 'Something went wrong'
