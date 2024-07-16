import pytest
from getml.exceptions import (
    ENTERPRISE_FEATURE_NOT_AVAILABLE_TEMPLATE,
    handle_engine_exception,
)


def test_handle_enterprise_feature_not_available_error():
    engine_error_message = (
        "The "
        "__name__"
        " "
        "__type__"
        " is not supported in the community edition. Please "
        "upgrade to getML enterprise to use this. An overview of what is "
        "supported in the community edition can be found in the official "
        "getML documentation."
    )

    with pytest.raises(OSError) as excinfo:
        handle_engine_exception(engine_error_message)

        assert str(excinfo.value) == ENTERPRISE_FEATURE_NOT_AVAILABLE_TEMPLATE.format(
            missing_feature_name="__name__", missing_feature_type="__type__"
        )
