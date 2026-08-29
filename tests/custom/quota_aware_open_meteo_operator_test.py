from datetime import timedelta
from unittest.mock import Mock

import pytest
from airflow.sdk.exceptions import AirflowException, TaskDeferred
from airflow.providers.standard.triggers.temporal import TimeDeltaTrigger

from dags.utils.custom.operators import (
    QuotaAwareOpenMeteoExtractionOperator,
)


def create_operator(callable_mock, parquet_paths=None):
    """Create an operator instance for testing."""
    return QuotaAwareOpenMeteoExtractionOperator(
        task_id="test_extraction",
        python_callable=callable_mock,
        period=["2026-01-01", "2026-01-02"],
        parquet_paths=parquet_paths or ["cities_0.parquet"],
    )


def test_operator_initialization():
    callable_mock = Mock()

    operator = create_operator(callable_mock)

    assert operator.python_callable == callable_mock
    assert operator.period == ["2026-01-01", "2026-01-02"]
    assert operator.parquet_paths == ["cities_0.parquet"]


def test_successful_extraction():
    callable_mock = Mock(return_value="output.parquet")
    operator = create_operator(callable_mock)

    result = operator.execute(context={})

    assert result == ["output.parquet"]

    callable_mock.assert_called_once_with(
        period=["2026-01-01", "2026-01-02"],
        cities_chunk_path="cities_0.parquet",
    )


def test_multiple_paths_are_processed():
    callable_mock = Mock(
        side_effect=[
            "output_0.parquet",
            "output_1.parquet",
            "output_2.parquet",
        ]
    )

    operator = create_operator(
        callable_mock,
        parquet_paths=[
            "cities_0.parquet",
            "cities_1.parquet",
            "cities_2.parquet",
        ],
    )

    result = operator.execute(context={})

    assert result == [
        "output_0.parquet",
        "output_1.parquet",
        "output_2.parquet",
    ]

    assert callable_mock.call_count == 3


def test_minutely_quota_defers():
    callable_mock = Mock(
        side_effect=Exception(
            "Minutely API request limit exceeded"
        )
    )

    operator = create_operator(callable_mock)

    with pytest.raises(TaskDeferred) as exc_info:
        operator.execute(context={})

    assert isinstance(exc_info.value.trigger, TimeDeltaTrigger)

    assert exc_info.value.kwargs == {
        "current_path_index": 0,
        "completed_paths": [],
    }


def test_hourly_quota_defers():
    callable_mock = Mock(
        side_effect=Exception(
            "Hourly API request limit exceeded"
        )
    )

    operator = create_operator(callable_mock)

    with pytest.raises(TaskDeferred) as exc_info:
        operator.execute(context={})

    assert isinstance(exc_info.value.trigger, TimeDeltaTrigger)

    assert exc_info.value.kwargs == {
        "current_path_index": 0,
        "completed_paths": [],
    }


def test_daily_quota_defers():
    callable_mock = Mock(
        side_effect=Exception(
            "Daily API request limit exceeded"
        )
    )

    operator = create_operator(callable_mock)

    with pytest.raises(TaskDeferred) as exc_info:
        operator.execute(context={})

    assert isinstance(exc_info.value.trigger, TimeDeltaTrigger)

    assert exc_info.value.kwargs == {
        "current_path_index": 0,
        "completed_paths": [],
    }


def test_non_quota_error_raises_airflow_exception():
    callable_mock = Mock(
        side_effect=Exception("API connection failed")
    )

    operator = create_operator(callable_mock)

    with pytest.raises(AirflowException):
        operator.execute(context={})


def test_resume_from_current_path():
    callable_mock = Mock(
        side_effect=[
            "output_1.parquet",
            "output_2.parquet",
        ]
    )

    operator = create_operator(
        callable_mock,
        parquet_paths=[
            "cities_0.parquet",
            "cities_1.parquet",
            "cities_2.parquet",
        ],
    )

    result = operator.execute(
        context={},
        current_path_index=1,
        completed_paths=["output_0.parquet"],
    )

    assert result == [
        "output_0.parquet",
        "output_1.parquet",
        "output_2.parquet",
    ]

    assert callable_mock.call_count == 2

    callable_mock.assert_any_call(
        period=["2026-01-01", "2026-01-02"],
        cities_chunk_path="cities_1.parquet",
    )

    callable_mock.assert_any_call(
        period=["2026-01-01", "2026-01-02"],
        cities_chunk_path="cities_2.parquet",
    )


def test_completed_paths_preserved_when_deferring():
    callable_mock = Mock(
        side_effect=[
            "output_0.parquet",
            Exception("Minutely API request limit exceeded"),
        ]
    )

    operator = create_operator(
        callable_mock,
        parquet_paths=[
            "cities_0.parquet",
            "cities_1.parquet",
            "cities_2.parquet",
        ],
    )

    with pytest.raises(TaskDeferred) as exc_info:
        operator.execute(context={})

    # cities_0 completed successfully before quota was hit
    assert exc_info.value.kwargs == {
        "current_path_index": 1,
        "completed_paths": ["output_0.parquet"],
    }

    assert callable_mock.call_count == 2