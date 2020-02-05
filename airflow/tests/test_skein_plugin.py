from airflow.operators.skein_plugin import SkeinOperator


def test_operator_can_create_hook():
    task = SkeinOperator(
        task_id='can_create_hook',
        application='pytest.py')
    assert task._make_hook() is not None
