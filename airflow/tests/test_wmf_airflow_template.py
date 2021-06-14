from airflow.models import TaskInstance
from airflow.operators import DummyOperator
from datetime import datetime

from wmf_airflow.template import DagConf, LazyJsonVariableAccessor, TemplatedSeq


def test_LazyJsonVariableAccessor(mock_airflow_variables):
    mock_airflow_variables({
        'pytest_var': {
            'a': 'aaa',
            'b': '123',
        },
    })

    # Simple property access
    accessor = LazyJsonVariableAccessor('pytest_var')
    assert accessor.a == 'aaa'
    assert accessor.b == '123'

    # Failures must be attribute errors
    try:
        accessor.c
    except AttributeError:
        pass
    else:
        assert False, 'Expected AttributeError'


def test_DagConf(mock_airflow_variables):
    mock_airflow_variables({
        'pytest_var': {
            'a': 'aaa',
        },
    })
    conf = DagConf('pytest_var')
    # Proper template output
    assert conf('a') == '{{ var.json.pytest_var.a }}'
    # macro expands to correct variable
    assert conf.macro.a == 'aaa'


def test_TemplatedSeq(dag, mock_airflow_variables):
    mock_airflow_variables({
        'pytest_var': {
            'baz': [1, 2],
            'q': 4,
        },
    })

    def render(var):
        op = DummyOperator(task_id='dummy', dag=dag)
        op.template_fields = ('templated',)
        op.templated = var
        TaskInstance(op, datetime(2001, 1, 15)).render_templates()
        return list(op.templated)

    # Simplest usage
    output = render(TemplatedSeq('hello,world'))
    assert output == ['hello', 'world']

    # Passing a transform fn
    output = render(TemplatedSeq.for_var(
        'var.json.pytest_var.baz',
        fn=lambda x: 'q=4/x=' + x))
    assert output == ['q=4/x=1', 'q=4/x=2']

    # Passing arguments to transform fn
    output = render(TemplatedSeq.for_var(
        'var.json.pytest_var.baz',
        fn_args=(4,),
        fn=lambda x, q: 'q={}/x={}'.format(q, x)))
    assert output == ['q=4/x=1', 'q=4/x=2']

    # Passing templated arguments to transform fn
    output = render(TemplatedSeq.for_var(
        'var.json.pytest_var.baz',
        fn_args=('{{ var.json.pytest_var.q }}',),
        fn=lambda x, q: 'q={}/x={}'.format(q, x)))
    assert output == ['q=4/x=1', 'q=4/x=2']
