from wmf_airflow.template import DagConf, LazyJsonVariableAccessor


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
