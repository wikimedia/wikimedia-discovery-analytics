import populate_integration
import pytest


@pytest.mark.parametrize('expect_retval,rows', [
    (0, [{'id': 5}]),
    (1, [{'id': 2, 'extra': 'qqq'}])
])
def test_failures_propagate_to_exit_code(expect_retval, rows, spark, mocker):
    mock_spark = mocker.MagicMock()
    mock_spark.createDataFrame.side_effect = spark.createDataFrame
    mock_spark.read.table.return_value = spark.range(0)

    # Avoid actual writes in spark, table doesnt exist anyways
    mocker.patch.object(populate_integration.HivePartitionWriter, 'overwrite_with')

    retval = populate_integration.main([
        (
            '/path/to/conf',
            {'partition': 'pytest/', 'rows': rows}
        ),
    ], mock_spark)
    assert retval == expect_retval


def test_type_mismatch_fails(spark, mocker):
    mock_spark = mocker.MagicMock()
    mock_spark.createDataFrame.side_effect = spark.createDataFrame
    mock_spark.read.table.return_value = spark.range(0)
    writer = mocker.MagicMock()

    try:
        populate_integration.import_rows(
            mock_spark, writer, [{'id': 'qqq'}])
    except TypeError as e:
        assert "can not accept object 'qqq'" in str(e), e
    else:
        assert False, "Importing rows with mismatched types must throw TypeError"
