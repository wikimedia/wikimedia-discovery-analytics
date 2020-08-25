from wmf_airflow.hdfs_cli import HdfsCliHook, HdfsCliSensor


def test_sensor(mocker):
    filesystem = [
        "/base/20200601/_IMPORTED",
    ]

    def exists_side_effect(filepath):
        return filepath in filesystem

    mocker.patch.object(HdfsCliHook, 'exists').side_effect = exists_side_effect
    sensor1 = HdfsCliSensor(task_id="import1", filepath="/base/20200601/_IMPORTED")
    sensor2 = HdfsCliSensor(task_id="import2", filepath="/base/20200602/_IMPORTED")
    assert sensor1.poke({})
    assert not sensor2.poke({})
    filesystem.append("/base/20200602/_IMPORTED")
    assert sensor2.poke({})
