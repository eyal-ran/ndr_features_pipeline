from ndr.config.project_parameters_loader import load_project_parameters


class DummyTable:
    def __init__(self, item):
        self._item = item

    def get_item(self, Key):
        return {"Item": self._item}


class DummyDDBResource:
    def __init__(self, item):
        self._item = item

    def Table(self, name):
        return DummyTable(self._item)


def test_load_project_parameters(monkeypatch):
    payload = {"ip_machine_mapping_s3_prefix": "s3://bucket/mapping/"}
    item = {"spec": payload}

    def fake_resource(name):
        assert name == "dynamodb"
        return DummyDDBResource(item)

    monkeypatch.setattr("boto3.resource", fake_resource)

    params = load_project_parameters(
        project_name="proj1",
        feature_spec_version="v1",
        table_name="dummy-table",
    )
    assert params["ip_machine_mapping_s3_prefix"] == "s3://bucket/mapping/"
