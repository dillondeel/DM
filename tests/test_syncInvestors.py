import os
import csv
import tempfile
from unittest.mock import patch, MagicMock

import pytest

import syncInvestors


class TestFetchLinkedTable:
    def test_single_page(self):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "records": [
                {"id": "rec1", "fields": {"Name": "Alpha"}},
                {"id": "rec2", "fields": {"Name": "Beta"}},
            ]
        }
        with patch("syncInvestors.requests.get", return_value=mock_response) as mock_get:
            result = syncInvestors.fetch_linked_table(
                "https://api.airtable.com/v0/base/table", {"Authorization": "Bearer key"}
            )
        assert len(result) == 2
        assert result[0]["id"] == "rec1"
        mock_get.assert_called_once()

    def test_multiple_pages(self):
        page1 = MagicMock()
        page1.json.return_value = {
            "records": [{"id": "rec1", "fields": {"Name": "Alpha"}}],
            "offset": "page2_offset",
        }
        page2 = MagicMock()
        page2.json.return_value = {
            "records": [{"id": "rec2", "fields": {"Name": "Beta"}}],
        }
        with patch("syncInvestors.requests.get", side_effect=[page1, page2]) as mock_get:
            result = syncInvestors.fetch_linked_table(
                "https://api.airtable.com/v0/base/table", {"Authorization": "Bearer key"}
            )
        assert len(result) == 2
        assert result[0]["id"] == "rec1"
        assert result[1]["id"] == "rec2"
        assert mock_get.call_count == 2

    def test_empty_response(self):
        mock_response = MagicMock()
        mock_response.json.return_value = {"records": []}
        with patch("syncInvestors.requests.get", return_value=mock_response):
            result = syncInvestors.fetch_linked_table(
                "https://api.airtable.com/v0/base/table", {"Authorization": "Bearer key"}
            )
        assert result == []

    def test_pagination_params_passed(self):
        page1 = MagicMock()
        page1.json.return_value = {
            "records": [{"id": "rec1", "fields": {}}],
            "offset": "next",
        }
        page2 = MagicMock()
        page2.json.return_value = {"records": [{"id": "rec2", "fields": {}}]}
        with patch("syncInvestors.requests.get", side_effect=[page1, page2]) as mock_get:
            syncInvestors.fetch_linked_table("http://url", {"Authorization": "Bearer k"})
        first_call_params = mock_get.call_args_list[0][1]["params"]
        second_call_params = mock_get.call_args_list[1][1]["params"]
        assert first_call_params == {}
        assert second_call_params == {"offset": "next"}


class TestBuildLinkedData:
    def test_funded_rounds_mapping(self):
        records = [
            {"id": "rec1", "fields": {"Fundraising Round": "Series A"}},
            {"id": "rec2", "fields": {"Fundraising Round": "Series B"}},
        ]
        with patch("syncInvestors.fetch_linked_table", return_value=records):
            result = syncInvestors.build_linked_data(
                {"Funded Rounds": "Fundraising%20Rounds%20-%20Companies"},
                "base123",
                {"Authorization": "Bearer key"},
            )
        assert result["Funded Rounds"] == {"rec1": "Series A", "rec2": "Series B"}

    def test_companies_funded_mapping(self):
        records = [
            {"id": "rec1", "fields": {"Company": "Acme Corp"}},
            {"id": "rec2", "fields": {"Company": "Beta Inc"}},
        ]
        with patch("syncInvestors.fetch_linked_table", return_value=records):
            result = syncInvestors.build_linked_data(
                {"Companies Funded": "Companies"},
                "base123",
                {"Authorization": "Bearer key"},
            )
        assert result["Companies Funded"] == {"rec1": "Acme Corp", "rec2": "Beta Inc"}

    def test_fallback_to_id_when_field_missing(self):
        records = [
            {"id": "rec1", "fields": {}},
        ]
        with patch("syncInvestors.fetch_linked_table", return_value=records):
            result = syncInvestors.build_linked_data(
                {"Funded Rounds": "SomeTable"},
                "base123",
                {"Authorization": "Bearer key"},
            )
        assert result["Funded Rounds"] == {"rec1": "rec1"}

    def test_empty_linked_table(self):
        with patch("syncInvestors.fetch_linked_table", return_value=[]):
            result = syncInvestors.build_linked_data(
                {"Funded Rounds": "SomeTable"},
                "base123",
                {"Authorization": "Bearer key"},
            )
        assert result["Funded Rounds"] == {}


class TestFetchMainTable:
    def test_single_page(self):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "records": [{"id": "rec1", "fields": {"Fund": "VC Fund"}}]
        }
        with patch("syncInvestors.requests.get", return_value=mock_response):
            result = syncInvestors.fetch_main_table("http://url", {"Authorization": "Bearer k"})
        assert len(result) == 1
        assert result[0]["fields"]["Fund"] == "VC Fund"

    def test_multiple_pages(self):
        page1 = MagicMock()
        page1.json.return_value = {
            "records": [{"id": "rec1", "fields": {"Fund": "Fund A"}}],
            "offset": "page2",
        }
        page2 = MagicMock()
        page2.json.return_value = {
            "records": [{"id": "rec2", "fields": {"Fund": "Fund B"}}],
        }
        with patch("syncInvestors.requests.get", side_effect=[page1, page2]):
            result = syncInvestors.fetch_main_table("http://url", {"Authorization": "Bearer k"})
        assert len(result) == 2


class TestTransformRecords:
    def test_resolves_linked_list_ids(self):
        records = [
            {"id": "rec1", "fields": {"Fund": "Alpha", "Funded Rounds": ["lr1", "lr2"]}}
        ]
        linked_tables = {"Funded Rounds": "FRTable"}
        linked_data = {"Funded Rounds": {"lr1": "Series A", "lr2": "Series B"}}
        fields, transformed = syncInvestors.transform_records(records, linked_tables, linked_data)
        assert transformed[0]["Funded Rounds"] == "Series A, Series B"

    def test_resolves_linked_scalar_id(self):
        records = [
            {"id": "rec1", "fields": {"Fund": "Alpha", "Companies Funded": "c1"}}
        ]
        linked_tables = {"Companies Funded": "Companies"}
        linked_data = {"Companies Funded": {"c1": "Acme Corp"}}
        fields, transformed = syncInvestors.transform_records(records, linked_tables, linked_data)
        assert transformed[0]["Companies Funded"] == "Acme Corp"

    def test_handles_empty_linked_value(self):
        records = [
            {"id": "rec1", "fields": {"Fund": "Alpha", "Companies Funded": ""}}
        ]
        linked_tables = {"Companies Funded": "Companies"}
        linked_data = {"Companies Funded": {"c1": "Acme"}}
        fields, transformed = syncInvestors.transform_records(records, linked_tables, linked_data)
        assert transformed[0]["Companies Funded"] == ""

    def test_unique_companies_list(self):
        records = [
            {"id": "rec1", "fields": {"Unique Companies": ["Acme", "Beta"]}}
        ]
        fields, transformed = syncInvestors.transform_records(records, {}, {})
        assert transformed[0]["Unique Companies"] == "Acme, Beta"

    def test_unique_companies_string(self):
        records = [
            {"id": "rec1", "fields": {"Unique Companies": "Acme, Beta"}}
        ]
        fields, transformed = syncInvestors.transform_records(records, {}, {})
        assert transformed[0]["Unique Companies"] == "Acme, Beta"

    def test_unknown_linked_id_preserved(self):
        records = [
            {"id": "rec1", "fields": {"Funded Rounds": ["unknown_id"]}}
        ]
        linked_tables = {"Funded Rounds": "FRTable"}
        linked_data = {"Funded Rounds": {"known_id": "Series A"}}
        fields, transformed = syncInvestors.transform_records(records, linked_tables, linked_data)
        assert transformed[0]["Funded Rounds"] == "unknown_id"

    def test_fields_collected_from_all_records(self):
        records = [
            {"id": "rec1", "fields": {"Fund": "Alpha", "Type": "VC"}},
            {"id": "rec2", "fields": {"Fund": "Beta", "Stage": "Early"}},
        ]
        fields, transformed = syncInvestors.transform_records(records, {}, {})
        assert "Fund" in fields
        assert "Type" in fields
        assert "Stage" in fields

    def test_empty_records(self):
        fields, transformed = syncInvestors.transform_records([], {}, {})
        assert fields == []
        assert transformed == []


class TestWriteCsv:
    def test_writes_valid_csv(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            output_path = f.name
        try:
            fields = ["Name", "Type"]
            records = [{"Name": "Alpha", "Type": "VC"}, {"Name": "Beta", "Type": "PE"}]
            syncInvestors.write_csv(fields, records, output_path)
            with open(output_path, "r") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
            assert len(rows) == 2
            assert rows[0]["Name"] == "Alpha"
            assert rows[1]["Type"] == "PE"
        finally:
            os.unlink(output_path)

    def test_handles_empty_records(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            output_path = f.name
        try:
            fields = ["Name"]
            syncInvestors.write_csv(fields, [], output_path)
            with open(output_path, "r") as f:
                content = f.read()
            assert "Name" in content
            lines = content.strip().split("\n")
            assert len(lines) == 1  # header only
        finally:
            os.unlink(output_path)


class TestMain:
    @patch("syncInvestors.write_csv")
    @patch("syncInvestors.transform_records")
    @patch("syncInvestors.fetch_main_table")
    @patch("syncInvestors.build_linked_data")
    def test_main_orchestration(self, mock_build, mock_fetch, mock_transform, mock_write):
        mock_build.return_value = {"Funded Rounds": {}}
        mock_fetch.return_value = [{"id": "rec1", "fields": {"Fund": "Test"}}]
        mock_transform.return_value = (["Fund"], [{"Fund": "Test"}])
        syncInvestors.main()
        mock_build.assert_called_once()
        mock_fetch.assert_called_once()
        mock_transform.assert_called_once()
        mock_write.assert_called_once_with(["Fund"], [{"Fund": "Test"}], "data/Investors.csv")

    @patch("syncInvestors.write_csv")
    @patch("syncInvestors.transform_records")
    @patch("syncInvestors.fetch_main_table")
    @patch("syncInvestors.build_linked_data")
    def test_main_empty_records(self, mock_build, mock_fetch, mock_transform, mock_write):
        mock_build.return_value = {}
        mock_fetch.return_value = []
        mock_transform.return_value = ([], [])
        syncInvestors.main()
        mock_write.assert_called_once_with([], [], "data/Investors.csv")
