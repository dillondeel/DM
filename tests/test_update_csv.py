import os
import csv
import tempfile
from unittest.mock import patch, MagicMock

import pytest

import update_csv


class TestFetchLinkedTable:
    def test_single_page(self):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "records": [
                {"id": "rec1", "fields": {"Company": "Acme"}},
                {"id": "rec2", "fields": {"Company": "Beta"}},
            ]
        }
        with patch("update_csv.requests.get", return_value=mock_response) as mock_get:
            result = update_csv.fetch_linked_table(
                "https://api.airtable.com/v0/base/table", {"Authorization": "Bearer key"}
            )
        assert len(result) == 2
        assert result[0]["fields"]["Company"] == "Acme"
        mock_get.assert_called_once()

    def test_multiple_pages(self):
        page1 = MagicMock()
        page1.json.return_value = {
            "records": [{"id": "rec1", "fields": {"Company": "Acme"}}],
            "offset": "next_page",
        }
        page2 = MagicMock()
        page2.json.return_value = {
            "records": [{"id": "rec2", "fields": {"Company": "Beta"}}],
        }
        with patch("update_csv.requests.get", side_effect=[page1, page2]) as mock_get:
            result = update_csv.fetch_linked_table("http://url", {"Authorization": "Bearer k"})
        assert len(result) == 2
        assert mock_get.call_count == 2

    def test_empty_table(self):
        mock_response = MagicMock()
        mock_response.json.return_value = {"records": []}
        with patch("update_csv.requests.get", return_value=mock_response):
            result = update_csv.fetch_linked_table("http://url", {"Authorization": "Bearer k"})
        assert result == []


class TestBuildLinkedData:
    def test_investors_mapping(self):
        records = [
            {"id": "inv1", "fields": {"Fund": "VC Fund A"}},
            {"id": "inv2", "fields": {"Fund": "PE Fund B"}},
        ]
        with patch("update_csv.fetch_linked_table", return_value=records):
            result = update_csv.build_linked_data(
                {"Investors": "Investors"}, "base123", {"Authorization": "Bearer key"}
            )
        assert result["Investors"] == {"inv1": "VC Fund A", "inv2": "PE Fund B"}

    def test_sector_mapping(self):
        records = [
            {"id": "sec1", "fields": {"Sector": "Tech"}},
            {"id": "sec2", "fields": {"Sector": "Health"}},
        ]
        with patch("update_csv.fetch_linked_table", return_value=records):
            result = update_csv.build_linked_data(
                {"Sector": "Sectors"}, "base123", {"Authorization": "Bearer key"}
            )
        assert result["Sector"] == {"sec1": "Tech", "sec2": "Health"}

    def test_category_mapping(self):
        records = [
            {"id": "cat1", "fields": {"Category": "SaaS"}},
        ]
        with patch("update_csv.fetch_linked_table", return_value=records):
            result = update_csv.build_linked_data(
                {"Category": "Categories"}, "base123", {"Authorization": "Bearer key"}
            )
        assert result["Category"] == {"cat1": "SaaS"}

    def test_company_mapping(self):
        records = [
            {"id": "comp1", "fields": {"Company": "Acme Corp"}},
        ]
        with patch("update_csv.fetch_linked_table", return_value=records):
            result = update_csv.build_linked_data(
                {"Company": "Companies"}, "base123", {"Authorization": "Bearer key"}
            )
        assert result["Company"] == {"comp1": "Acme Corp"}

    def test_default_name_mapping(self):
        records = [
            {"id": "rec1", "fields": {"Name": "Something"}},
        ]
        with patch("update_csv.fetch_linked_table", return_value=records):
            result = update_csv.build_linked_data(
                {"UnknownField": "SomeTable"}, "base123", {"Authorization": "Bearer key"}
            )
        assert result["UnknownField"] == {"rec1": "Something"}

    def test_fallback_to_id_when_field_missing(self):
        records = [{"id": "rec1", "fields": {}}]
        with patch("update_csv.fetch_linked_table", return_value=records):
            result = update_csv.build_linked_data(
                {"Investors": "Investors"}, "base123", {"Authorization": "Bearer key"}
            )
        assert result["Investors"] == {"rec1": "rec1"}

    def test_empty_linked_table(self):
        with patch("update_csv.fetch_linked_table", return_value=[]):
            result = update_csv.build_linked_data(
                {"Company": "Companies"}, "base123", {"Authorization": "Bearer key"}
            )
        assert result["Company"] == {}


class TestFetchMainTable:
    def test_single_page(self):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "records": [{"id": "rec1", "fields": {"Fundraising Round": "Series A"}}]
        }
        with patch("update_csv.requests.get", return_value=mock_response):
            result = update_csv.fetch_main_table("http://url", {"Authorization": "Bearer k"})
        assert len(result) == 1

    def test_multiple_pages(self):
        page1 = MagicMock()
        page1.json.return_value = {
            "records": [{"id": "rec1", "fields": {"Round": "A"}}],
            "offset": "p2",
        }
        page2 = MagicMock()
        page2.json.return_value = {
            "records": [{"id": "rec2", "fields": {"Round": "B"}}],
        }
        with patch("update_csv.requests.get", side_effect=[page1, page2]):
            result = update_csv.fetch_main_table("http://url", {"Authorization": "Bearer k"})
        assert len(result) == 2


class TestTransformRecords:
    def test_resolves_linked_list_ids(self):
        records = [
            {"id": "rec1", "fields": {"Round": "A", "Investors": ["inv1", "inv2"]}}
        ]
        linked_tables = {"Investors": "Investors"}
        linked_data = {"Investors": {"inv1": "Fund A", "inv2": "Fund B"}}
        fields, transformed = update_csv.transform_records(records, linked_tables, linked_data)
        assert transformed[0]["Investors"] == "Fund A, Fund B"

    def test_filters_unknown_ids_in_list(self):
        records = [
            {"id": "rec1", "fields": {"Investors": ["inv1", "unknown_id"]}}
        ]
        linked_tables = {"Investors": "Investors"}
        linked_data = {"Investors": {"inv1": "Fund A"}}
        fields, transformed = update_csv.transform_records(records, linked_tables, linked_data)
        assert transformed[0]["Investors"] == "Fund A"

    def test_resolves_scalar_linked_id(self):
        records = [
            {"id": "rec1", "fields": {"Company": "comp1"}}
        ]
        linked_tables = {"Company": "Companies"}
        linked_data = {"Company": {"comp1": "Acme Corp"}}
        fields, transformed = update_csv.transform_records(records, linked_tables, linked_data)
        assert transformed[0]["Company"] == "Acme Corp"

    def test_removes_unwanted_columns(self):
        records = [
            {
                "id": "rec1",
                "fields": {
                    "Round": "A",
                    "Source": "manual",
                    "Sector (from Company)": "Tech",
                    "Category (from Company)": "SaaS",
                },
            }
        ]
        fields, transformed = update_csv.transform_records(records, {}, {})
        assert "Source" not in transformed[0]
        assert "Sector (from Company)" not in transformed[0]
        assert "Category (from Company)" not in transformed[0]
        assert "Source" not in fields
        assert "Sector (from Company)" not in fields
        assert "Category (from Company)" not in fields

    def test_preserves_non_linked_fields(self):
        records = [
            {"id": "rec1", "fields": {"Round": "Series A", "Amount": "10M"}}
        ]
        fields, transformed = update_csv.transform_records(records, {}, {})
        assert transformed[0]["Round"] == "Series A"
        assert transformed[0]["Amount"] == "10M"

    def test_empty_list_linked_field(self):
        records = [
            {"id": "rec1", "fields": {"Investors": []}}
        ]
        linked_tables = {"Investors": "Investors"}
        linked_data = {"Investors": {"inv1": "Fund A"}}
        fields, transformed = update_csv.transform_records(records, linked_tables, linked_data)
        assert transformed[0]["Investors"] == ""

    def test_fields_collected_from_multiple_records(self):
        records = [
            {"id": "rec1", "fields": {"Round": "A", "Amount": "5M"}},
            {"id": "rec2", "fields": {"Round": "B", "Stage": "Seed"}},
        ]
        fields, transformed = update_csv.transform_records(records, {}, {})
        assert "Round" in fields
        assert "Amount" in fields
        assert "Stage" in fields

    def test_empty_records(self):
        fields, transformed = update_csv.transform_records([], {}, {})
        assert fields == []
        assert transformed == []


class TestWriteCsv:
    def test_writes_valid_csv(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            output_path = f.name
        try:
            fields = ["Round", "Amount"]
            records = [
                {"Round": "Series A", "Amount": "10M"},
                {"Round": "Series B", "Amount": "25M"},
            ]
            update_csv.write_csv(fields, records, output_path)
            with open(output_path, "r") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
            assert len(rows) == 2
            assert rows[0]["Round"] == "Series A"
            assert rows[1]["Amount"] == "25M"
        finally:
            os.unlink(output_path)

    def test_handles_empty_records(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            output_path = f.name
        try:
            update_csv.write_csv(["Col1"], [], output_path)
            with open(output_path, "r") as f:
                content = f.read()
            assert "Col1" in content
            lines = content.strip().split("\n")
            assert len(lines) == 1
        finally:
            os.unlink(output_path)


class TestMain:
    @patch("update_csv.write_csv")
    @patch("update_csv.transform_records")
    @patch("update_csv.fetch_main_table")
    @patch("update_csv.build_linked_data")
    def test_main_orchestration(self, mock_build, mock_fetch, mock_transform, mock_write):
        mock_build.return_value = {"Company": {}}
        mock_fetch.return_value = [{"id": "rec1", "fields": {"Round": "A"}}]
        mock_transform.return_value = (["Round"], [{"Round": "A"}])
        update_csv.main()
        mock_build.assert_called_once()
        mock_fetch.assert_called_once()
        mock_transform.assert_called_once()
        mock_write.assert_called_once_with(
            ["Round"], [{"Round": "A"}], "data/fundraising_rounds_companies.csv"
        )

    @patch("update_csv.write_csv")
    @patch("update_csv.transform_records")
    @patch("update_csv.fetch_main_table")
    @patch("update_csv.build_linked_data")
    def test_main_empty_records(self, mock_build, mock_fetch, mock_transform, mock_write):
        mock_build.return_value = {}
        mock_fetch.return_value = []
        mock_transform.return_value = ([], [])
        update_csv.main()
        mock_write.assert_called_once_with(
            [], [], "data/fundraising_rounds_companies.csv"
        )
