import pytest
import polars as pl

from nemdb.isp import isp
from nemdb.isp.isp import _detect_header_row, _clean_table, _NON_DATA_SHEETS

ISP_2025_FILE = "2025-inputs-and-assumptions-workbook(1).xlsm"


# ---------------------------------------------------------------------------
# Unit tests for helper functions
# ---------------------------------------------------------------------------


class TestDetectHeaderRow:
    def test_simple_header_at_row_0(self):
        df = pl.DataFrame(
            {
                "a": ["col_a", "1", "2"],
                "b": ["col_b", "3", "4"],
                "c": ["col_c", "5", "6"],
            }
        )
        assert _detect_header_row(df) == 0

    def test_header_after_description_rows(self):
        """Description rows have only the first column filled."""
        df = pl.DataFrame(
            {
                "a": ["Title", "Some description", None, "col_a", "1"],
                "b": [None, None, None, "col_b", "2"],
                "c": [None, None, None, "col_c", "3"],
            }
        )
        assert _detect_header_row(df) == 3

    def test_skips_section_header_with_sparse_first_cols(self):
        """A section header row like 'Build costs | null | null | 2 | 3 | ...'
        should be skipped in favour of the real header below it."""
        df = pl.DataFrame(
            {
                "a": ["desc", None, "Section", "col_a", "val"],
                "b": [None, None, None, "col_b", "val"],
                "c": [None, None, None, "col_c", "val"],
                "d": [None, None, "2", "2024-25", "100"],
                "e": [None, None, "3", "2025-26", "200"],
            }
        )
        # Row 2 has a=Section, b=None, c=None -> only 1 of first 3 non-null -> skip
        # Row 3 has a=col_a, b=col_b, c=col_c -> 3 of 3 non-null -> match
        assert _detect_header_row(df) == 3

    def test_returns_0_when_no_row_matches(self):
        df = pl.DataFrame(
            {
                "a": ["x", None],
                "b": [None, None],
                "c": [None, None],
            }
        )
        assert _detect_header_row(df) == 0

    def test_respects_max_scan(self):
        rows = [[None, None, None]] * 10 + [["a", "b", "c"]]
        df = pl.DataFrame(
            {"x": [r[0] for r in rows], "y": [r[1] for r in rows], "z": [r[2] for r in rows]}
        )
        # Should find it at row 10
        assert _detect_header_row(df, max_scan=20) == 10
        # Should NOT find it with max_scan=5
        assert _detect_header_row(df, max_scan=5) == 0

    def test_threshold_parameter(self):
        """With high threshold, sparse rows are rejected."""
        df = pl.DataFrame(
            {
                "a": ["col_a", "1"],
                "b": ["col_b", "2"],
                "c": [None, "3"],
                "d": [None, "4"],
                "e": [None, "5"],
            }
        )
        # Row 0: 2/5 = 40% non-null. threshold=0.4 -> min_non_null=max(3, 2)=3 -> row 0 has 2 < 3 -> skip
        # Row 1: 5/5 -> match
        assert _detect_header_row(df, threshold=0.4) == 1


class TestCleanTable:
    def test_drops_all_null_columns(self):
        df = pl.DataFrame(
            {
                "a": ["1", "2"],
                "b": [None, None],
                "c": ["3", "4"],
            }
        )
        result = _clean_table(df)
        assert result.columns == ["a", "c"]

    def test_drops_all_null_rows(self):
        df = pl.DataFrame(
            {
                "a": ["1", None, "3"],
                "b": ["x", None, "z"],
            }
        )
        result = _clean_table(df)
        assert len(result) == 2
        assert result["a"].to_list() == ["1", "3"]

    def test_preserves_partial_null_rows(self):
        df = pl.DataFrame(
            {
                "a": ["1", None, "3"],
                "b": ["x", "y", None],
            }
        )
        result = _clean_table(df)
        assert len(result) == 3


# ---------------------------------------------------------------------------
# Existing test (2024 workbook)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "func",
    [
        isp.read_coal_prices,
    ],
)
def test_read_functions(func):
    _ = func()
    assert True


# ---------------------------------------------------------------------------
# Integration tests against the 2025 workbook
# ---------------------------------------------------------------------------


@pytest.fixture
def isp_2025():
    return isp.ISPAssumptions(ISP_2025_FILE)


class TestISPAssumptions2025:
    def test_tables_returns_all_sheets(self, isp_2025):
        tables = isp_2025.tables
        assert len(tables) > 50
        assert "Assumptions Summary" in tables
        assert "Build costs" in tables

    def test_data_tables_excludes_metadata(self, isp_2025):
        data = isp_2025.data_tables
        for name in _NON_DATA_SHEETS:
            assert name not in data
        assert "Build costs" in data
        assert len(data) == len(isp_2025.tables) - len(
            [s for s in _NON_DATA_SHEETS if s in isp_2025.tables]
        )

    def test_repr(self, isp_2025):
        r = repr(isp_2025)
        assert "ISP spreadsheet" in r
        assert "Build costs" in r

    def test_read_table_returns_raw(self, isp_2025):
        """read_table should return the raw sheet including description rows."""
        df = isp_2025.read_table("Build costs")
        assert isinstance(df, pl.DataFrame)
        # Raw sheet has description text in the first column header
        assert "Go to Assumptions Summary" in df.columns[0]


class TestReadCleanTable:
    def test_build_costs(self, isp_2025):
        df = isp_2025.read_clean_table("Build costs")
        assert isinstance(df, pl.DataFrame)
        assert "Technology1" in df.columns
        assert "IASR Scenario" in df.columns
        # Should have financial year columns
        fy_cols = [c for c in df.columns if "-" in c and c[:4].isdigit()]
        assert len(fy_cols) > 0
        assert len(df) > 0

    def test_heat_rates(self, isp_2025):
        df = isp_2025.read_clean_table("Heat rates")
        assert "IASR ID" in df.columns
        assert "Power Station" in df.columns
        assert "Heat rate (GJ/MWh)" in df.columns
        assert len(df) > 0

    def test_retirement(self, isp_2025):
        df = isp_2025.read_clean_table("Retirement")
        assert "IASR ID" in df.columns
        assert "Expected Closure Year (Calendar year)" in df.columns

    def test_capacity_factors(self, isp_2025):
        df = isp_2025.read_clean_table("Capacity Factors ")
        assert "REZ ID / Subregion" in df.columns
        assert "Technology" in df.columns
        assert "Region" in df.columns

    def test_maximum_capacity(self, isp_2025):
        df = isp_2025.read_clean_table("Maximum capacity")
        assert "IASR ID" in df.columns
        assert "Power Station" in df.columns
        assert "Technology" in df.columns

    def test_coal_and_biomass_price(self, isp_2025):
        df = isp_2025.read_clean_table("Coal and Biomass price")
        assert "Generator" in df.columns
        assert "Scenario" in df.columns
        fy_cols = [c for c in df.columns if "-" in c and c[:4].isdigit()]
        assert len(fy_cols) > 20

    def test_scenarios(self, isp_2025):
        df = isp_2025.read_clean_table("Scenarios")
        assert "Parameter" in df.columns
        assert "Slower Growth" in df.columns
        assert "Step Change" in df.columns

    def test_storage_properties(self, isp_2025):
        df = isp_2025.read_clean_table("Storage properties")
        assert "Technology" in df.columns
        assert len(df) > 5

    def test_explicit_header_row(self, isp_2025):
        """Passing header_row explicitly should override auto-detection."""
        df = isp_2025.read_clean_table("Build costs", header_row=9)
        assert "Technology1" in df.columns

    def test_no_all_null_columns_in_output(self, isp_2025):
        df = isp_2025.read_clean_table("Build costs")
        for col in df.columns:
            assert df[col].null_count() < len(df), f"Column {col!r} is all null"

    def test_no_all_null_rows_in_output(self, isp_2025):
        df = isp_2025.read_clean_table("Build costs")
        # No row should be entirely null (already filtered)
        for i in range(len(df)):
            row = df.row(i)
            assert any(v is not None for v in row)


class TestSummary:
    def test_summary_columns(self, isp_2025):
        summary = isp_2025.summary()
        assert summary.columns == [
            "Worksheet",
            "Assumptions Grouping",
            "Description",
            "Source",
        ]

    def test_summary_contains_data_sheets(self, isp_2025):
        summary = isp_2025.summary()
        worksheets = summary["Worksheet"].to_list()
        assert "Scenarios" in worksheets
        assert "Build costs" not in worksheets  # column name is "Build costs" in sheet but "Build Costs" in summary
        # Check a few known entries
        assert any("Coal" in w for w in worksheets if w is not None)
        assert any("Retirement" in w for w in worksheets if w is not None)

    def test_summary_has_descriptions(self, isp_2025):
        summary = isp_2025.summary()
        descriptions = summary["Description"].to_list()
        assert any(d is not None and len(d) > 10 for d in descriptions)

    def test_summary_no_supporting_materials(self, isp_2025):
        """Summary should stop before the 'Supporting Materials' section."""
        summary = isp_2025.summary()
        worksheets = summary["Worksheet"].to_list()
        assert not any(
            "Supporting Materials" in str(w) for w in worksheets if w is not None
        )

    def test_summary_row_count(self, isp_2025):
        summary = isp_2025.summary()
        # Should have roughly one row per data sheet (80 sheets - 3 metadata)
        assert len(summary) >= 50
        assert len(summary) <= 100
