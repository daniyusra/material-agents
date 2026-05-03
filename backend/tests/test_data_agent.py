import pandas as pd
import pytest

from app.agents.data_agent import (
    _parse_classification,
    _strip_fences,
    df_info,
    execute_pandas_code,
    execute_viz_code,
    route_after_execute,
    route_by_intent,
)


class TestExecutePandasCode:
    def test_mean(self):
        df = pd.DataFrame({"a": [1.0, 2.0, 3.0]})
        assert execute_pandas_code("result = df['a'].mean()", df) == "2.0"

    def test_max(self):
        df = pd.DataFrame({"sales": [10, 20, 30]})
        assert execute_pandas_code("result = df['sales'].max()", df) == "30"

    def test_min(self):
        df = pd.DataFrame({"sales": [10, 20, 30]})
        assert execute_pandas_code("result = df['sales'].min()", df) == "10"

    def test_multi_statement(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        code = "filtered = df[df['a'] > 1]\nresult = filtered['b'].sum()"
        assert execute_pandas_code(code, df) == "11"

    def test_comparison(self):
        df = pd.DataFrame({"x": [1, 2, 3], "y": [4, 2, 1]})
        assert execute_pandas_code("result = (df['x'] > df['y']).sum()", df) == "1"

    def test_no_result_variable(self):
        out = execute_pandas_code("x = 1", pd.DataFrame({"a": [1]}))
        assert "no `result` variable" in out

    def test_syntax_error(self):
        out = execute_pandas_code("result = df[", pd.DataFrame({"a": [1]}))
        assert out.startswith("ExecutionError:")

    def test_runtime_error_bad_column(self):
        out = execute_pandas_code("result = df['nonexistent'].mean()", pd.DataFrame({"a": [1]}))
        assert out.startswith("ExecutionError:")

    def test_returns_string(self):
        result = execute_pandas_code("result = df['a'].mean()", pd.DataFrame({"a": [1, 2, 3]}))
        assert isinstance(result, str)


class TestExecuteVizCode:
    def test_histogram_returns_figure_dict(self):
        df = pd.DataFrame({"values": [1, 2, 3, 4, 5]})
        status, fig = execute_viz_code("result = px.histogram(df, x='values')", df)
        assert status == "Chart generated successfully."
        assert fig is not None
        assert "data" in fig

    def test_scatter_returns_figure_dict(self):
        df = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]})
        status, fig = execute_viz_code("result = px.scatter(df, x='x', y='y')", df)
        assert status == "Chart generated successfully."
        assert fig is not None

    def test_non_figure_result_errors(self):
        df = pd.DataFrame({"a": [1]})
        status, fig = execute_viz_code("result = 42", df)
        assert status.startswith("ExecutionError:")
        assert fig is None

    def test_no_result_variable(self):
        status, fig = execute_viz_code("x = 1", pd.DataFrame({"a": [1]}))
        assert "no `result` variable" in status
        assert fig is None

    def test_syntax_error(self):
        status, fig = execute_viz_code("result = px.histogram(df,", pd.DataFrame({"a": [1]}))
        assert status.startswith("ExecutionError:")
        assert fig is None

    def test_figure_dict_has_layout(self):
        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        _, fig = execute_viz_code("result = px.scatter(df, x='a', y='b')", df)
        assert fig is not None
        assert "layout" in fig


class TestDfInfo:
    def test_includes_shape(self):
        df = pd.DataFrame({"x": [1, 2], "y": ["a", "b"]})
        info = df_info(df)
        assert "2 rows" in info
        assert "2 columns" in info

    def test_includes_column_names_and_types(self):
        df = pd.DataFrame({"revenue": [1.0], "units": [1]})
        info = df_info(df)
        assert "revenue" in info
        assert "units" in info

    def test_includes_sample_rows(self):
        df = pd.DataFrame({"col": [99, 88, 77, 66]})
        info = df_info(df)
        assert "99" in info
        assert "66" not in info  # fourth row excluded from head(3)


class TestStripFences:
    def test_strips_python_fence(self):
        assert _strip_fences("```python\nresult = 1\n```") == "result = 1"

    def test_strips_json_fence(self):
        assert _strip_fences('```json\n{"a": 1}\n```') == '{"a": 1}'

    def test_strips_plain_fence(self):
        assert _strip_fences("```\nresult = 1\n```") == "result = 1"

    def test_no_fence_unchanged(self):
        assert _strip_fences("result = 1") == "result = 1"

    def test_strips_whitespace(self):
        assert _strip_fences("  result = 1  ") == "result = 1"


class TestParseClassification:
    def test_not_data(self):
        intent, chart = _parse_classification('{"intent": "NOT_DATA", "chart_type": null}')
        assert intent == "NOT_DATA"
        assert chart is None

    def test_data_only(self):
        intent, chart = _parse_classification('{"intent": "DATA_ONLY", "chart_type": null}')
        assert intent == "DATA_ONLY"
        assert chart is None

    def test_data_viz_histogram(self):
        intent, chart = _parse_classification('{"intent": "DATA_VIZ", "chart_type": "histogram"}')
        assert intent == "DATA_VIZ"
        assert chart == "histogram"

    def test_invalid_json_falls_back_to_data_only(self):
        intent, chart = _parse_classification("not json at all")
        assert intent == "DATA_ONLY"
        assert chart is None

    def test_unknown_intent_falls_back_to_data_only(self):
        intent, _ = _parse_classification('{"intent": "SOMETHING_NEW", "chart_type": null}')
        assert intent == "DATA_ONLY"

    def test_strips_fences_before_parsing(self):
        raw = '```json\n{"intent": "DATA_VIZ", "chart_type": "scatter"}\n```'
        intent, chart = _parse_classification(raw)
        assert intent == "DATA_VIZ"
        assert chart == "scatter"


class TestRouting:
    def _state(self, **overrides):
        base = {
            "messages": [], "provider": "anthropic", "file_id": "x",
            "question": "", "intent": None, "chart_type": None,
            "generated_code": None, "execution_result": None,
            "plotly_json": None, "retry_count": 0,
        }
        return {**base, **overrides}

    def test_route_by_intent_not_data(self):
        assert route_by_intent(self._state(intent="NOT_DATA")) == "plain_chat"

    def test_route_by_intent_data_only(self):
        assert route_by_intent(self._state(intent="DATA_ONLY")) == "generate_code"

    def test_route_by_intent_data_viz(self):
        assert route_by_intent(self._state(intent="DATA_VIZ")) == "generate_code"

    def test_route_after_execute_success(self):
        assert route_after_execute(self._state(execution_result="2.0", retry_count=0)) == "synthesize"

    def test_route_after_execute_error_first_attempt(self):
        s = self._state(execution_result="ExecutionError: ...", retry_count=0)
        assert route_after_execute(s) == "fix_code"

    def test_route_after_execute_error_retry_exhausted(self):
        s = self._state(execution_result="ExecutionError: ...", retry_count=1)
        assert route_after_execute(s) == "synthesize"

    def test_route_after_viz_success(self):
        s = self._state(intent="DATA_VIZ", execution_result="Chart generated successfully.", retry_count=0)
        assert route_after_execute(s) == "synthesize"
