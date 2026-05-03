import pandas as pd
import pytest

from app.agents.data_agent import (
    _strip_fences,
    df_info,
    execute_pandas_code,
    route_after_code,
    route_after_execute,
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
        df = pd.DataFrame({"a": [1]})
        out = execute_pandas_code("x = 1", df)
        assert "no `result` variable" in out

    def test_syntax_error(self):
        df = pd.DataFrame({"a": [1]})
        out = execute_pandas_code("result = df[", df)
        assert out.startswith("ExecutionError:")

    def test_runtime_error_bad_column(self):
        df = pd.DataFrame({"a": [1]})
        out = execute_pandas_code("result = df['nonexistent'].mean()", df)
        assert out.startswith("ExecutionError:")

    def test_returns_string(self):
        df = pd.DataFrame({"a": [1, 2, 3]})
        result = execute_pandas_code("result = df['a'].mean()", df)
        assert isinstance(result, str)


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
        assert "99" in info   # first row present
        assert "66" not in info  # fourth row excluded from head(3)


class TestStripFences:
    def test_strips_python_fence(self):
        assert _strip_fences("```python\nresult = 1\n```") == "result = 1"

    def test_strips_plain_fence(self):
        assert _strip_fences("```\nresult = 1\n```") == "result = 1"

    def test_no_fence_unchanged(self):
        assert _strip_fences("result = 1") == "result = 1"

    def test_strips_whitespace(self):
        assert _strip_fences("  result = 1  ") == "result = 1"


class TestRouting:
    def _state(self, **overrides):
        base = {
            "messages": [],
            "provider": "anthropic",
            "file_id": "x",
            "question": "",
            "generated_code": None,
            "execution_result": None,
            "retry_count": 0,
            "is_data_question": False,
        }
        return {**base, **overrides}

    def test_route_after_code_data_question(self):
        assert route_after_code(self._state(is_data_question=True)) == "execute_code"

    def test_route_after_code_not_data(self):
        assert route_after_code(self._state(is_data_question=False)) == "plain_chat"

    def test_route_after_execute_success(self):
        s = self._state(execution_result="2.0", retry_count=0)
        assert route_after_execute(s) == "synthesize"

    def test_route_after_execute_error_first_attempt(self):
        s = self._state(execution_result="ExecutionError: ...", retry_count=0)
        assert route_after_execute(s) == "fix_code"

    def test_route_after_execute_error_retry_exhausted(self):
        s = self._state(execution_result="ExecutionError: ...", retry_count=1)
        assert route_after_execute(s) == "synthesize"
