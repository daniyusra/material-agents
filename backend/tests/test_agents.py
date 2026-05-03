import pytest
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage

from app.agents.chat import _to_lc_messages as anthropic_fn
from app.agents.chat_openai import _to_lc_messages as openai_fn


@pytest.mark.parametrize("fn", [anthropic_fn, openai_fn], ids=["anthropic", "openai"])
class TestToLcMessages:
    def test_user_message(self, fn):
        result = fn([{"role": "user", "content": "hello"}])
        assert len(result) == 1
        assert isinstance(result[0], HumanMessage)
        assert result[0].content == "hello"

    def test_assistant_message(self, fn):
        result = fn([{"role": "assistant", "content": "hi there"}])
        assert len(result) == 1
        assert isinstance(result[0], AIMessage)
        assert result[0].content == "hi there"

    def test_system_message(self, fn):
        result = fn([{"role": "system", "content": "you are helpful"}])
        assert len(result) == 1
        assert isinstance(result[0], SystemMessage)
        assert result[0].content == "you are helpful"

    def test_multi_turn_ordering(self, fn):
        msgs = [
            {"role": "system", "content": "sys"},
            {"role": "user", "content": "q"},
            {"role": "assistant", "content": "a"},
            {"role": "user", "content": "follow up"},
        ]
        result = fn(msgs)
        assert len(result) == 4
        assert isinstance(result[0], SystemMessage)
        assert isinstance(result[1], HumanMessage)
        assert isinstance(result[2], AIMessage)
        assert isinstance(result[3], HumanMessage)

    def test_missing_content_defaults_to_empty_string(self, fn):
        result = fn([{"role": "user"}])
        assert result[0].content == ""

    def test_unknown_role_is_skipped(self, fn):
        result = fn([{"role": "unknown", "content": "x"}])
        assert result == []

    def test_empty_list(self, fn):
        assert fn([]) == []
