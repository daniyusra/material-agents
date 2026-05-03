from langchain_core.messages import AIMessage, HumanMessage, SystemMessage

from app.agents.chat import _to_lc_messages


class TestToLcMessages:
    def test_user_message(self):
        result = _to_lc_messages([{"role": "user", "content": "hello"}])
        assert len(result) == 1
        assert isinstance(result[0], HumanMessage)
        assert result[0].content == "hello"

    def test_assistant_message(self):
        result = _to_lc_messages([{"role": "assistant", "content": "hi there"}])
        assert len(result) == 1
        assert isinstance(result[0], AIMessage)
        assert result[0].content == "hi there"

    def test_system_message(self):
        result = _to_lc_messages([{"role": "system", "content": "you are helpful"}])
        assert len(result) == 1
        assert isinstance(result[0], SystemMessage)
        assert result[0].content == "you are helpful"

    def test_multi_turn_ordering(self):
        msgs = [
            {"role": "system", "content": "sys"},
            {"role": "user", "content": "q"},
            {"role": "assistant", "content": "a"},
            {"role": "user", "content": "follow up"},
        ]
        result = _to_lc_messages(msgs)
        assert len(result) == 4
        assert isinstance(result[0], SystemMessage)
        assert isinstance(result[1], HumanMessage)
        assert isinstance(result[2], AIMessage)
        assert isinstance(result[3], HumanMessage)

    def test_missing_content_defaults_to_empty_string(self):
        result = _to_lc_messages([{"role": "user"}])
        assert result[0].content == ""

    def test_unknown_role_is_skipped(self):
        assert _to_lc_messages([{"role": "unknown", "content": "x"}]) == []

    def test_empty_list(self):
        assert _to_lc_messages([]) == []
