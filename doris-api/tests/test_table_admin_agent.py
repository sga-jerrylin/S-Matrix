from table_admin_agent import TableAdminAgent


class FakeTableAdminDb:
    def get_table_schema(self, table_name):
        return [
            {"Field": "institution_name", "Type": "VARCHAR"},
            {"Field": "city", "Type": "VARCHAR"},
            {"Field": "year", "Type": "INT"},
        ]

    def execute_query(self, sql, params=None):
        if "LIMIT 3" in sql:
            return [{"institution_name": "绿色未来", "city": "广州", "year": 2022}]
        return []


def test_generate_sql_for_subtask_retries_with_single_table_prompt(monkeypatch):
    prompts = []

    class FakeVanna:
        def __init__(self, *args, **kwargs):
            pass

        def generate_sql(self, question):
            return "SELECT COUNT(*) FROM `中国环保公益组织现状调研数据2022_2024`"

        def extract_table_names(self, sql):
            if "中国环保公益组织现状调研数据2022_2024" in sql:
                return ["中国环保公益组织现状调研数据2022_2024"]
            return ["institutions"]

        def submit_prompt(self, prompt):
            prompts.append(prompt)
            return "SELECT COUNT(*) FROM `institutions` WHERE `city` LIKE '%广州%'"

        def auto_fuzzy_match_locations(self, sql):
            return sql

    monkeypatch.setattr("table_admin_agent.VannaDorisOpenAI", FakeVanna)

    agent = TableAdminAgent(doris_client_override=FakeTableAdminDb())
    sql = agent.generate_sql_for_subtask(
        {"table": "institutions", "question": "广州有多少机构？"},
        "广州有多少机构？",
        {"api_key": "test-key", "model": "deepseek-chat", "base_url": "https://api.deepseek.com"},
    )

    assert sql == "SELECT COUNT(*) FROM `institutions` WHERE `city` LIKE '%广州%'"
    assert prompts
    assert "ONLY use table `institutions`" in prompts[0]


def test_generate_sql_for_subtask_uses_single_table_prompt_first(monkeypatch):
    prompts = []

    class FakeVanna:
        def __init__(self, *args, **kwargs):
            pass

        def generate_sql(self, question):
            raise AssertionError("generic multi-table prompt should not be used for targeted subtasks")

        def extract_table_names(self, sql):
            return ["institutions"] if "institutions" in sql else []

        def submit_prompt(self, prompt):
            prompts.append(prompt)
            return "SELECT COUNT(*) FROM `institutions` WHERE `city` LIKE '%广州%'"

        def auto_fuzzy_match_locations(self, sql):
            return sql

        def get_similar_question_sql(self, question, **kwargs):
            return []

    monkeypatch.setattr("table_admin_agent.VannaDorisOpenAI", FakeVanna)

    agent = TableAdminAgent(doris_client_override=FakeTableAdminDb())
    sql = agent.generate_sql_for_subtask(
        {"table": "institutions", "question": "广州有多少机构？"},
        "广州有多少机构？",
        {"api_key": "test-key", "model": "deepseek-chat", "base_url": "https://api.deepseek.com"},
    )

    assert sql == "SELECT COUNT(*) FROM `institutions` WHERE `city` LIKE '%广州%'"
    assert len(prompts) == 1
    assert "ONLY use table `institutions`" in prompts[0]
