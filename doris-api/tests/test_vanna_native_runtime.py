import asyncio

from fastapi.testclient import TestClient

from vanna_native_runtime import (
    build_native_runtime_backbone,
    load_native_imports,
    probe_vanna_native_runtime,
    resolve_legacy_vanna_base,
)


def test_load_native_imports_exposes_required_symbols():
    imports = load_native_imports()

    assert imports.Agent is not None
    assert imports.ToolRegistry is not None
    assert imports.RequestContext is not None
    assert imports.DemoAgentMemory is not None
    assert imports.MockLlmService is not None
    assert imports.VannaFastAPIServer is not None
    assert imports.LegacyVannaAdapter is not None


def test_resolve_legacy_vanna_base_uses_supported_path():
    vanna_base, import_path = resolve_legacy_vanna_base()
    assert import_path in {"vanna.legacy.base", "vanna.base"}
    assert vanna_base is not None
    assert vanna_base.__module__.startswith("vanna.")


def test_probe_vanna_native_runtime_reports_key_signatures():
    probe = probe_vanna_native_runtime().as_dict()

    assert probe["modules"]["vanna"]["available"] is True
    assert probe["modules"]["vanna.servers.fastapi"]["available"] is True
    assert "Agent.__init__" in probe["signatures"]
    assert "llm_service" in probe["signatures"]["Agent.__init__"]
    assert "tool_registry" in probe["signatures"]["Agent.__init__"]
    assert "user_resolver" in probe["signatures"]["Agent.__init__"]
    assert "agent_memory" in probe["signatures"]["Agent.__init__"]


def test_native_runtime_backbone_initializes_agent_and_sidecar():
    backbone = build_native_runtime_backbone(
        default_user_id="phase1-user",
        default_user_groups=["user"],
        mock_response_content="phase1 backbone ok",
    )

    ctx = backbone.new_request_context(
        headers={"x-test": "1"},
        metadata={"user_id": "phase1-user", "groups": ["admin"]},
    )
    assert ctx.metadata["user_id"] == "phase1-user"
    resolved_user = asyncio.run(backbone.user_resolver.resolve_user(ctx))
    assert resolved_user.id == "phase1-user"
    assert "admin" in resolved_user.group_memberships

    app = backbone.create_sse_sidecar_app()
    client = TestClient(app)
    resp = client.post("/api/vanna/v2/chat_poll", json={"message": "hello"})
    assert resp.status_code == 200
    payload = resp.json()
    assert "chunks" in payload
    assert "total_chunks" in payload
    assert payload["total_chunks"] >= 1
