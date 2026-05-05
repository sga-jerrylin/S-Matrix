# Vanna 2 Native Spike (Isolated)

This directory contains an isolated spike for evaluating Vanna 2 native capabilities.

Scope:
- Verify native `Agent` + `ToolRegistry` + `servers.fastapi` surfaces are callable.
- Verify `LegacyVannaAdapter` bridge still works as transition path.
- Keep DorisClaw production NLQ chain unchanged.

Out of scope:
- No replacement of `/api/query/natural`.
- No frontend integration of `<vanna-chat>`.
- No runtime/Docker changes.

## Scripts

- `probe_vanna2_native.py`
  - Probes module availability, key signatures, and version consistency.
- `poc_native_chat_server.py`
  - Builds a minimal native agent and validates `chat_poll` + `chat_sse`.
- `poc_registry_and_legacy_adapter.py`
  - Validates tool access control and `LegacyVannaAdapter` bridge behavior.

## Run

```powershell
python experimental\vanna2_native_spike\probe_vanna2_native.py
python experimental\vanna2_native_spike\poc_native_chat_server.py
python experimental\vanna2_native_spike\poc_registry_and_legacy_adapter.py
```

All scripts only print JSON probe output and do not modify runtime state.
