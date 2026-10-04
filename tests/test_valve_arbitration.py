"""混水阀执行权仲裁在平台、控制台与审计之间的贯通。"""

from __future__ import annotations

from heatnet.console.api import Api


def test_valve_arbitration_api_records_credential_and_moves_valve(platform):
    api = Api(platform)
    status, payload = api.handle(
        "POST",
        "/api/valve/arbitrate",
        {},
        {"station": "st01", "source": "local", "target_pct": 42.0, "command_id": "local-1"},
    )
    assert status == 200
    assert payload["ok"] is True
    assert payload["completed"] is True
    assert payload["request"]["status"] == "completed"
    assert payload["valve"]["position_pct"] == 42.0
    assert payload["audit"]["action"] == "arbitration_granted"
    assert payload["audit"]["payload"]["command_id"] == "local-1"
    assert platform.durable.load("valve/st01.mix")["position_pct"] == 42.0

    status, duplicate = api.handle(
        "POST",
        "/api/valve/arbitrate",
        {},
        {"station": "st01", "source": "local", "target_pct": 80.0, "command_id": "local-1"},
    )
    assert status == 200
    assert duplicate["ok"] is False
    assert duplicate["completed"] is False
    assert duplicate["verdict"]["decision"] == "duplicate"
    assert duplicate["audit"]["action"] == "arbitration_duplicate"
    assert duplicate["valve"]["position_pct"] == 42.0

    status, snapshot = api.handle("GET", "/api/valve/st01/arbitration", {"limit": "50"}, {})
    assert status == 200
    assert [item["decision"] for item in snapshot["verdicts"]][-1] == "duplicate"
    assert snapshot["snapshot"]["history"][0]["command_id"] == "local-1"


def test_valve_arbitration_is_visible_in_status_and_zone_batch(platform):
    api = Api(platform)
    status, payload = api.handle(
        "POST",
        "/api/valve/arbitrate-zone",
        {},
        {"zone": "north", "source": "zone", "target_pct": 33.0},
    )
    assert status == 200
    assert payload["ok"] is True
    assert len(payload["reports"]) == 3

    status, state = api.handle("GET", "/api/status", {}, {})
    assert status == 200
    st01 = state["stations"]["detail"]["st01"]
    assert st01["arbitration"]["active"] is None
    assert st01["valve"]["position_pct"] == 33.0


def test_duplicate_command_remains_invalid_after_platform_restore(tmp_path):
    from heatnet.app import build_platform
    from heatnet.config import default_config

    root = str(tmp_path / "state")
    platform = build_platform(default_config(), storage_root=root)
    result = platform.arbitrate_valve("st01", 35.0, "manual", "manual-once")
    assert result["completed"] is True

    restored = build_platform(default_config(), storage_root=root)
    duplicate = restored.arbitrate_valve("st01", 90.0, "manual", "manual-once")
    assert duplicate["verdict"]["decision"] == "duplicate"
    assert restored.valves["st01"].position_pct() == 35.0
