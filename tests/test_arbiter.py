"""执行权仲裁：优先级裁决、抢占与排队时序、并发判重、凭据哈希链。"""

from __future__ import annotations

import threading

import pytest

from heatnet.arbiter import (
    AWAIT,
    BATCH,
    DUPLICATE,
    EXPIRED,
    GRANT,
    INTERLOCK,
    INTERRUPTED,
    MANUAL,
    PREEMPT,
    QUEUE,
    SUPERSEDE,
    TAKE_EVENTS,
    VOID,
    ValveArbiter,
    ValveCommand,
    ValveDriver,
    digest_of,
    policy_for,
)
from heatnet.audit.log import AuditLog
from heatnet.clock import ManualClock
from heatnet.config import default_config
from heatnet.eventbus import EventBus
from heatnet.store.journal import JsonlJournal
from heatnet.valve.valve import Valve


def _parts(tmp_path, *, ttl_s: float = 300.0):
    clock = ManualClock()
    bus = EventBus()
    valve = Valve("st01.mix", default_config().valve, position_pct=50.0)
    driver = ValveDriver(valve)
    arbiter = ValveArbiter(driver, clock, bus, queue_ttl_s=ttl_s)
    audit = AuditLog(clock, bus, JsonlJournal(str(tmp_path), "audit"))
    return clock, bus, valve, arbiter, audit


def _cmd(command_id: str, source: str, target_pct: float) -> ValveCommand:
    return ValveCommand(command_id, source, target_pct)


def test_policy_table_is_fixed():
    assert policy_for(INTERLOCK).priority > policy_for(MANUAL).priority
    assert policy_for(MANUAL).priority > policy_for(BATCH).priority
    assert policy_for(INTERLOCK).preemptive is True
    assert policy_for(MANUAL).mode == PREEMPT
    assert policy_for(BATCH).mode == AWAIT
    with pytest.raises(ValueError):
        policy_for("scada-override")
    with pytest.raises(ValueError):
        _cmd("x", "scada", 10.0)
    with pytest.raises(ValueError):
        _cmd("x", BATCH, 120.0)
    with pytest.raises(ValueError):
        _cmd("", BATCH, 10.0)


def test_grant_then_complete_drives_valve(tmp_path):
    clock, _bus, valve, arbiter, _audit = _parts(tmp_path)
    verdict = arbiter.submit(_cmd("b1", BATCH, 60.0))
    assert verdict["event"] == GRANT
    assert verdict["holder_id"] == "b1"
    assert arbiter.status()["holder"]["command_id"] == "b1"
    clock.advance(0.4)
    arbiter.tick()
    assert arbiter.status()["holder"] is not None
    clock.advance(0.5)
    produced = arbiter.tick()
    assert [item["event"] for item in produced] == ["complete"]
    assert valve.position_pct() == pytest.approx(60.0)
    assert arbiter.is_idle()
    arbiter.verify()


def test_lower_preempt_source_is_voided_interlock_preempts_manual(tmp_path):
    clock, _bus, valve, arbiter, _audit = _parts(tmp_path)
    # 联锁在执：手动抢占不进来，立即作废（抢占来源不排队）。
    assert arbiter.submit(_cmd("i0", INTERLOCK, 80.0))["event"] == GRANT
    voided = arbiter.submit(_cmd("m1", MANUAL, 20.0))
    assert voided["event"] == VOID
    assert voided["displaced_id"] == "i0"
    # 联锁走完，手动拿权。
    arbiter.advance(10.0, step_s=0.1)
    assert arbiter.submit(_cmd("m0", MANUAL, 30.0))["event"] == GRANT
    # 联锁抢占手动：被抢方先 interrupted，抢占方再落 preempt。
    clock.advance(0.1)
    preempt = arbiter.submit(_cmd("i1", INTERLOCK, 10.0))
    assert preempt["event"] == PREEMPT
    assert preempt["displaced_id"] == "m0"
    events = [item["event"] for item in arbiter.verdicts()]
    assert events[-2:] == ["interrupted", "preempt"]
    chain = arbiter.verdicts()
    assert chain[-2]["command_id"] == "m0"
    assert chain[-1]["command_id"] == "i1"
    assert valve.target_pct() == pytest.approx(10.0)
    arbiter.advance(10.0, step_s=0.1)
    assert valve.position_pct() == pytest.approx(10.0)
    arbiter.verify()


def test_batch_waits_and_dispatches_in_arrival_fifo(tmp_path):
    _clock, _bus, valve, arbiter, _audit = _parts(tmp_path)
    arbiter.submit(_cmd("i1", INTERLOCK, 40.0))
    first = arbiter.submit(_cmd("b1", BATCH, 60.0))
    second = arbiter.submit(_cmd("b2", BATCH, 70.0))
    assert first["event"] == QUEUE and first["queue_depth"] == 1
    assert second["event"] == QUEUE and second["queue_depth"] == 2
    assert [item["command_id"] for item in arbiter.status()["waiting"]] == ["b1", "b2"]
    # 联锁走完 -> b1 放行 -> b1 走完 -> b2 放行 -> b2 走完。
    produced = arbiter.advance(10.0, step_s=0.1)
    assert [item["event"] for item in produced] == [
        "complete",
        "dispatch",
        "complete",
        "dispatch",
        "complete",
    ]
    assert [item["command_id"] for item in produced if item["event"] == "dispatch"] == [
        "b1",
        "b2",
    ]
    assert valve.position_pct() == pytest.approx(70.0)
    arbiter.verify()


def test_arrival_order_never_raises_priority(tmp_path):
    _clock, _bus, _valve, arbiter, _audit = _parts(tmp_path)
    # 低优先级批量先到并持权。
    arbiter.submit(_cmd("b0", BATCH, 90.0))
    # 高优先级手动后到，照样抢占——裁决只认来源优先级，与到达先后无关。
    assert arbiter.submit(_cmd("m0", MANUAL, 30.0))["event"] == PREEMPT
    # 手动持权时，后到的批量只能排队，哪怕批量这一路更早来过。
    assert arbiter.submit(_cmd("b9", BATCH, 95.0))["event"] == QUEUE


def test_same_source_supersedes_holder_but_distinct_ids_queue(tmp_path):
    _clock, _bus, valve, arbiter, _audit = _parts(tmp_path)
    arbiter.submit(_cmd("b1", BATCH, 80.0))
    arbiter.submit(_cmd("b2", BATCH, 30.0))
    events = {item["command_id"]: item["event"] for item in arbiter.verdicts()}
    assert events["b1"] == "interrupted"
    assert events["b2"] == SUPERSEDE
    # 联锁插进来持权，之后两条不同标识的批量按到达顺序排队，互不顶替。
    arbiter.submit(_cmd("i1", INTERLOCK, 10.0))
    arbiter.submit(_cmd("b3", BATCH, 40.0))
    arbiter.submit(_cmd("b4", BATCH, 60.0))
    assert [item["command_id"] for item in arbiter.status()["waiting"]] == ["b3", "b4"]
    # 同标识的排队指令第二次提交只算重复，队列深度不变。
    duplicate = arbiter.submit(_cmd("b3", BATCH, 40.0))
    assert duplicate["event"] == DUPLICATE
    assert len(arbiter.status()["waiting"]) == 2
    arbiter.advance(15.0, step_s=0.1)
    assert valve.position_pct() == pytest.approx(60.0)
    arbiter.verify()


def test_concurrent_duplicate_command_id_takes_effect_once(tmp_path):
    _clock, bus, _valve, arbiter, _audit = _parts(tmp_path)
    results: list[str] = []
    barrier = threading.Barrier(8)

    def submit() -> None:
        barrier.wait()
        results.append(arbiter.submit(_cmd("dup-1", INTERLOCK, 25.0))["event"])

    threads = [threading.Thread(target=submit) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    take = [item for item in results if item in TAKE_EVENTS]
    assert len(take) == 1
    assert take[0] == GRANT
    assert results.count(DUPLICATE) == 7
    assert len(bus.history("arbiter")) == len(arbiter.verdicts())
    # 串行再来一次，仍是重复。
    assert arbiter.submit(_cmd("dup-1", INTERLOCK, 25.0))["event"] == DUPLICATE


def test_expired_queue_entry_is_voided_before_dispatch(tmp_path):
    clock, _bus, valve, arbiter, _audit = _parts(tmp_path, ttl_s=1.0)
    arbiter.submit(_cmd("i1", INTERLOCK, 20.0))
    queued = arbiter.submit(_cmd("b1", BATCH, 60.0))
    assert queued["event"] == QUEUE
    # 联锁要走 2.5s，排队条目 1s 就过期；推进 5s 后只允许联锁走完，
    # 批量条目即使已经轮到也必须作废、阀位停在联锁目标上。
    clock.advance(5.0)
    produced = arbiter.tick()
    assert {item["command_id"]: item["event"] for item in produced}["b1"] == EXPIRED
    assert arbiter.is_idle()
    assert valve.position_pct() == pytest.approx(20.0)
    arbiter.verify()


def test_verdicts_feed_console_and_audit_from_same_source(tmp_path):
    clock, bus, _valve, arbiter, audit = _parts(tmp_path)
    arbiter.submit(_cmd("m1", MANUAL, 70.0))
    clock.advance(0.1)
    arbiter.submit(_cmd("i1", INTERLOCK, 15.0))
    # 控制台读仲裁器凭据，审计读事件总线落盘，两边关键字段必须一致。
    console = arbiter.verdicts()
    journal = audit.replay_from_journal()
    assert len(journal) == len(console)
    for verdict, record in zip(console, journal):
        assert record["action"] == verdict["event"]
        assert record["payload"]["digest"] == verdict["digest"]
        assert record["payload"]["valve_position"] == verdict["valve_position"]
        assert record["payload"]["command_id"] == verdict["command_id"]
        assert record["asset"] == "st01.mix"
    # 抢占在控制台与审计两侧都是告警档。
    alarms = [item for item in journal if item["severity"] == "alarm"]
    assert {item["action"] for item in alarms} >= {"interrupted", "preempt"}
    # 仲裁器状态里的链尾摘要就是审计最后一条凭据摘要。
    assert arbiter.status()["last_digest"] == journal[-1]["payload"]["digest"]


def test_verdict_chain_detects_tampering(tmp_path):
    _clock, _bus, _valve, arbiter, _audit = _parts(tmp_path)
    arbiter.submit(_cmd("b1", BATCH, 60.0))
    arbiter.submit(_cmd("i1", INTERLOCK, 10.0))
    arbiter.verify()
    # 从外部改判一条结论，重算摘要必然对不上，链校验必须报错。
    target = arbiter._verdicts[0]  # noqa: SLF001
    object.__setattr__(target, "source", MANUAL)
    assert digest_of(target) != target.digest
    with pytest.raises(ValueError):
        arbiter.verify()


def test_zero_travel_grant_completes_immediately(tmp_path):
    _clock, _bus, valve, arbiter, _audit = _parts(tmp_path)
    verdict = arbiter.submit(_cmd("b0", BATCH, 50.0))  # 阀位初始即 50%
    assert verdict["event"] == GRANT
    assert arbiter.is_idle()
    assert valve.position_pct() == pytest.approx(50.0)
    assert [item["event"] for item in arbiter.verdicts()] == ["grant", "complete"]
