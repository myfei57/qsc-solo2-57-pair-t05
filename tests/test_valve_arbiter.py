"""混水阀执行权来源优先级、抢占、排队与幂等裁决。"""

from __future__ import annotations

import threading

from heatnet.valve.arbiter import (
    COMPLETED,
    GRANTED,
    PREEMPTED,
    QUEUED,
    VOIDED,
    ValveArbiter,
    resolve_source,
)


class BlockingAction:
    """可在动作内部同步测试线程的阻塞动作。"""

    def __init__(self) -> None:
        self.started = threading.Event()
        self.release = threading.Event()
        self.cancelled = False
        self.calls = 0

    def __call__(self, request, token):
        self.calls += 1
        self.started.set()
        if not self.release.wait(timeout=2.0):
            raise TimeoutError("测试动作未被释放")
        if token.cancelled():
            self.cancelled = True
            raise RuntimeError("cancelled")
        return {"target_pct": request.target_pct}


def test_high_priority_local_preempts_and_lower_waits_are_voided():
    arbiter = ValveArbiter("st01.mix")
    zone = BlockingAction()
    manual = BlockingAction()

    zone_verdict = arbiter.submit("zone-1", "zone", 40.0, zone)
    assert zone_verdict.decision == GRANTED
    assert zone.started.wait(timeout=2.0)

    queued = arbiter.submit("manual-1", "manual", 50.0, manual)
    assert queued.decision == QUEUED
    assert queued.reason == "wait_for_current_action"

    local = BlockingAction()
    local_verdict = arbiter.submit("local-1", "local", 70.0, local)
    assert local_verdict.decision == GRANTED
    assert local_verdict.reason == "source_priority_preemption"
    assert local.started.wait(timeout=2.0)

    snapshot = arbiter.snapshot()
    assert snapshot["active"]["command_id"] == "local-1"
    assert snapshot["pending"] == []
    statuses = {item["command_id"]: item["status"] for item in snapshot["history"]}
    assert statuses == {"zone-1": PREEMPTED, "manual-1": VOIDED, "local-1": GRANTED}
    decisions = [item["decision"] for item in snapshot["verdicts"]]
    assert decisions == [GRANTED, QUEUED, PREEMPTED, VOIDED, GRANTED]

    zone.release.set()
    local.release.set()
    assert arbiter.wait("local-1", timeout=2.0) is True
    assert arbiter.request("zone-1")["status"] == PREEMPTED
    assert arbiter.request("manual-1")["status"] == VOIDED
    assert zone.calls == 1
    assert manual.calls == 0


def test_manual_does_not_preempt_and_takes_priority_queue_after_completion():
    arbiter = ValveArbiter("st01.mix")
    zone1 = BlockingAction()
    first = arbiter.submit("zone-1", "zone", 30.0, zone1)
    assert first.decision == GRANTED
    assert zone1.started.wait(timeout=2.0)

    zone2 = arbiter.submit("zone-2", "zone", 35.0)
    manual_action = BlockingAction()
    manual = arbiter.submit("manual-1", "manual", 60.0, manual_action)
    assert zone2.decision == QUEUED
    assert manual.decision == QUEUED
    assert [item["command_id"] for item in arbiter.snapshot()["pending"]] == [
        "manual-1",
        "zone-2",
    ]

    zone1.release.set()
    assert arbiter.wait("zone-1", timeout=2.0) is True
    assert arbiter.snapshot()["active"]["command_id"] == "manual-1"
    assert arbiter.request("manual-1")["status"] == GRANTED
    assert manual_action.started.wait(timeout=2.0)
    manual_action.release.set()
    assert arbiter.wait("manual-1", timeout=2.0) is True


def test_same_priority_local_is_arrival_order_not_preemption():
    arbiter = ValveArbiter("st01.mix")
    first_action = BlockingAction()
    first = arbiter.submit("local-1", "local", 30.0, first_action)
    assert first.decision == GRANTED
    assert first_action.started.wait(timeout=2.0)

    second = arbiter.submit("local-2", "local", 40.0)
    assert second.decision == QUEUED
    assert second.reason == "same_priority_arrival_order"
    assert arbiter.snapshot()["active"]["command_id"] == "local-1"
    assert [item["command_id"] for item in arbiter.snapshot()["pending"]] == ["local-2"]


def test_same_command_id_is_effective_once_even_under_concurrency():
    arbiter = ValveArbiter("st01.mix")
    barrier = threading.Barrier(3)
    verdicts: list[str] = []

    def submit() -> None:
        barrier.wait()
        verdict = arbiter.submit("same-id", "zone", 45.0)
        verdicts.append(verdict.decision)

    threads = [threading.Thread(target=submit) for _ in range(3)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert verdicts.count(GRANTED) == 1
    assert verdicts.count("duplicate") == 2
    assert len(arbiter.snapshot()["history"]) == 1
    assert [item["decision"] for item in arbiter.verdicts()].count(GRANTED) == 1


def test_preemption_rule_is_source_priority_and_source_names_are_fixed():
    local = resolve_source("本地联锁")
    manual = resolve_source("值班员")
    zone = resolve_source("分区批量")
    assert local.policy.priority > manual.policy.priority > zone.policy.priority
    assert local.policy.preempt is True
    assert manual.policy.preempt is False
    assert zone.policy.preempt is False
    assert local.source == "本地联锁"


def test_lower_submission_while_local_is_active_is_voided():
    arbiter = ValveArbiter("st01.mix")
    active = BlockingAction()
    assert arbiter.submit("local-1", "local", 80.0, active).decision == GRANTED
    assert active.started.wait(timeout=2.0)

    manual = arbiter.submit("manual-1", "manual", 40.0)
    zone = arbiter.submit("zone-1", "zone", 30.0)
    assert manual.decision == VOIDED
    assert zone.decision == VOIDED
    assert arbiter.snapshot()["pending"] == []
