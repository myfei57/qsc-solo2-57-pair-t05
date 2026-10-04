"""混水阀执行权仲裁。

三路写方（``batch`` 分区批量、``manual`` 值班员手动、``interlock`` 本地联锁）
对同一只混水阀的调节必须经过 :class:`~heatnet.arbiter.arbiter.ValveArbiter`：
来源策略在 :mod:`~heatnet.arbiter.policy` 里定死（联锁/手动抢占、批量等待，
优先级只认来源），裁决结论成 :mod:`~heatnet.arbiter.verdict` 哈希链凭据，阀位
动作统一走 :class:`~heatnet.arbiter.driver.ValveDriver`。
"""

from __future__ import annotations

from .arbiter import ValveArbiter
from .driver import HolderView, ValveCommand, ValveDriver
from .policy import (
    AWAIT,
    BATCH,
    INTERLOCK,
    MANUAL,
    MODES,
    POLICY,
    PREEMPT,
    SOURCES,
    SourcePolicy,
    policy_for,
)
from .verdict import (
    ALL_EVENTS,
    COMPLETE,
    DISPATCH,
    DUPLICATE,
    EXPIRED,
    GRANT,
    INTERRUPTED,
    PREEMPT as PREEMPT_EVENT,
    QUEUE,
    SUPERSEDE,
    TAKE_EVENTS,
    TERMINAL_EVENTS,
    VOID,
    Verdict,
    digest_of,
    seal,
    verify_chain,
)

__all__ = [
    "ValveArbiter",
    "ValveCommand",
    "ValveDriver",
    "HolderView",
    "SourcePolicy",
    "policy_for",
    "POLICY",
    "SOURCES",
    "MODES",
    "BATCH",
    "MANUAL",
    "INTERLOCK",
    "PREEMPT",
    "AWAIT",
    "Verdict",
    "seal",
    "digest_of",
    "verify_chain",
    "ALL_EVENTS",
    "TAKE_EVENTS",
    "TERMINAL_EVENTS",
    "GRANT",
    "QUEUE",
    "VOID",
    "DUPLICATE",
    "PREEMPT_EVENT",
    "SUPERSEDE",
    "DISPATCH",
    "COMPLETE",
    "EXPIRED",
    "INTERRUPTED",
]
