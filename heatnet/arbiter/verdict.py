"""裁决凭据（裁决单）及其哈希链。

每次裁决都产出一张不可变的 :class:`Verdict`：谁（来源/指令标识）、在什么时刻、
拿到什么结论、对阀位产生了什么影响、上一张凭据是什么。凭据之间以前一条哈希
串成链，后写的把先写的盖掉就会在链上断口——控制台展示与审计落盘读的是同一份
凭据，不存在"两套口径"。

凭据只描述事实，不做任何裁决动作；裁决逻辑在 :mod:`heatnet.arbiter.arbiter`。
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field

GENESIS_HASH = "0" * 64

# 一张指令第一次见到仲裁器：
GRANT = "grant"        # 空闲，直接拿权执行
QUEUE = "queue"        # 等待态：当前有在执动作，排队等放行
VOID = "void"          # 作废：抢占来源遇更高优先级，或不允许排队
DUPLICATE = "duplicate"  # 同一指令标识已受理过，第二次提交直接驳回
# 已受理指令的后续凭据：
PREEMPT = "preempt"    # 抢占成功，原在执指令被打断（记在抢占方名下）
SUPERSEDE = "supersede"  # 同一路持权期间改目标，旧指令被新指令顶替（记在新指令名下）
DISPATCH = "dispatch"  # 在执动作走完，排队队首被放行拿权
COMPLETE = "complete"  # 持权动作正常走完
EXPIRED = "expired"    # 排队条目超过有效期仍未放行，作废
# 被顶替方自己的终态凭据：
INTERRUPTED = "interrupted"  # 持权动作被更高优先级或同一路新指令打断

# 一张指令标识最多产生一张"拿权"凭据（GRANT / DISPATCH），其余均为非拿权结论。
TAKE_EVENTS = frozenset({GRANT, DISPATCH})
# 终态凭据：指令拿到终态结论后，同标识再来的任何提交都算重复。
TERMINAL_EVENTS = frozenset(
    {VOID, DUPLICATE, COMPLETE, EXPIRED, INTERRUPTED}
)
ALL_EVENTS = frozenset(
    {
        GRANT,
        QUEUE,
        VOID,
        DUPLICATE,
        PREEMPT,
        SUPERSEDE,
        DISPATCH,
        COMPLETE,
        EXPIRED,
        INTERRUPTED,
    }
)

# 凭据结论对应的审计严重度，抢占与作废要让值班员一眼看见。
SEVERITY = {
    GRANT: "notice",
    QUEUE: "notice",
    VOID: "warning",
    DUPLICATE: "warning",
    PREEMPT: "alarm",
    SUPERSEDE: "notice",
    DISPATCH: "notice",
    COMPLETE: "notice",
    EXPIRED: "warning",
    INTERRUPTED: "alarm",
}


@dataclass(frozen=True)
class Verdict:
    """一张裁决凭据。"""

    seq: int
    ts: float
    event: str
    command_id: str
    source: str
    target_pct: float | None
    valve_position: float
    valve_target: float
    holder_id: str
    displaced_id: str
    queue_depth: int
    detail: str
    prior_hash: str
    digest: str

    def as_dict(self) -> dict:
        return {
            "seq": self.seq,
            "ts": round(self.ts, 3),
            "event": self.event,
            "command_id": self.command_id,
            "source": self.source,
            "target_pct": None if self.target_pct is None else round(self.target_pct, 2),
            "valve_position": round(self.valve_position, 2),
            "valve_target": round(self.valve_target, 2),
            "holder_id": self.holder_id,
            "displaced_id": self.displaced_id,
            "queue_depth": self.queue_depth,
            "detail": self.detail,
            "prior_hash": self.prior_hash,
            "digest": self.digest,
        }


def _canonical(parts: dict) -> str:
    """把参与哈希的字段规范化，保证同输入永远得到同摘要。"""

    return json.dumps(parts, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def seal(
    seq: int,
    ts: float,
    event: str,
    command_id: str,
    source: str,
    target_pct: float | None,
    valve_position: float,
    valve_target: float,
    holder_id: str,
    displaced_id: str,
    queue_depth: int,
    detail: str,
    prior_hash: str,
) -> Verdict:
    """组装一张凭据并盖上摘要（摘要覆盖全部业务字段与上一条摘要）。"""

    if event not in ALL_EVENTS:
        raise ValueError(f"未知裁决结论 {event!r}")
    body = {
        "seq": int(seq),
        "ts": round(float(ts), 3),
        "event": event,
        "command_id": str(command_id),
        "source": str(source),
        "target_pct": None if target_pct is None else round(float(target_pct), 4),
        "valve_position": round(float(valve_position), 4),
        "valve_target": round(float(valve_target), 4),
        "holder_id": str(holder_id),
        "displaced_id": str(displaced_id),
        "queue_depth": int(queue_depth),
        "detail": str(detail),
        "prior_hash": str(prior_hash),
    }
    digest = hashlib.sha256(_canonical(body).encode("utf-8")).hexdigest()
    return Verdict(
        seq=body["seq"],
        ts=body["ts"],
        event=event,
        command_id=body["command_id"],
        source=source,
        target_pct=target_pct,
        valve_position=valve_position,
        valve_target=valve_target,
        holder_id=holder_id,
        displaced_id=displaced_id,
        queue_depth=queue_depth,
        detail=detail,
        prior_hash=prior_hash,
        digest=digest,
    )


def digest_of(verdict: Verdict) -> str:
    """按凭据字段重算摘要，供链校验使用。"""

    return seal(
        verdict.seq,
        verdict.ts,
        verdict.event,
        verdict.command_id,
        verdict.source,
        verdict.target_pct,
        verdict.valve_position,
        verdict.valve_target,
        verdict.holder_id,
        verdict.displaced_id,
        verdict.queue_depth,
        verdict.detail,
        verdict.prior_hash,
    ).digest


def verify_chain(verdicts: list[Verdict]) -> None:
    """逐条校验哈希链；任何一处对不上就抛出 :class:`ValueError`。"""

    prior = GENESIS_HASH
    for verdict in verdicts:
        if verdict.prior_hash != prior:
            raise ValueError(f"凭据 {verdict.seq} 断链：前摘不匹配")
        if digest_of(verdict) != verdict.digest:
            raise ValueError(f"凭据 {verdict.seq} 摘要被改动")
        prior = verdict.digest
