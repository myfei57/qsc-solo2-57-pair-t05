"""混水阀执行权仲裁。

三路写指令只在 :class:`ValveArbiter` 门口排队：本地联锁按来源优先级立即抢占，
值班员手动指令等当前动作结束后接管，分区批量指令排队等待。抢占只看来源优先级，
同优先级严格按到达顺序；同一 ``command_id`` 在进程内只生效一次，重复提交直接作
废。仲裁结果先交给监听器落审计，再启动动作，保证阀位变化一定能找到对应凭据。
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Callable

GRANTED = "granted"
QUEUED = "queued"
PREEMPTED = "preempted"
VOIDED = "voided"
DUPLICATE = "duplicate"
COMPLETED = "completed"
FAILED = "failed"

TERMINAL_STATUSES = frozenset((COMPLETED, FAILED, PREEMPTED, VOIDED, DUPLICATE))


@dataclass(frozen=True)
class SourcePolicy:
    """一路指令来源的优先级与冲突策略。"""

    name: str
    priority: int
    preempt: bool = False
    void_lower: bool = False


@dataclass(frozen=True)
class SourceRule:
    """来源匹配规则。"""

    aliases: frozenset[str]
    policy: SourcePolicy


# 抢占固定按来源优先级，不按到达顺序；到达顺序只用于同优先级排队。
SOURCE_RULES: tuple[SourceRule, ...] = (
    SourceRule(
        frozenset(("local", "local_interlock", "local-interlock", "interlock", "本地联锁")),
        SourcePolicy("local_interlock", priority=300, preempt=True, void_lower=True),
    ),
    SourceRule(
        frozenset(("operator", "manual", "operator_manual", "shift_operator", "值班员")),
        SourcePolicy("operator_manual", priority=200, preempt=False),
    ),
    SourceRule(
        frozenset(("zone", "zone_batch", "zone-batch", "batch", "dispatch", "分区批量")),
        SourcePolicy("zone_batch", priority=100, preempt=False),
    ),
)


@dataclass(frozen=True)
class SourceDecision:
    """规范化后的来源策略。"""

    source: str
    policy: SourcePolicy

    def as_dict(self) -> dict:
        return {
            "source": self.source,
            "kind": self.policy.name,
            "priority": self.policy.priority,
            "preempt": self.policy.preempt,
        }


def resolve_source(source: str) -> SourceDecision:
    """把外部来源名解析成固定优先级策略。"""

    name = str(source).strip()
    for rule in SOURCE_RULES:
        if name in rule.aliases:
            return SourceDecision(name, rule.policy)
    raise ValueError(f"未知阀门指令来源: {source}")


class CommandCancelled(Exception):
    """动作已经失去执行权，协作者应停止继续写阀。"""


@dataclass
class ValveToken:
    """一次执行权租约；仲裁器抢占时置位取消信号。"""

    command_id: str
    _event: threading.Event = field(default_factory=threading.Event)

    def cancel(self) -> None:
        """撤销本次执行权。"""

        self._event.set()

    def cancelled(self) -> bool:
        """执行权是否已被撤销。"""

        return self._event.is_set()

    def wait(self, timeout: float | None = None) -> bool:
        """等待撤销信号，返回是否已撤销。"""

        return self._event.wait(timeout)

    def raise_if_cancelled(self) -> None:
        """已撤销时抛异常，供动作在关键写阀点检查。"""

        if self.cancelled():
            raise CommandCancelled(f"阀门指令已被抢占: {self.command_id}")


@dataclass
class ValveRequest:
    """一次阀门写指令。"""

    command_id: str
    source: str
    target_pct: float
    seq: int
    submitted_ns: int
    policy: SourcePolicy
    status: str = QUEUED
    result: object = None
    error: str = ""
    action: ValveAction | None = None
    grant_verdict_id: int | None = None

    def as_dict(self) -> dict:
        return {
            "command_id": self.command_id,
            "source": self.source,
            "source_kind": self.policy.name,
            "priority": self.policy.priority,
            "target_pct": round(self.target_pct, 4),
            "seq": self.seq,
            "submitted_ns": self.submitted_ns,
            "status": self.status,
            "result": self.result,
            "error": self.error,
            "grant_verdict_id": self.grant_verdict_id,
        }


@dataclass
class ArbitrationVerdict:
    """一条裁决凭据。"""

    verdict_id: int
    ts: float
    decision: str
    request: ValveRequest
    incumbent: ValveRequest | None = None
    reason: str = ""

    def as_dict(self) -> dict:
        payload = {
            "verdict_id": self.verdict_id,
            "ts": self.ts,
            "decision": self.decision,
            "reason": self.reason,
            "request": self.request.as_dict(),
        }
        if self.incumbent is not None:
            payload["incumbent"] = self.incumbent.as_dict()
        return payload


ValveAction = Callable[[ValveRequest, ValveToken], object]
VerdictListener = Callable[..., None]


class ValveArbiter:
    """单只混水阀的执行权仲裁器。"""

    def __init__(
        self,
        valve_name: str,
        *,
        listener: VerdictListener | None = None,
        time_func: Callable[[], float] | None = None,
    ) -> None:
        self._valve_name = str(valve_name)
        self._listener = listener
        self._time_func = time_func or time.monotonic
        self._lock = threading.RLock()
        self._active: ValveRequest | None = None
        self._active_token: ValveToken | None = None
        self._pending: list[ValveRequest] = []
        self._history: list[ValveRequest] = []
        self._verdicts: list[ArbitrationVerdict] = []
        self._events: dict[str, threading.Event] = {}
        self._seen: set[str] = set()
        self._next_seq = 1
        self._next_verdict = 1

    @property
    def name(self) -> str:
        return self._valve_name

    def submit(
        self,
        command_id: str,
        source: str,
        target_pct: float,
        action: ValveAction | None = None,
    ) -> ArbitrationVerdict:
        """提交一条指令，按来源策略裁决并返回凭据。"""

        identifier = str(command_id)
        if not identifier:
            raise ValueError("command_id 不能为空")
        try:
            target = max(0.0, min(100.0, float(target_pct)))
        except (TypeError, ValueError) as error:
            raise ValueError("目标开度必须是数值") from error
        decision_source = resolve_source(source)

        with self._lock:
            now = self._time_func()
            if identifier in self._seen:
                request = self._request(identifier)
                verdict = self._verdict(DUPLICATE, request, self._active, "duplicate_command", now)
                self._emit(verdict)
                return verdict

            request = ValveRequest(
                command_id=identifier,
                source=decision_source.source,
                target_pct=target,
                seq=self._next_seq,
                submitted_ns=time.monotonic_ns(),
                policy=decision_source.policy,
                action=action,
            )
            self._next_seq += 1
            self._seen.add(identifier)
            self._history.append(request)
            self._events[identifier] = threading.Event()

            if self._active is None:
                granted, voided, starter = self._prepare_grant(
                    request, "actuator_idle", now
                )
                self._emit_all(voided, granted)
                starter()
                return granted

            incumbent = self._active
            if request.policy.preempt and request.policy.priority > incumbent.policy.priority:
                incumbent.status = PREEMPTED
                incumbent.error = "preempted_by_higher_source"
                preempted = self._verdict(
                    PREEMPTED, incumbent, request, "higher_source_preemption", now
                )
                self._active_token.cancel()
                granted, voided, starter = self._prepare_grant(
                    request, "source_priority_preemption", now
                )
                self._emit_all([preempted, *voided], granted)
                starter()
                return granted

            if incumbent.policy.priority > request.policy.priority and incumbent.policy.void_lower:
                request.status = VOIDED
                request.error = "voided_by_higher_source"
                verdict = self._verdict(
                    VOIDED, request, incumbent, "higher_source_holds_execution", now
                )
                self._emit(verdict)
                return verdict

            reason = (
                "same_priority_arrival_order"
                if request.policy.priority == incumbent.policy.priority
                else "wait_for_current_action"
            )
            request.status = QUEUED
            self._pending.append(request)
            self._pending.sort(key=lambda item: (-item.policy.priority, item.seq))
            verdict = self._verdict(QUEUED, request, incumbent, reason, now)
            self._emit(verdict)
            return verdict

    def complete(self, command_id: str, result: object = None) -> ArbitrationVerdict:
        """由自定义执行器在动作正常结束时归还执行权。"""

        return self._finish(str(command_id), COMPLETED, result=result)

    def wait(self, command_id: str, timeout: float | None = None) -> bool:
        """等待某条指令进入终态，返回是否正常完成。"""

        with self._lock:
            request = self._request(command_id)
            event = self._events[request.command_id]
            terminal = request.status in TERMINAL_STATUSES
        if not terminal:
            event.wait(timeout)
        with self._lock:
            request = self._request(command_id)
            return request.status == COMPLETED

    def snapshot(self) -> dict:
        """返回控制台展示用的仲裁状态。"""

        with self._lock:
            return {
                "valve": self._valve_name,
                "active": None if self._active is None else self._active.as_dict(),
                "pending": [request.as_dict() for request in self._pending],
                "history": [request.as_dict() for request in self._history],
                "verdicts": [verdict.as_dict() for verdict in self._verdicts],
                "last_verdict_id": self._next_verdict - 1,
            }

    def verdicts(self, limit: int | None = None) -> list[dict]:
        """返回裁决凭据，最新的在最后。"""

        with self._lock:
            selected = list(self._verdicts) if limit is None else list(self._verdicts)[-limit:]
            return [verdict.as_dict() for verdict in selected]

    def request(self, command_id: str) -> dict:
        """按指令标识返回状态。"""

        with self._lock:
            return self._request(str(command_id)).as_dict()

    def remember(
        self,
        command_id: str,
        source: str,
        target_pct: float,
        status: str,
        *,
        seq: int | None = None,
        ts_ns: int | None = None,
    ) -> None:
        """从落盘审计恢复一条已知指令，保证重启后同一标识仍不能再次生效。"""

        identifier = str(command_id)
        decision_source = resolve_source(source)
        assigned_seq = int(seq) if seq is not None and int(seq) > 0 else None
        with self._lock:
            if identifier in self._seen:
                return
            request = ValveRequest(
                command_id=identifier,
                source=decision_source.source,
                target_pct=max(0.0, min(100.0, float(target_pct))),
                seq=assigned_seq if assigned_seq is not None else self._next_seq,
                submitted_ns=int(ts_ns) if ts_ns is not None else time.monotonic_ns(),
                policy=decision_source.policy,
                status=status if status in TERMINAL_STATUSES else COMPLETED,
            )
            if assigned_seq is None:
                self._next_seq += 1
            else:
                self._next_seq = max(self._next_seq, assigned_seq + 1)
            self._seen.add(identifier)
            self._history.append(request)
            self._events[identifier] = threading.Event()
            if request.status in TERMINAL_STATUSES:
                self._events[identifier].set()

    def _prepare_grant(
        self,
        request: ValveRequest,
        reason: str,
        now: float,
    ) -> tuple[ArbitrationVerdict, list[ArbitrationVerdict], Callable[[], None]]:
        token = ValveToken(request.command_id)
        self._active = request
        self._active_token = token
        request.status = GRANTED
        voided: list[ArbitrationVerdict] = []
        if request.policy.void_lower:
            remaining: list[ValveRequest] = []
            for queued in self._pending:
                if queued.policy.priority < request.policy.priority:
                    queued.status = VOIDED
                    queued.error = "voided_by_higher_source"
                    voided.append(
                        self._verdict(VOIDED, queued, request, "voided_by_granted_source", now)
                    )
                else:
                    remaining.append(queued)
            self._pending = remaining
        granted = self._verdict(GRANTED, request, None, reason, now)
        request.grant_verdict_id = granted.verdict_id

        def starter() -> None:
            if request.action is not None:
                worker = threading.Thread(
                    target=self._run_action,
                    args=(request, token, request.action),
                    name=f"valve-{self._valve_name}-{request.command_id}",
                    daemon=True,
                )
                worker.start()

        return granted, voided, starter

    def _run_action(self, request: ValveRequest, token: ValveToken, action: ValveAction) -> None:
        try:
            result = action(request, token)
        except Exception as error:  # 动作异常也必须归还执行权。
            self._finish(
                request.command_id,
                FAILED,
                result=None,
                error=str(error),
                preempted=token.cancelled(),
            )
            return
        self._finish(
            request.command_id,
            PREEMPTED if token.cancelled() else COMPLETED,
            result=result,
            preempted=token.cancelled(),
        )

    def _finish(
        self,
        command_id: str,
        status: str,
        *,
        result: object = None,
        error: str = "",
        preempted: bool = False,
    ) -> ArbitrationVerdict:
        if status not in (COMPLETED, FAILED, PREEMPTED):
            raise ValueError("终态只能是 completed、failed 或 preempted")
        with self._lock:
            request = self._request(command_id)
            if request.status in TERMINAL_STATUSES:
                verdict = next(item for item in reversed(self._verdicts) if item.request is request)
                return verdict
            if self._active is not request:
                raise ValueError("只能终结当前持有执行权的指令")
            final_status = PREEMPTED if preempted else status
            request.status = final_status
            request.result = result
            request.error = error
            verdict = self._verdict(
                final_status,
                request,
                None,
                "action_preempted" if final_status == PREEMPTED else f"action_{final_status}",
                self._time_func(),
            )
            self._active = None
            self._active_token = None
            self._emit(verdict)

            if self._pending:
                nxt = self._pending.pop(0)
                granted, voided, starter = self._prepare_grant(
                    nxt, "previous_action_finished", self._time_func()
                )
                self._emit_all(voided, granted)
                starter()
            return verdict

    def _verdict(
        self,
        decision: str,
        request: ValveRequest,
        incumbent: ValveRequest | None,
        reason: str,
        now: float,
    ) -> ArbitrationVerdict:
        verdict = ArbitrationVerdict(
            verdict_id=self._next_verdict,
            ts=now,
            decision=decision,
            request=request,
            incumbent=incumbent,
            reason=reason,
        )
        self._next_verdict += 1
        self._verdicts.append(verdict)
        return verdict

    def _emit_all(self, before: list[ArbitrationVerdict], verdict: ArbitrationVerdict) -> None:
        for item in before:
            self._emit(item)
        self._emit(verdict)

    def _emit(self, verdict: ArbitrationVerdict) -> None:
        if self._listener is not None:
            self._listener(verdict)
        if verdict.decision in TERMINAL_STATUSES:
            event = self._events.get(verdict.request.command_id)
            if event is not None:
                event.set()

    def _request(self, command_id: str) -> ValveRequest:
        for request in reversed(self._history):
            if request.command_id == command_id:
                return request
        raise KeyError(f"未知阀门指令: {command_id}")
