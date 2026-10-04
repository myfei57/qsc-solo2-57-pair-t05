"""单阀执行权仲裁器。

三路写方（分区批量、值班员手动、本地联锁）不再各自直接写阀，统一走
:meth:`ValveArbiter.submit`。仲裁器在一把锁内完成三件事，顺序不可拆：

1. **判重**：指令标识原子登记，同一路下并发提交的同标识指令只有第一次生效，
   其余直接拿 ``duplicate``；
2. **裁决**：按 :mod:`heatnet.arbiter.policy` 里定死的来源策略给出 grant /
   queue / void / preempt / supersede 结论，到达顺序永不提升优先级；
3. **落凭据**：每张结论成一张哈希链凭据，经事件总线同时送给控制台展示与
   审计落盘，凭据上的阀位就是当时的真实阀位——裁决、阀位、审计同源。

时间推进有两种用法：可推进时钟的标定/测试场景直接调 :meth:`tick`（处理到当前
时刻已到期的动作）；真实时钟场景调 :meth:`advance` 让阀位按行程速率推进并
处理到期动作。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field

from ..clock import Clock
from ..eventbus import Event, EventBus
from .driver import HolderView, ValveCommand, ValveDriver
from .policy import AWAIT, POLICY, policy_for
from .verdict import (
    COMPLETE,
    DISPATCH,
    DUPLICATE,
    EXPIRED,
    GENESIS_HASH,
    GRANT,
    INTERRUPTED,
    PREEMPT,
    QUEUE,
    SEVERITY,
    SUPERSEDE,
    VOID,
    Verdict,
    seal,
    verify_chain,
)

COMPONENT = "valve-arbiter"


@dataclass(frozen=True)
class _Waiting:
    """队列里一条等待放行的指令。"""

    command: ValveCommand
    arrival: int
    enqueue_ts: float
    expires_ts: float


@dataclass(frozen=True)
class ArbiterStatus:
    """仲裁器当前状态快照（控制台展示用）。"""

    valve: str
    holder: HolderView | None
    waiting: tuple[HolderView, ...] = field(default_factory=tuple)
    verdicts: int = 0
    grants: int = 0
    preempts: int = 0
    supersedes: int = 0
    queued: int = 0
    voided: int = 0
    duplicates: int = 0
    expired: int = 0
    last_digest: str = GENESIS_HASH

    def as_dict(self) -> dict:
        return {
            "valve": self.valve,
            "holder": None
            if self.holder is None
            else {
                "command_id": self.holder.command_id,
                "source": self.holder.source,
                "target_pct": round(self.holder.target_pct, 2),
                "granted_ts": round(self.holder.granted_ts, 3),
                "ready_ts": round(self.holder.ready_ts, 3),
            },
            "waiting": [
                {
                    "command_id": item.command_id,
                    "source": item.source,
                    "target_pct": round(item.target_pct, 2),
                    "priority": policy_for(item.source).priority,
                }
                for item in self.waiting
            ],
            "counts": {
                "verdicts": self.verdicts,
                "grant": self.grants,
                "preempt": self.preempts,
                "supersede": self.supersedes,
                "queue": self.queued,
                "void": self.voided,
                "duplicate": self.duplicates,
                "expired": self.expired,
            },
            "last_digest": self.last_digest,
        }


class ValveArbiter:
    """一只混水阀对应一个仲裁器。"""

    def __init__(
        self,
        driver: ValveDriver,
        clock: Clock,
        bus: EventBus,
        *,
        queue_ttl_s: float = 300.0,
        seen_ttl_s: float = 3600.0,
        seen_limit: int = 4096,
    ) -> None:
        if queue_ttl_s <= 0:
            raise ValueError("排队有效期必须为正")
        self._driver = driver
        self._clock = clock
        self._bus = bus
        self._queue_ttl = float(queue_ttl_s)
        self._seen_ttl = float(seen_ttl_s)
        self._seen_limit = max(1, int(seen_limit))
        self._lock = threading.RLock()
        self._holder: HolderView | None = None
        self._waiting: list[_Waiting] = []
        self._seen: dict[str, float] = {}
        self._arrivals = 0
        self._verdicts: list[Verdict] = []
        self._last_hash = GENESIS_HASH
        self._last_completed: Verdict | None = None
        self._last_now: float | None = None
        # 执行权时间线游标：上一次泵处理到的事件时刻，粗粒度跳时也不丢中间事件。
        self._pump_cursor: float | None = None

    @property
    def driver(self) -> ValveDriver:
        return self._driver

    def submit(self, command: ValveCommand) -> dict:
        """提交一条调阀指令，返回本次裁决凭据（字典形式）。

        判重、裁决、出凭据在同一把锁内完成，因此并发提交同标识指令时，谁先
        拿到锁谁生效，其余一律 ``duplicate``——"同一标识只生效一次"不依赖
        调用方自觉。
        """

        with self._lock:
            now = self._clock.now()
            self._purge_seen(now)
            self._elapse_locked(now)
            if command.command_id in self._seen:
                verdict = self._record(
                    now,
                    DUPLICATE,
                    command,
                    detail=f"{command.command_id} 已受理，重复提交作废",
                    displaced_id=self._holder.command_id if self._holder else "",
                )
                return verdict.as_dict()
            self._seen[command.command_id] = now
            return self._decide_locked(command, now).as_dict()

    def tick(self) -> list[dict]:
        """控制周期：按距上次的经过时间推阀位，处理到期动作与排队。"""

        with self._lock:
            return [item.as_dict() for item in self._elapse_locked(self._clock.now())]

    def advance(self, seconds: float, step_s: float = 0.1) -> list[dict]:
        """推进时钟并让阀位按行程速率走，返回期间产出的凭据。

        可推进时钟会被一次性拨到目标时刻；``step_s`` 只把推进切成多个周期，
        便于观察在途凭据，不改变最终裁决。
        """

        if seconds < 0:
            raise ValueError("时间不能回退")
        produced: list[dict] = []
        with self._lock:
            remaining = float(seconds)
            while remaining > 1e-9:
                dt = min(step_s, remaining)
                self._clock.sleep(dt)
                produced.extend(item.as_dict() for item in self._elapse_locked(self._clock.now()))
                remaining -= dt
        return produced

    def is_idle(self) -> bool:
        """当前是否既无持权动作也无排队。"""

        with self._lock:
            return self._holder is None and not self._waiting

    def status(self) -> dict:
        """返回控制台展示用的状态快照。"""

        with self._lock:
            counts = {
                GRANT: 0,
                PREEMPT: 0,
                SUPERSEDE: 0,
                QUEUE: 0,
                VOID: 0,
                DUPLICATE: 0,
                EXPIRED: 0,
                COMPLETE: 0,
                DISPATCH: 0,
            }
            for verdict in self._verdicts:
                counts[verdict.event] = counts.get(verdict.event, 0) + 1
            snapshot = ArbiterStatus(
                valve=self._driver.valve.name,
                holder=self._holder,
                waiting=tuple(item.command for item in self._waiting),
                verdicts=len(self._verdicts),
                grants=counts[GRANT] + counts[DISPATCH],
                preempts=counts[PREEMPT],
                supersedes=counts[SUPERSEDE],
                queued=counts[QUEUE],
                voided=counts[VOID],
                duplicates=counts[DUPLICATE],
                expired=counts[EXPIRED],
                last_digest=self._last_hash,
            )
            return snapshot.as_dict()

    def verdicts(self, limit: int | None = None) -> list[dict]:
        """返回裁决凭据（按时间升序），供控制台与审计核对。"""

        with self._lock:
            items = list(self._verdicts) if limit is None else list(self._verdicts)[-limit:]
            return [item.as_dict() for item in items]

    def verify(self) -> None:
        """校验全部凭据哈希链；被人改过顺序或内容会在此暴露。"""

        with self._lock:
            verify_chain(list(self._verdicts))

    # ---- 锁内裁决 -----------------------------------------------------

    def _decide_locked(self, command: ValveCommand, now: float) -> Verdict:
        policy = policy_for(command.source)
        holder = self._holder

        # 空闲：唯一拿权路径之一，立即执行。
        if holder is None:
            return self._grant_locked(command, now, GRANT, "")

        # 同一路持权期间再次下达：改目标（supersede），旧指令被新指令顶替。
        if holder.source == command.source:
            old = ValveCommand(holder.command_id, holder.source, holder.target_pct)
            self._driver.set_target(command.target_pct)
            view = HolderView(
                command_id=command.command_id,
                source=command.source,
                target_pct=command.target_pct,
                granted_ts=holder.granted_ts,
                ready_ts=ValveDriver.ready_at(now, self._driver.travel_seconds(command.target_pct)),
            )
            self._holder = view
            # 先给被顶替的旧指令落终态凭据，再记新指令的顶替凭据，时序不颠倒。
            self._record(
                now,
                INTERRUPTED,
                old,
                displaced_id=command.command_id,
                detail=f"同一路 {command.source} 改目标，{holder.command_id} 被打断",
            )
            return self._record(
                now,
                SUPERSEDE,
                command,
                displaced_id=old.command_id,
                detail=f"同一路 {command.source} 改目标，顶替 {old.command_id}",
            )

        # 不同来源：只比来源优先级，到达顺序不参与。
        incoming_priority = POLICY[command.source].priority
        holder_priority = POLICY[holder.source].priority
        if incoming_priority > holder_priority:
            if not policy.preemptive:
                # 高优先级却是等待态（当前策略表里不存在，留作策略扩展时的防线）：
                # 不允许高权限指令悄悄插队，按其模式排队。
                return self._enqueue_locked(command, now)
            displaced = holder
            old = ValveCommand(displaced.command_id, displaced.source, displaced.target_pct)
            self._holder = None
            # 抢占的时序：被抢方先落 interrupted，抢占方再落 preempt，
            # 同一把锁内完成，中间不可能插进来第三条指令。
            self._record(
                now,
                INTERRUPTED,
                old,
                displaced_id=command.command_id,
                detail=f"{command.source} 抢占，{displaced.source} 的在执动作被打断",
            )
            return self._grant_locked(
                command,
                now,
                PREEMPT,
                displaced.command_id,
                detail=f"{command.source} 优先级更高，抢占 {displaced.source} 的在执动作",
            )

        # 平级或更低：抢占来源不排队，立即作废；等待来源排队。
        if policy.mode == AWAIT:
            return self._enqueue_locked(command, now)
        return self._record(
            now,
            VOID,
            command,
            displaced_id=holder.command_id,
            detail=(
                f"{command.source} 优先级不高于在执的 {holder.source}，"
                "抢占指令立即作废"
            ),
        )

    def _grant_locked(
        self, command: ValveCommand, now: float, event: str, displaced_id: str, detail: str = ""
    ) -> Verdict:
        """执行拿权；若已到位则在同一把锁内同步走完并释放执行权。"""

        self._driver.set_target(command.target_pct)
        travel = self._driver.travel_seconds(command.target_pct)
        ready_ts = ValveDriver.ready_at(now, travel)
        self._holder = HolderView(
            command_id=command.command_id,
            source=command.source,
            target_pct=command.target_pct,
            granted_ts=now,
            ready_ts=ready_ts,
        )
        if not detail:
            detail = f"{command.source} 拿到执行权，目标开度 {round(command.target_pct, 2)}%"
        granted = self._record(now, event, command, displaced_id=displaced_id, detail=detail)
        # 游标锚到拿权时刻；有行程则执行权占用到 ready_ts，零行程立即释放。
        self._pump_cursor = ready_ts if ready_ts > now else now
        if ready_ts <= now:
            self._complete_holder_locked(now)
        return granted

    def _enqueue_locked(self, command: ValveCommand, now: float) -> Verdict:
        self._arrivals += 1
        expires = now + self._queue_ttl
        # 纯到达 FIFO：优先级只决定能不能抢占，排队不按来源重排，同来源的不同
        # 标识指令也各自排队，由其自身标识的判重保证幂等。
        self._waiting.append(
            _Waiting(
                command=command,
                arrival=self._arrivals,
                enqueue_ts=now,
                expires_ts=expires,
            )
        )
        detail = (
            f"{command.source} 排队等待执行权（第 {len(self._waiting)} 位），"
            f"有效期至 {round(expires, 3)}"
        )
        return self._record(now, QUEUE, command, detail=detail)

    def _elapse_locked(self, now: float) -> list[Verdict]:
        """推进到 ``now``：先让阀位按经过时间走完，再处理到期与排队。"""

        if self._last_now is None:
            self._last_now = now
        dt = now - self._last_now
        if dt < 0:
            raise ValueError("时间不能回退")
        self._last_now = now
        # 有持权动作时，阀位按行程速率向目标推进；到位后 settle_band 内
        # valve.step 会停在目标开度，ready_ts 到点即判走完。
        if self._holder is not None and dt > 0:
            self._driver.advance(dt)
        return self._pump_locked(now)

    def _pump_locked(self, now: float) -> list[Verdict]:
        """按真实事件时刻把执行权时间线推进到 ``now``。

        关键：即使本次调用一次性跳过了很长一段时间（控制周期粗、或可推进时钟
        被直接拨到远处），也必须逐段算出每个事件的实际发生时刻——

        - 持权动作在 ``ready_ts`` 走完；
        - 队首最早在"执行权空闲时刻"被放行，若放行时刻晚于它的有效期，
          它是在有效期内没轮到，判 ``expired``；反之放行并按其行程占用执行权；
        - 同一路零行程指令放行即走完，执行权不后移，继续放后续队首。

        只在区间终点统一裁决会把"TTL 之前就该放行"的队首误判成过期，因此这里
        维护一条从当前时刻滚动到 ``now`` 的时间线，产出的每张凭据都带它真实
        的发生时刻（时间戳单调不倒挂）。
        """

        produced: list[Verdict] = []
        # 时间线游标沿真实事件时刻滚动：拿权时锚定、每次泵只向前。
        cursor = now if self._pump_cursor is None else self._pump_cursor
        while True:
            # 持权动作：把它的走完时刻纳入时间线。
            if self._holder is not None:
                if self._holder.ready_ts > now:
                    break
                ready_ts = max(cursor, self._holder.ready_ts)
                self._complete_holder_locked(ready_ts)
                cursor = ready_ts
                self._pump_cursor = cursor
                produced.append(self._last_completed)
                continue
            if not self._waiting:
                break
            head = self._waiting[0]
            # 队首放行时刻 = 当前时间线位置（执行权空闲时刻），不会早于入队。
            dispatch_ts = max(cursor, head.enqueue_ts)
            if dispatch_ts >= head.expires_ts:
                self._waiting.pop(0)
                cursor = max(cursor, head.expires_ts)
                produced.append(
                    self._record(
                        min(now, head.expires_ts),
                        EXPIRED,
                        head.command,
                        detail=(
                            f"排队到有效期 {round(head.expires_ts, 3)} 仍未轮到放行，作废"
                        ),
                    )
                )
                continue
            if dispatch_ts > now:
                break  # 执行权要到 now 之后才空得下来，本次推进到此为止
            self._waiting.pop(0)
            granted = self._grant_locked(
                head.command,
                dispatch_ts,
                DISPATCH,
                "",
                detail=(
                    "执行权空闲后放行排队队首"
                    f"（已等待 {round(dispatch_ts - head.enqueue_ts, 3)}s）"
                ),
            )
            produced.append(granted)
            if self._holder is None:
                # 零行程：complete 已在 grant 内于 dispatch_ts 记录。
                produced.append(self._last_completed)
                cursor = dispatch_ts
            else:
                cursor = self._holder.ready_ts
        self._pump_cursor = cursor
        return produced

    def _complete_holder_locked(self, now: float) -> None:
        holder = self._holder
        if holder is None:
            return
        # 先落盘最终开度（持权目标值），成功后才把内存阀位对齐并出 complete
        # 凭据——与工程其他调阀路径同一条"先落盘、再改内存"顺序；落盘失败则
        # 异常上抛、执行权不释放、不出凭据，调用方可原样重试。
        target = self._driver.target()
        self._driver.persist_final(target)
        position = self._driver.snap_to_target()
        command = ValveCommand(holder.command_id, holder.source, holder.target_pct)
        self._holder = None
        self._last_completed = self._record(
            now,
            COMPLETE,
            command,
            detail=f"动作走完，阀位确认在 {round(position, 2)}%",
        )

    # ---- 凭据与判重表 -------------------------------------------------

    def _record(
        self,
        ts: float,
        event: str,
        command: ValveCommand,
        *,
        displaced_id: str = "",
        detail: str = "",
    ) -> Verdict:
        valve = self._driver.snapshot()
        verdict = seal(
            seq=len(self._verdicts) + 1,
            ts=ts,
            event=event,
            command_id=command.command_id,
            source=command.source,
            target_pct=command.target_pct,
            valve_position=valve["position_pct"],
            valve_target=valve["target_pct"],
            holder_id=self._holder.command_id if self._holder else "",
            displaced_id=displaced_id,
            queue_depth=len(self._waiting),
            detail=detail,
            prior_hash=self._last_hash,
        )
        self._verdicts.append(verdict)
        self._last_hash = verdict.digest
        self._bus.publish(
            Event.of(
                "arbiter",
                COMPONENT,
                event,
                severity=SEVERITY[event],
                message=detail,
                asset=self._driver.valve.name,
                command_id=command.command_id,
                source=command.source,
                priority=policy_for(command.source).priority,
                target_pct=round(command.target_pct, 2),
                valve_position=round(valve["position_pct"], 2),
                valve_target=round(valve["target_pct"], 2),
                holder_id=verdict.holder_id,
                displaced_id=displaced_id,
                queue_depth=len(self._waiting),
                verdict_seq=verdict.seq,
                digest=verdict.digest,
                prior_hash=verdict.prior_hash,
            )
        )
        return verdict

    def _purge_seen(self, now: float) -> None:
        """丢弃超过保留期的已受理标识（活动中的指令标识永不丢）。"""

        active = {item.command.command_id for item in self._waiting}
        if self._holder is not None:
            active.add(self._holder.command_id)
        stale = [
            command_id
            for command_id, seen_ts in self._seen.items()
            if command_id not in active and now - seen_ts > self._seen_ttl
        ]
        for command_id in stale:
            self._seen.pop(command_id, None)
        if len(self._seen) <= self._seen_limit:
            return
        ordered = sorted(self._seen.items(), key=lambda pair: pair[1])
        for command_id, _seen_ts in ordered[: len(self._seen) - self._seen_limit]:
            if command_id not in active:
                self._seen.pop(command_id, None)
