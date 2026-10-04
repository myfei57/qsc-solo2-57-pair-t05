"""阀位执行器：仲裁器与物理阀之间的唯一写口。

仲裁器不直接碰 :class:`~heatnet.valve.valve.Valve`，所有改目标、判到位、落盘
都收敛到 :class:`ValveDriver`。这样"同一时刻只有一路拿执行权"在代码结构上有
保证：拿不到执行权的指令拿不到 driver，自然写不了阀。

到位判定遵循工程里既有的"先落盘、再改内存"顺序：抢占只改目标开度，阀位随
``step`` 物理推进；动作走完时先把最终开度落盘，再确认到位。
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Callable

from ..valve.valve import Valve
from .policy import policy_for


@dataclass(frozen=True)
class ValveCommand:
    """一条送到仲裁器的调阀指令，标识全局唯一。"""

    command_id: str
    source: str
    target_pct: float

    def __post_init__(self) -> None:
        if not str(self.command_id):
            raise ValueError("指令标识不能为空")
        policy_for(self.source)  # 未知来源在入口就拦下
        target = float(self.target_pct)
        if not 0.0 <= target <= 100.0:
            raise ValueError("目标开度必须在 0~100 之间")
        object.__setattr__(self, "target_pct", target)


@dataclass(frozen=True)
class HolderView:
    """当前执行权持有方的只读视图。"""

    command_id: str
    source: str
    target_pct: float
    granted_ts: float
    ready_ts: float


# 到位落盘回调：拿到阀门名与最终开度，由装配层决定写到哪里（DurableState）。
PersistPosition = Callable[[str, float], None]


class ValveDriver:
    """把仲裁结论翻译成阀位动作，并负责到位落盘。"""

    def __init__(self, valve: Valve, persist: PersistPosition | None = None) -> None:
        self._valve = valve
        self._persist = persist

    @property
    def valve(self) -> Valve:
        return self._valve

    def travel_seconds(self, target_pct: float) -> float:
        """从当前开度走到目标开度需要的秒数（已经到位则为 0）。"""

        distance = abs(float(target_pct) - self._valve.position_pct())
        if self._valve.limits.settled(self._valve.position_pct(), float(target_pct)):
            return 0.0
        return distance / self._valve.limits.travel_pct_s

    def set_target(self, target_pct: float) -> float:
        """改阀位目标（抢占时调用，阀位随后按行程速率追过去）。"""

        return self._valve.set_target(target_pct)

    def advance(self, dt: float) -> float:
        """把阀位向当前目标推进一个步长（到位即钉在目标开度）。"""

        if dt <= 0:
            raise ValueError("步长必须为正")
        return self._valve.step(dt)

    def snap_to_target(self) -> float:
        """把开度对齐到当前目标（不新增动作计数）。

        行程走完瞬间的内存到位确认用：细粒度推进下开度本来就已停在目标，粗
        粒度跳时则用它把内存开度补齐，两者都不应再多记一次动作。必须在到位
        落盘成功之后才调用，保证"先落盘、再改内存"。
        """

        return self._valve.align_to_target()

    def target(self) -> float:
        """返回当前目标开度（落盘先于内存对齐时，落的就是这个最终值）。"""

        return self._valve.target_pct()

    def persist_final(self, position_pct: float) -> None:
        """动作走完时按工程约定先落盘最终开度；未装配存储时为直通。"""

        if self._persist is not None:
            self._persist(self._valve.name, float(position_pct))

    def snapshot(self) -> dict:
        """返回阀位快照（与阀门自身快照同口径）。"""

        snapshot = self._valve.snapshot()
        return {"position_pct": snapshot.position_pct, "target_pct": snapshot.target_pct}

    @staticmethod
    def ready_at(now: float, travel_seconds: float) -> float:
        """到位时刻取整到毫秒，避免浮点尾差让"最后一步"提前或延后。"""

        return math.ceil((now + travel_seconds) * 1000.0) / 1000.0
