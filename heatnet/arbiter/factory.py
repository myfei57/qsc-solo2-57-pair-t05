"""把仲裁器装到一只物理混水阀上。

装配层负责两件装配代码不该内联进仲裁器的事：

1. 用既有 :func:`~heatnet.valve.move.move_valve` 的"先落盘、再改内存"口径构造
   到位落盘回调，让仲裁器在不依赖存储模块的前提下仍然遵守同一条落盘顺序；
2. 为每只阀建一个 :class:`~heatnet.arbiter.arbiter.ValveArbiter`，共享时钟与
   事件总线——审计与控制台订阅同一条总线，裁决凭据一处产出、两处可见。
"""

from __future__ import annotations

from ..clock import Clock
from ..eventbus import EventBus
from ..store.state import DurableState
from ..valve.valve import Valve
from .arbiter import ValveArbiter
from .driver import ValveDriver


def build_arbiter(
    valve: Valve,
    clock: Clock,
    bus: EventBus,
    durable: DurableState | None = None,
    *,
    queue_ttl_s: float = 300.0,
) -> ValveArbiter:
    """为一只阀构造执行权仲裁器；``durable`` 给定时到位先落盘再确认。"""

    def persist(name: str, position_pct: float) -> None:
        if durable is None:
            return
        # 与 move_valve 相同的载荷形状，复用同一份当前态文档。
        durable.save(
            f"valve/{name}",
            {
                "name": name,
                "position_pct": round(float(position_pct), 4),
                "moves": valve.moves(),
            },
        )

    driver = ValveDriver(valve, persist if durable is not None else None)
    return ValveArbiter(driver, clock, bus, queue_ttl_s=queue_ttl_s)
