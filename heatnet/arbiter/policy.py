"""指令来源与裁决策略（定死，不接受运行时覆盖）。

一只混水阀只有三路写方，权限高低与冲突处置方式在设计阶段就固化成常量表：

================  ======  ======  ==========================
来源              优先级  冲突态  拿不到执行权时
================  ======  ======  ==========================
本地联锁 interlock  100   抢占    立即作废（void）
值班员手动 manual    50   抢占    立即作废（void）
分区批量 batch       10   等待    排队（queue）
================  ======  ======  ==========================

两条铁律：

1. **抢占与否只由来源决定**：``preempt`` 来源可以立即打断在执动作，``await``
   来源必须等当前动作走完，任何人不能按"谁先到"改判；
2. **到达顺序永远不提升优先级**：冲突时先比来源优先级，到达顺序只在同来源
   的排队条目之间决定先后（FIFO）。
"""

from __future__ import annotations

from dataclasses import dataclass

# 三个固定来源标识，调用方不允许自创新来源。
INTERLOCK = "interlock"
MANUAL = "manual"
BATCH = "batch"

SOURCES: tuple[str, ...] = (INTERLOCK, MANUAL, BATCH)

# 抢占：拿到权之前若已有在执动作，立即切走；拿不到权（遇到更高优先级）时作废。
PREEMPT = "preempt"
# 等待：不打断在执动作，排队等其走完；等待超过有效期再作废。
AWAIT = "await"

MODES: tuple[str, ...] = (PREEMPT, AWAIT)


@dataclass(frozen=True)
class SourcePolicy:
    """一路指令来源的固定策略。"""

    source: str
    priority: int
    mode: str

    @property
    def preemptive(self) -> bool:
        """该来源是否允许抢占在执动作。"""

        return self.mode == PREEMPT

    def as_dict(self) -> dict:
        return {"source": self.source, "priority": self.priority, "mode": self.mode}


# 下标即来源；数值越大权限越高，联锁永远压过手动，手动压过批量。
POLICY: dict[str, SourcePolicy] = {
    INTERLOCK: SourcePolicy(INTERLOCK, 100, PREEMPT),
    MANUAL: SourcePolicy(MANUAL, 50, PREEMPT),
    BATCH: SourcePolicy(BATCH, 10, AWAIT),
}


def policy_for(source: str) -> SourcePolicy:
    """返回来源策略；未知来源直接拒绝，不允许走默认权限。"""

    try:
        return POLICY[str(source)]
    except KeyError:
        raise ValueError(f"未知指令来源 {source!r}，合法来源：{', '.join(SOURCES)}") from None
