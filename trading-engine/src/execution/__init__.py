"""Order execution layer (live + paper)."""

from src.execution.kill_switch import (
    KILL_SWITCH_PATH,
    KillSwitchState,
    arm_kill_switch,
    disarm_kill_switch,
    is_kill_switch_armed,
    read_kill_switch_state,
    require_kill_switch_clear,
)

__all__ = [
    "KILL_SWITCH_PATH",
    "KillSwitchState",
    "arm_kill_switch",
    "disarm_kill_switch",
    "is_kill_switch_armed",
    "read_kill_switch_state",
    "require_kill_switch_clear",
]
