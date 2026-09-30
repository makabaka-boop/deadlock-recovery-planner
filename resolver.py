"""死锁解除模拟器.

后端接收至多 12 个作业与 15 个单实例资源的当前状态:

- 每个资源至多由一个作业持有, 每个作业可持有多个资源;
- 每个作业同一时刻至多等待一个资源 (waits_for 为资源 ID 或 None);
- 部分作业不可中止 (受保护), 其余作业各有正整数中止代价.

模拟规则: 反复执行以下两步直至不能前进 ——

1. 把每个空闲资源授予等待它的作业中 ID 最小者;
2. 让不再等待的作业完成, 释放其所持全部资源.

若仍有作业卡住, 则精确枚举可中止作业子集, 选出总中止代价最小、
且中止并释放资源后剩余作业均能完成的集合; 代价并列时取排序后
中止 ID 序列最小者. 受保护作业导致无解时明确报告 (infeasible),
绝不强行释放其资源.

入口: ``resolve(state: dict) -> dict``.
"""

MAX_JOBS = 12
MAX_RESOURCES = 15


class InvalidState(ValueError):
    """输入状态不合法."""


def _is_int(value):
    return isinstance(value, int) and not isinstance(value, bool)


class _Job:
    __slots__ = ("id", "holds", "waits_for", "abortable", "abort_cost")

    def __init__(self, job_id, holds, waits_for, abortable, abort_cost):
        self.id = job_id
        self.holds = set(holds)
        self.waits_for = waits_for
        self.abortable = abortable
        self.abort_cost = abort_cost


class _Sim:
    """一次模拟: 维护持有者表并产生事件回放."""

    def __init__(self, jobs, resources):
        self.jobs = jobs                  # job_id -> _Job
        self.resources = set(resources)
        self.holder = {}                  # resource_id -> job_id
        for job in jobs.values():
            for rid in job.holds:
                self.holder[rid] = job.id
        self.events = []
        self.completed = set()
        self.aborted = set()

    def clone(self):
        """复制当前局面用于试算, 不继承也不产生事件."""
        other = _Sim.__new__(_Sim)
        other.jobs = {
            jid: _Job(j.id, j.holds, j.waits_for, j.abortable, j.abort_cost)
            for jid, j in self.jobs.items()
        }
        other.resources = set(self.resources)
        other.holder = dict(self.holder)
        other.events = []
        other.completed = set(self.completed)
        other.aborted = set(self.aborted)
        return other

    def running_ids(self):
        return [
            jid for jid in self.jobs
            if jid not in self.completed and jid not in self.aborted
        ]

    def _grant(self, jid, rid, record=True):
        job = self.jobs[jid]
        self.holder[rid] = jid
        job.holds.add(rid)
        job.waits_for = None
        if record:
            self.events.append({"type": "grant", "job": jid, "resource": rid})

    def _complete(self, jid, record=True):
        job = self.jobs[jid]
        released = sorted(job.holds)
        for rid in released:
            del self.holder[rid]
        job.holds.clear()
        self.completed.add(jid)
        if record:
            self.events.append(
                {"type": "complete", "job": jid, "released": released})

    def _abort(self, jid, record=True):
        job = self.jobs[jid]
        released = sorted(job.holds)
        for rid in released:
            del self.holder[rid]
        job.holds.clear()
        job.waits_for = None
        self.aborted.add(jid)
        if record:
            self.events.append(
                {"type": "abort", "job": jid, "released": released})

    def run_until_stall(self, record=True):
        """交替执行授予与完成, 直至整轮无任何进展."""
        while True:
            progress = False
            # 1) 空闲资源授予等待它的最小 ID 作业
            for rid in sorted(self.resources):
                if rid in self.holder:
                    continue
                waiters = [
                    jid for jid in self.running_ids()
                    if self.jobs[jid].waits_for == rid
                ]
                if waiters:
                    self._grant(min(waiters), rid, record)
                    progress = True
            # 2) 不再等待的作业完成并释放资源
            for jid in sorted(self.running_ids()):
                if self.jobs[jid].waits_for is None:
                    self._complete(jid, record)
                    progress = True
            if not progress:
                return


def _parse_and_validate(state):
    if not isinstance(state, dict):
        raise InvalidState("状态必须是 JSON 对象")
    raw_jobs = state.get("jobs")
    raw_resources = state.get("resources")
    if not isinstance(raw_jobs, list):
        raise InvalidState("缺少 jobs 列表")
    if not isinstance(raw_resources, list):
        raise InvalidState("缺少 resources 列表")
    if len(raw_jobs) > MAX_JOBS:
        raise InvalidState("作业数超过上限 %d" % MAX_JOBS)
    if len(raw_resources) > MAX_RESOURCES:
        raise InvalidState("资源数超过上限 %d" % MAX_RESOURCES)

    resources = []
    for rid in raw_resources:
        if not _is_int(rid):
            raise InvalidState("资源 ID 必须是整数: %r" % (rid,))
        resources.append(rid)
    if len(set(resources)) != len(resources):
        raise InvalidState("资源 ID 存在重复")
    resource_set = set(resources)

    jobs = {}
    for entry in raw_jobs:
        if not isinstance(entry, dict):
            raise InvalidState("作业条目必须是对象")
        jid = entry.get("id")
        if not _is_int(jid):
            raise InvalidState("作业 ID 必须是整数: %r" % (jid,))
        if jid in jobs:
            raise InvalidState("作业 %d 重复" % jid)

        holds = entry.get("holds", [])
        if not isinstance(holds, list) or any(not _is_int(r) for r in holds):
            raise InvalidState("作业 %d 的 holds 必须是整数资源 ID 列表" % jid)
        if len(set(holds)) != len(holds):
            raise InvalidState("作业 %d 重复持有同一资源" % jid)
        unknown = [r for r in holds if r not in resource_set]
        if unknown:
            raise InvalidState("作业 %d 持有未声明的资源 %r" % (jid, unknown))

        waits_for = entry.get("waits_for")
        if waits_for is not None:
            if not _is_int(waits_for):
                raise InvalidState(
                    "作业 %d 的 waits_for 必须是资源 ID 或 null" % jid)
            if waits_for not in resource_set:
                raise InvalidState(
                    "作业 %d 等待未声明的资源 %d" % (jid, waits_for))
            if waits_for in holds:
                raise InvalidState(
                    "作业 %d 等待自己持有的资源 %d" % (jid, waits_for))

        abortable = entry.get("abortable", True)
        if not isinstance(abortable, bool):
            raise InvalidState("作业 %d 的 abortable 必须是布尔值" % jid)
        cost = entry.get("abort_cost")
        if abortable:
            if not _is_int(cost) or cost <= 0:
                raise InvalidState(
                    "可中止作业 %d 的 abort_cost 必须是正整数" % jid)
        else:
            cost = None

        jobs[jid] = _Job(jid, holds, waits_for, abortable, cost)

    holder_of = {}
    for jid, job in jobs.items():
        for rid in job.holds:
            if rid in holder_of:
                raise InvalidState(
                    "资源 %d 被作业 %d 和 %d 同时持有" % (rid, holder_of[rid], jid))
            holder_of[rid] = jid
    return jobs, resource_set


def _select_abort_set(sim, stuck):
    """在卡住的可中止作业中枚举子集, 返回 (总代价, 排序后 ID 元组) 或 None.

    最小化总中止代价; 并列时取排序后 ID 序列最小者.
    """
    abortable = sorted(j for j in stuck if sim.jobs[j].abortable)
    best = None
    for mask in range(1, 1 << len(abortable)):
        subset = [abortable[i] for i in range(len(abortable)) if mask & (1 << i)]
        cost = sum(sim.jobs[j].abort_cost for j in subset)
        ids = tuple(sorted(subset))
        if best is not None and (cost, ids) >= best:
            continue
        trial = sim.clone()
        for jid in subset:
            trial._abort(jid, record=False)
        trial.run_until_stall(record=False)
        if not trial.running_ids():
            best = (cost, ids)
    return best


def resolve(state):
    """接收当前状态, 返回授予/完成/中止的完整回放.

    返回 dict, 字段:
      status      "resolved" | "infeasible"
      events      按序事件: grant / complete / abort
      completed   已完成作业 ID (升序)
      aborted     被中止作业 ID (升序)
      abort_cost  总中止代价
      stuck       仅 infeasible 时非空: 卡住作业的明细
      message     仅 infeasible 时存在: 无解原因
    """
    jobs, resources = _parse_and_validate(state)
    sim = _Sim(jobs, resources)
    sim.run_until_stall(record=True)

    stuck = sim.running_ids()
    aborted_ids = []
    abort_cost = 0

    if stuck:
        best = _select_abort_set(sim, stuck)
        if best is None:
            stuck_detail = [
                {
                    "id": jid,
                    "holds": sorted(sim.jobs[jid].holds),
                    "waits_for": sim.jobs[jid].waits_for,
                    "abortable": sim.jobs[jid].abortable,
                }
                for jid in sorted(stuck)
            ]
            protected = [d["id"] for d in stuck_detail if not d["abortable"]]
            return {
                "status": "infeasible",
                "message": (
                    "死锁无法解除: 即使中止全部可中止作业, 受保护作业 %s "
                    "仍相互等待; 不会强行释放其资源" % protected
                ),
                "events": sim.events,
                "completed": sorted(sim.completed),
                "aborted": [],
                "abort_cost": 0,
                "stuck": stuck_detail,
            }
        abort_cost, ids = best
        aborted_ids = list(ids)
        for jid in ids:
            sim._abort(jid, record=True)
        sim.run_until_stall(record=True)

    return {
        "status": "resolved",
        "events": sim.events,
        "completed": sorted(sim.completed),
        "aborted": aborted_ids,
        "abort_cost": abort_cost,
        "stuck": [],
    }
