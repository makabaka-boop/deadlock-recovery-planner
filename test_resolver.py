"""测试: 枚举小规模中止子集并独立重放, 核对死锁解除与最小性.

运行: python3 -m unittest test_resolver -v
"""

import copy
import http.client
import itertools
import json
import random
import threading
import unittest

from resolver import InvalidState, resolve


# ---------------------------------------------------------------------------
# 独立重放器: 在测试中重新实现模拟规则, 不依赖 resolver 内部,
# 用于核对 "中止某子集后剩余作业均能完成".
# ---------------------------------------------------------------------------

def independent_replay(jobs_spec, aborted=()):
    """返回 (completed, stuck): 中止 aborted 后按规则模拟到不能前进."""
    holds = {j["id"]: set(j.get("holds", [])) for j in jobs_spec}
    waits = {j["id"]: j.get("waits_for") for j in jobs_spec}
    resources = set()
    for j in jobs_spec:
        resources |= set(j.get("holds", []))
        if j.get("waits_for") is not None:
            resources.add(j["waits_for"])

    aborted = set(aborted)
    done = set()
    holder = {}
    for jid, rs in holds.items():
        for r in rs:
            holder[r] = jid
    for jid in aborted:
        for r in holds[jid]:
            holder.pop(r, None)
        holds[jid] = set()
        waits[jid] = None

    changed = True
    while changed:
        changed = False
        # 空闲资源授予等待它的最小 ID 作业
        for r in sorted(resources):
            if r not in holder:
                waiters = [j for j in waits
                           if j not in done and j not in aborted
                           and waits[j] == r]
                if waiters:
                    j = min(waiters)
                    holder[r] = j
                    holds[j].add(r)
                    waits[j] = None
                    changed = True
        # 不再等待的作业完成并释放资源
        for j in sorted(waits):
            if j not in done and j not in aborted and waits[j] is None:
                done.add(j)
                for r in holds[j]:
                    holder.pop(r, None)
                holds[j] = set()
                changed = True
    stuck = set(waits) - done - aborted
    return done, stuck


# ---------------------------------------------------------------------------
# 回放合法性核对: 逐步折叠事件流, 验证每步都符合模拟规则.
# ---------------------------------------------------------------------------

def fold_events(state, result):
    """逐步核对回放并折叠, 返回 (alive, waits, holder)."""
    holds = {j["id"]: set(j.get("holds", [])) for j in state["jobs"]}
    waits = {j["id"]: j.get("waits_for") for j in state["jobs"]}
    abortable = {j["id"]: j.get("abortable", True) for j in state["jobs"]}
    alive = set(holds)
    holder = {}
    for jid, rs in holds.items():
        for r in rs:
            assert r not in holder, "输入中资源被重复持有"
            holder[r] = jid

    for ev in result["events"]:
        t = ev["type"]
        j = ev["job"]
        assert j in alive, "事件作用于已结束作业: %r" % (ev,)
        if t == "grant":
            r = ev["resource"]
            assert waits[j] == r, "作业 %d 并未等待资源 %d" % (j, r)
            assert r not in holder, "资源 %d 非空闲却被授予" % r
            waiters = [w for w in alive if waits[w] == r]
            assert j == min(waiters), "资源 %d 未授予最小 ID 等待者" % r
            holder[r] = j
            holds[j].add(r)
            waits[j] = None
        elif t == "complete":
            assert waits[j] is None, "作业 %d 仍在等待却被完成" % j
            assert sorted(holds[j]) == ev["released"], "完成时释放列表不符"
            for r in holds[j]:
                holder.pop(r, None)
            holds[j] = set()
            alive.discard(j)
        elif t == "abort":
            assert abortable[j], "受保护作业 %d 被强行中止" % j
            assert sorted(holds[j]) == ev["released"], "中止时释放列表不符"
            for r in holds[j]:
                holder.pop(r, None)
            holds[j] = set()
            waits[j] = None
            alive.discard(j)
        else:
            raise AssertionError("未知事件类型: %r" % (ev,))
    return alive, waits, holder


def check_resolved_result(state, result):
    """核对 resolved 结果: 回放合法、死锁解除、中止集最小."""
    jobs = {j["id"]: j for j in state["jobs"]}
    assert result["status"] == "resolved"
    assert result["stuck"] == []

    # 回放合法, 终态: 无存活作业、无资源被占用
    alive, waits, holder = fold_events(state, result)
    assert alive == set(), "resolved 后仍有作业未结束: %r" % alive
    assert holder == {}, "resolved 后仍有资源被持有: %r" % holder

    # completed / aborted 互补且与事件一致
    aborted = result["aborted"]
    completed = result["completed"]
    assert aborted == sorted(aborted)
    assert set(aborted) | set(completed) == set(jobs)
    assert not (set(aborted) & set(completed))
    assert {e["job"] for e in result["events"] if e["type"] == "abort"} \
        == set(aborted)
    assert {e["job"] for e in result["events"] if e["type"] == "complete"} \
        == set(completed)

    # 中止的都是可中止作业, 代价核算正确
    for jid in aborted:
        assert jobs[jid].get("abortable", True), "受保护作业被中止"
    best_cost = sum(jobs[j]["abort_cost"] for j in aborted)
    assert result["abort_cost"] == best_cost

    # 独立重放: 返回的中止集合确实解除死锁
    done, stuck = independent_replay(state["jobs"], aborted)
    assert stuck == set(), "中止 %r 后仍有作业卡住: %r" % (aborted, stuck)

    # 枚举全部可中止作业子集: 任何严格更优 (代价更小, 或代价相同
    # 但排序后 ID 序列更小) 的候选都不可行 —— 核对最小性.
    abortable_ids = sorted(j for j, spec in jobs.items()
                           if spec.get("abortable", True))
    best_key = (best_cost, aborted)
    for r in range(len(abortable_ids) + 1):
        for subset in itertools.combinations(abortable_ids, r):
            cost = sum(jobs[j]["abort_cost"] for j in subset)
            key = (cost, list(subset))
            if key >= best_key:
                continue
            _, stuck = independent_replay(state["jobs"], subset)
            assert stuck != set(), (
                "存在更优可行中止集 %r (代价 %d), 实现却返回 %r (代价 %d)"
                % (list(subset), cost, aborted, best_cost))


def check_infeasible_result(state, result):
    """核对 infeasible 结果: 明确报告, 且不强行释放受保护作业的资源."""
    jobs = {j["id"]: j for j in state["jobs"]}
    assert result["status"] == "infeasible"
    assert result["message"], "infeasible 必须给出原因"
    assert result["aborted"] == []
    assert result["abort_cost"] == 0
    assert result["stuck"], "infeasible 必须列出卡住作业"

    # 不强行释放资源: 回放中没有任何 abort, 也没有对卡住作业释放资源
    assert all(e["type"] != "abort" for e in result["events"])
    stuck_ids = {d["id"] for d in result["stuck"]}
    acted = {e["job"] for e in result["events"]}
    assert not (stuck_ids & acted), "卡住作业的资源被强行释放"

    # 回放合法; 终态存活作业恰为 stuck
    alive, waits, holder = fold_events(state, result)
    assert alive == stuck_ids

    # 独立验证: 枚举可中止作业的全部子集, 无一能解除死锁
    abortable_ids = sorted(j for j, spec in jobs.items()
                           if spec.get("abortable", True))
    for r in range(len(abortable_ids) + 1):
        for subset in itertools.combinations(abortable_ids, r):
            _, stuck = independent_replay(state["jobs"], subset)
            assert stuck != set(), "子集 %r 实际可行, 实现却报告无解" % (subset,)

    # 无解必因受保护作业: stuck 中至少一个不可中止
    assert any(not jobs[j].get("abortable", True) for j in stuck_ids)


def check_result(state, result):
    if result["status"] == "resolved":
        check_resolved_result(state, result)
    else:
        check_infeasible_result(state, result)


def job(jid, holds=(), waits=None, abortable=True, cost=1):
    return {"id": jid, "holds": list(holds), "waits_for": waits,
            "abortable": abortable,
            "abort_cost": cost if abortable else None}


# ---------------------------------------------------------------------------
# 场景测试
# ---------------------------------------------------------------------------

class ScenarioTests(unittest.TestCase):

    def test_chain_completes_without_abort(self):
        state = {"resources": [1, 2], "jobs": [
            job(1, holds=[], waits=2, cost=5),
            job(2, holds=[2], waits=None, cost=7),
        ]}
        result = resolve(state)
        self.assertEqual(result["status"], "resolved")
        self.assertEqual(result["events"], [
            {"type": "complete", "job": 2, "released": [2]},
            {"type": "grant", "job": 1, "resource": 2},
            {"type": "complete", "job": 1, "released": [2]},
        ])
        self.assertEqual(result["aborted"], [])
        self.assertEqual(result["abort_cost"], 0)
        self.assertEqual(result["completed"], [1, 2])
        check_result(state, result)

    def test_grant_prefers_smallest_job_id(self):
        state = {"resources": [10], "jobs": [
            job(1, waits=10, cost=1),
            job(2, waits=10, cost=1),
            job(3, holds=[10], cost=1),
        ]}
        result = resolve(state)
        grants = [e for e in result["events"] if e["type"] == "grant"]
        self.assertEqual([g["job"] for g in grants], [1, 2])
        self.assertEqual(result["completed"], [1, 2, 3])
        check_result(state, result)

    def test_idle_jobs_complete_in_id_order(self):
        state = {"resources": [1], "jobs": [job(3), job(1), job(2)]}
        result = resolve(state)
        self.assertEqual(
            [e["job"] for e in result["events"] if e["type"] == "complete"],
            [1, 2, 3])
        check_result(state, result)

    def test_wait_for_free_resource_granted_immediately(self):
        state = {"resources": [7], "jobs": [job(1, waits=7, cost=3)]}
        result = resolve(state)
        self.assertEqual(result["events"], [
            {"type": "grant", "job": 1, "resource": 7},
            {"type": "complete", "job": 1, "released": [7]},
        ])
        check_result(state, result)

    def test_completion_releases_all_held_resources(self):
        state = {"resources": [1, 2, 3], "jobs": [
            job(1, holds=[1, 2, 3], cost=4),
            job(2, waits=2, cost=4),
        ]}
        result = resolve(state)
        self.assertEqual(result["events"][0],
                         {"type": "complete", "job": 1, "released": [1, 2, 3]})
        self.assertEqual(result["completed"], [1, 2])
        check_result(state, result)

    def test_deadlock_aborts_cheapest(self):
        state = {"resources": [101, 102], "jobs": [
            job(1, holds=[101], waits=102, cost=5),
            job(2, holds=[102], waits=101, cost=3),
        ]}
        result = resolve(state)
        self.assertEqual(result["events"], [
            {"type": "abort", "job": 2, "released": [102]},
            {"type": "grant", "job": 1, "resource": 102},
            {"type": "complete", "job": 1, "released": [101, 102]},
        ])
        self.assertEqual(result["aborted"], [2])
        self.assertEqual(result["abort_cost"], 3)
        self.assertEqual(result["completed"], [1])
        check_result(state, result)

    def test_tie_break_picks_lexicographically_smallest(self):
        # 三元环, 中止 2 或 3 代价同为 2, 取排序后 ID 序列最小者 [2]
        state = {"resources": [1, 2, 3], "jobs": [
            job(1, holds=[1], waits=2, cost=4),
            job(2, holds=[2], waits=3, cost=2),
            job(3, holds=[3], waits=1, cost=2),
        ]}
        result = resolve(state)
        self.assertEqual(result["aborted"], [2])
        self.assertEqual(result["abort_cost"], 2)
        self.assertEqual(result["completed"], [1, 3])
        check_result(state, result)

    def test_two_independent_cycles_with_downstream_waiter(self):
        # 环 A: 1<->2 (中止 1 代价 2 最便宜); 环 B: 3<->4 (中止 4 代价 1);
        # 作业 5 等待环 A 中作业 2 持有的资源, 环解开后随之完成.
        state = {"resources": [11, 12, 13, 14], "jobs": [
            job(1, holds=[11], waits=12, cost=2),
            job(2, holds=[12], waits=11, cost=5),
            job(3, holds=[13], waits=14, cost=7),
            job(4, holds=[14], waits=13, cost=1),
            job(5, holds=[], waits=12, cost=9),
        ]}
        result = resolve(state)
        self.assertEqual(result["aborted"], [1, 4])
        self.assertEqual(result["abort_cost"], 3)
        self.assertEqual(result["completed"], [2, 3, 5])
        check_result(state, result)

    def test_partial_progress_before_deadlock(self):
        # 作业 3 先完成, 之后 1<->2 死锁才暴露
        state = {"resources": [1, 2, 3], "jobs": [
            job(1, holds=[1], waits=2, cost=8),
            job(2, holds=[2], waits=1, cost=6),
            job(3, holds=[3], waits=None, cost=1),
        ]}
        result = resolve(state)
        self.assertEqual(result["events"][0],
                         {"type": "complete", "job": 3, "released": [3]})
        self.assertEqual(result["aborted"], [2])
        self.assertEqual(result["abort_cost"], 6)
        check_result(state, result)

    def test_protected_cycle_reported_infeasible(self):
        state = {"resources": [1, 2], "jobs": [
            job(1, holds=[1], waits=2, abortable=False),
            job(2, holds=[2], waits=1, abortable=False),
        ]}
        result = resolve(state)
        self.assertEqual(result["status"], "infeasible")
        self.assertEqual(result["events"], [])
        self.assertEqual(result["aborted"], [])
        self.assertIn("受保护", result["message"])
        self.assertEqual({d["id"] for d in result["stuck"]}, {1, 2})
        self.assertTrue(all(not d["abortable"] for d in result["stuck"]))
        check_result(state, result)

    def test_mixed_cycle_aborts_only_abortable(self):
        # 受保护作业可以被保留, 只要中止对方即可解开
        state = {"resources": [1, 2], "jobs": [
            job(1, holds=[1], waits=2, abortable=False),
            job(2, holds=[2], waits=1, cost=100),
        ]}
        result = resolve(state)
        self.assertEqual(result["status"], "resolved")
        self.assertEqual(result["aborted"], [2])
        self.assertEqual(result["abort_cost"], 100)
        self.assertEqual(result["completed"], [1])
        check_result(state, result)

    def test_cheapest_cycle_member_protected(self):
        # 环上最便宜者受保护, 只能选次便宜的可中止作业
        state = {"resources": [1, 2, 3], "jobs": [
            job(1, holds=[1], waits=2, abortable=False),
            job(2, holds=[2], waits=3, cost=5),
            job(3, holds=[3], waits=1, cost=9),
        ]}
        result = resolve(state)
        self.assertEqual(result["aborted"], [2])
        self.assertEqual(result["abort_cost"], 5)
        check_result(state, result)

    def test_protected_waiter_rescued_by_abort(self):
        # 受保护作业等待环内资源: 中止环上一员即可救活它;
        # 中止 1 或 2 代价相同, 取 ID 序列最小者 [1]
        state = {"resources": [1, 2], "jobs": [
            job(1, holds=[1], waits=2, cost=1),
            job(2, holds=[2], waits=1, cost=1),
            job(3, holds=[], waits=1, abortable=False),
        ]}
        result = resolve(state)
        self.assertEqual(result["status"], "resolved")
        self.assertEqual(result["aborted"], [1])
        self.assertEqual(result["completed"], [2, 3])
        check_result(state, result)

    def test_protected_cycle_blocks_everything(self):
        # 环 1<->2 本可中止解开, 但 3<->4 全受保护 -> 整体无解,
        # 且不得强行中止任何作业 (可解的环也保持原样等待)
        state = {"resources": [1, 2, 3, 4], "jobs": [
            job(1, holds=[1], waits=2, cost=1),
            job(2, holds=[2], waits=1, cost=1),
            job(3, holds=[3], waits=4, abortable=False),
            job(4, holds=[4], waits=3, abortable=False),
        ]}
        result = resolve(state)
        self.assertEqual(result["status"], "infeasible")
        self.assertEqual(result["events"], [])
        self.assertEqual({d["id"] for d in result["stuck"]}, {1, 2, 3, 4})
        check_result(state, result)

    def test_empty_state(self):
        result = resolve({"resources": [], "jobs": []})
        self.assertEqual(result["status"], "resolved")
        self.assertEqual(result["events"], [])

    def test_state_not_mutated_and_deterministic(self):
        state = {"resources": [1, 2, 3], "jobs": [
            job(1, holds=[1], waits=2, cost=4),
            job(2, holds=[2], waits=3, cost=2),
            job(3, holds=[3], waits=1, cost=2),
        ]}
        snapshot = copy.deepcopy(state)
        first = resolve(state)
        second = resolve(state)
        self.assertEqual(state, snapshot)
        self.assertEqual(first, second)


# ---------------------------------------------------------------------------
# 输入校验测试
# ---------------------------------------------------------------------------

class ValidationTests(unittest.TestCase):

    def assert_invalid(self, state):
        with self.assertRaises(InvalidState):
            resolve(state)

    def test_too_many_jobs(self):
        self.assert_invalid({
            "resources": [],
            "jobs": [job(i) for i in range(1, 14)],
        })

    def test_too_many_resources(self):
        self.assert_invalid({"resources": list(range(1, 17)), "jobs": []})

    def test_twelve_jobs_fifteen_resources_accepted(self):
        state = {"resources": list(range(1, 16)),
                 "jobs": [job(i, holds=[i]) for i in range(1, 13)]}
        self.assertEqual(resolve(state)["status"], "resolved")

    def test_duplicate_job_id(self):
        self.assert_invalid({"resources": [], "jobs": [job(1), job(1)]})

    def test_duplicate_resource_id(self):
        self.assert_invalid({"resources": [1, 1], "jobs": []})

    def test_resource_held_by_two_jobs(self):
        self.assert_invalid({"resources": [1], "jobs": [
            job(1, holds=[1]), job(2, holds=[1])]})

    def test_wait_for_unknown_resource(self):
        self.assert_invalid({"resources": [1], "jobs": [job(1, waits=99)]})

    def test_hold_unknown_resource(self):
        self.assert_invalid({"resources": [1], "jobs": [job(1, holds=[2])]})

    def test_wait_for_own_resource(self):
        self.assert_invalid({"resources": [1], "jobs": [
            job(1, holds=[1], waits=1)]})

    def test_abort_cost_must_be_positive_int(self):
        self.assert_invalid({"resources": [], "jobs": [job(1, cost=0)]})
        self.assert_invalid({"resources": [], "jobs": [job(1, cost=-3)]})
        bad = job(1)
        del bad["abort_cost"]
        self.assert_invalid({"resources": [], "jobs": [bad]})
        bad = job(1)
        bad["abort_cost"] = 1.5
        self.assert_invalid({"resources": [], "jobs": [bad]})

    def test_abortable_must_be_bool(self):
        bad = job(1)
        bad["abortable"] = "yes"
        self.assert_invalid({"resources": [], "jobs": [bad]})

    def test_shape_errors(self):
        self.assert_invalid("not a dict")
        self.assert_invalid({"jobs": []})
        self.assert_invalid({"resources": []})
        self.assert_invalid({"resources": [], "jobs": ["nope"]})


# ---------------------------------------------------------------------------
# 随机性质测试: 独立重放核对死锁解除与最小性
# ---------------------------------------------------------------------------

def random_state(rng):
    n_jobs = rng.randint(1, 8)
    n_res = rng.randint(1, 10)
    res_ids = rng.sample(range(1, 100), n_res)
    job_ids = sorted(rng.sample(range(1, 100), n_jobs))
    holds = {j: [] for j in job_ids}
    for r in res_ids:
        if rng.random() < 0.7:
            holds[rng.choice(job_ids)].append(r)
    jobs = []
    for j in job_ids:
        candidates = [r for r in res_ids if r not in holds[j]]
        waits = None
        if candidates and rng.random() < 0.6:
            waits = rng.choice(candidates)
        abortable = rng.random() < 0.7
        jobs.append({"id": j, "holds": holds[j], "waits_for": waits,
                     "abortable": abortable,
                     "abort_cost": rng.randint(1, 9) if abortable else None})
    return {"resources": res_ids, "jobs": jobs}


class PropertyTests(unittest.TestCase):

    def test_random_states(self):
        rng = random.Random(20260930)
        resolved = infeasible = 0
        for _ in range(300):
            state = random_state(rng)
            snapshot = copy.deepcopy(state)
            result = resolve(state)
            self.assertEqual(state, snapshot, "输入状态被修改")
            check_result(state, result)
            if result["status"] == "resolved":
                resolved += 1
            else:
                infeasible += 1
        # 两种结局都应被覆盖到
        self.assertGreater(resolved, 0)
        self.assertGreater(infeasible, 0)


# ---------------------------------------------------------------------------
# HTTP 后端冒烟测试
# ---------------------------------------------------------------------------

class ServerTests(unittest.TestCase):

    def test_resolve_endpoint(self):
        from server import make_server
        server = make_server("127.0.0.1", 0)
        port = server.server_address[1]
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            body = json.dumps({"resources": [1, 2], "jobs": [
                job(1, holds=[1], waits=2, cost=5),
                job(2, holds=[2], waits=1, cost=3),
            ]})
            conn = http.client.HTTPConnection("127.0.0.1", port)
            conn.request("POST", "/resolve", body=body,
                         headers={"Content-Type": "application/json"})
            resp = conn.getresponse()
            payload = json.loads(resp.read())
            self.assertEqual(resp.status, 200)
            self.assertEqual(payload["status"], "resolved")
            self.assertEqual(payload["aborted"], [2])

            conn.request("POST", "/resolve", body='{"jobs": 42}')
            resp = conn.getresponse()
            self.assertEqual(resp.status, 400)
            conn.close()
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)


if __name__ == "__main__":
    unittest.main()
