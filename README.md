# 死锁解除模拟器

后端接收至多 **12 个作业** 与 **15 个单实例资源** 的当前状态，模拟
"空闲资源授予最小 ID 等待者 / 不再等待的作业完成并释放资源" 直至不能前进；
若仍有作业卡住，精确枚举可中止作业子集，选出**总中止代价最小**、且中止后
剩余作业均能完成的集合（代价并列时取**排序后中止 ID 序列最小者**）。
受保护（不可中止）作业导致无解时明确报告 `infeasible`，绝不强行释放其资源。

## 文件

- `resolver.py` — 核心逻辑，入口 `resolve(state: dict) -> dict`
- `server.py` — 标准库 HTTP 包装：`POST /resolve`
- `test_resolver.py` — 测试（`unittest`，无需第三方依赖）

## 输入

```json
{
  "resources": [1, 2],
  "jobs": [
    {"id": 1, "holds": [1], "waits_for": 2, "abortable": true,  "abort_cost": 5},
    {"id": 2, "holds": [2], "waits_for": 1, "abortable": false, "abort_cost": null}
  ]
}
```

约束：每个资源至多一个持有者；每个作业至多等待一个资源；
可中止作业的 `abort_cost` 必须为正整数。非法输入抛出 `InvalidState`
（HTTP 层返回 400）。

## 输出

```json
{
  "status": "resolved",
  "events": [
    {"type": "abort",    "job": 2, "released": [2]},
    {"type": "grant",    "job": 1, "resource": 2},
    {"type": "complete", "job": 1, "released": [1, 2]}
  ],
  "completed": [1],
  "aborted": [2],
  "abort_cost": 5,
  "stuck": []
}
```

`status` 为 `infeasible` 时附带 `message` 与 `stuck`（卡住作业明细），
且回放中不含任何中止事件。

## 运行

```bash
python3 -m unittest test_resolver -v   # 测试
python3 server.py 127.0.0.1 8000       # 启动 HTTP 后端
```

## 测试方法

测试在 `test_resolver.py` 中**独立重实现**模拟规则（`independent_replay`），
对每个用例：

1. 逐步折叠返回的事件流，核对每次授予都发生在资源空闲时、且授予等待者中
   ID 最小者，完成/中止时释放列表与实际持有一致；
2. 用返回的中止集合独立重放，验证死锁确实解除；
3. **枚举全部可中止作业子集**，验证不存在代价更小（或代价相同但排序后
   ID 序列更小）的可行集合 —— 核对最小性与并列打破规则；
4. 对 `infeasible` 结果，枚举全部子集确认无一可行，且回放未强行释放
   受保护作业的资源。

另含 300 轮随机状态性质测试（固定种子），覆盖 resolved / infeasible 两种结局。
