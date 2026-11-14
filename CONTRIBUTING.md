# Issue 约定

Issue 用标题前缀和 GitHub Labels 标明涉及的模块，两者保持一致。分类名称区分大小写。

| 分类 | 范围 |
|---|---|
| `Workload` | 负载生成、解释执行、trace 格式、算子表达 |
| `Core` | 核调度、ROB、TileReg、执行单元和 cost model |
| `NoC` | CommUnit、Calendar、路由和网络传输 |
| `Others` | 前三类之外的工程、工具和仓库维护 |

一个 issue 可以同时涉及多个模块。联合分类按 `Workload → Core → NoC` 排列，每类出现一次；`Others` 单独使用。例如：

- `[Core] 修正输出空间分配导致的死锁`，Labels 为 `Core`。
- `[Core][NoC] 核与 CommUnit 的交互协议`，Labels 为 `Core`、`NoC`。
- `[Workload][Core][NoC] 多核通信负载的端到端验证`，添加对应三个 Labels。

`bug`、`enhancement`、`documentation` 等其他 Labels 可以继续使用，标题仅列模块分类。

## 创建与修改

网页创建 issue 时，选择对应单类或联合分类模板，会预填标题前缀并添加对应 Labels。模板覆盖三类模块的全部非空组合及 Others；填写前缀后的具体标题即可。

正文按笔记形式写清楚“背景与目标”“问题或方案”“例子与验证”。前两项必填，例子与验证按需补充；设计讨论可以写尚待确认的问题和验证计划。

通过 CLI、API 或空白入口创建时，主动设置相同的标题前缀和 Labels。例如：

```bash
gh issue create --repo hmljy2020/wse-perf-sim \
  --title '[Core][NoC] 核与 CommUnit 的交互协议' \
  --label Core --label NoC \
  --body-file workspace/issue-body.md
```

修改已有 issue 的分类时，同时调整标题前缀和分类 Labels，保留仍适用的其他 Labels。没有设置标签权限的贡献者可请维护者协助同步 Labels。

## 维护格式

模板位于 `.github/ISSUE_TEMPLATE/`，合并到默认分支后在网页生效。仓库需提前创建 `Workload`、`Core`、`NoC`、`Others` 四个 Labels。模板提供默认值；编辑后的标题和手动创建的 issue 由提交者及维护者按本约定核对。
