# distributed-trace-ingestor
JavaScript 分布式 Trace 接入库，处理乱序 span、延迟完成、父子缺口和批次提交

## 特性

- **批次原子提交**：`submitBatch()` 全量校验后才落库；任一 span 非法或触发大小限制，整批拒绝，索引保持不变。所有索引只反映已提交批次。
- **乱序缓冲**：span 可按任意顺序、跨批次到达。父节点未到的 span 记为 `orphans`，**绝不提升为 root**；父节点到达后自动挂载到正确层级。
- **去重**：失败批次重试（同 `traceId + spanId`）不会产生双份节点；提供 `batchId` 时整批幂等。in-flight span（无 `endTime`）后补结束时间时合并到原节点。
- **时钟漂移容忍**：`endTime` 略早于 `startTime`（在 `maxClockSkewMs` 内）时钳位为 0 时长并打 `clockSkewClamped` 标记；超出容忍则整批拒绝。

## 用法

```js
const { TraceIngestor } = require('./src/ingestor');

const ing = new TraceIngestor({
  waitWindowMs: 30_000,      // 等待窗口：超过后输出 partial trace
  retentionMs: 3_600_000,    // 保留时间：complete/partial trace 的留存时长
  maxSpansPerTrace: 10_000,  // 单条 trace 的 span 上限（防超大 trace）
  maxClockSkewMs: 5_000,     // 时钟漂移容忍
});

ing.on('complete', (view) => console.log('完整 trace:', view.traceId));
ing.on('partial', (view) => console.log('partial trace:', view.traceId, view.missingParents));

ing.submitBatch([
  { traceId: 't1', spanId: 'child', parentSpanId: 'root',
    startTime: 110, endTime: 190, status: 'ok' },
], { batchId: 'batch-1' }); // batchId 可选，用于失败重试幂等

ing.getTrace('t1');   // 查询视图，只含已提交批次
ing.flushExpired();   // 定期调用：输出超时的 partial trace
ing.purgeExpired();   // 定期调用：清理超过保留期的 trace
```

### Span 字段

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `traceId` / `spanId` | string | 必填，非空 |
| `parentSpanId` | string \| null | 根 span 为 `null` |
| `startTime` | number | 必填，毫秒时间戳 |
| `endTime` | number \| null | `null` 表示 in-flight（尚未结束） |
| `status` | `'ok' \| 'error' \| 'unset'` | 缺省 `'unset'` |

## Partial trace

一条 trace 进入 `complete` 需要同时满足：根 span 已到达、所有 span 都有 `endTime`、不存在缺失的父节点。微服务环境下根 span 常常最后才到、甚至永远丢失，因此接入器按**等待窗口**工作：

- 自最近一次收到该 trace 的 span 起，超过 `waitWindowMs` 仍未完整的 trace，由 `flushExpired()` 输出为 **partial trace**（`state: 'partial'`），并触发 `partial` 事件。
- partial 视图中，`orphans` 列出父节点缺失的子树（保持原层级，不提升），`missingParents` 列出缺失的父 span ID，便于下游判断缺口。
- partial 只输出一次；之后迟到的 span 仍会合并进该 trace，但状态保持 `partial`，不会重复触发事件。

## 保留时间（retention）

- `complete` / `partial` 状态的 trace 在闭合后保留 `retentionMs`，之后由 `purgeExpired()` 删除，同时清理过期的 `batchId` 幂等记录。
- 仍在 `buffering` 的 trace 不受保留期影响——它们由等待窗口（`waitWindowMs`）负责推进到 `partial`。
- 两个清理方法都是被动式的（调用时才扫描），建议由接入方用定时器周期触发，例如 `setInterval(() => { ing.flushExpired(); ing.purgeExpired(); }, 10_000)`。

## 测试

```sh
node --test
```

覆盖：跨批次父子挂载、重复 span / 失败批次重试去重、失败批次原子拒绝、超大 trace 限额、时钟漂移钳位、partial trace 输出与保留期清理。
