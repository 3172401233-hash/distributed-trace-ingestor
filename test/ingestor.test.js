'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const {
  TraceIngestor,
  BatchValidationError,
  TraceTooLargeError,
} = require('../src/ingestor');

/** 可手动推进的时钟，保证窗口/保留期测试确定性 */
function makeClock(start = 1_000_000) {
  let t = start;
  return {
    now: () => t,
    advance(ms) {
      t += ms;
    },
  };
}

function span(overrides = {}) {
  return {
    traceId: 't1',
    spanId: 's1',
    parentSpanId: null,
    startTime: 100,
    endTime: 200,
    status: 'ok',
    ...overrides,
  };
}

function makeIngestor(overrides = {}) {
  const clock = makeClock();
  const ing = new TraceIngestor({
    waitWindowMs: 1_000,
    retentionMs: 5_000,
    maxSpansPerTrace: 100,
    maxClockSkewMs: 50,
    now: clock.now,
    ...overrides,
  });
  return { ing, clock };
}

test('跨批次父子：父节点未到时子节点不被提升为 root，父节点到达后正确挂载', () => {
  const { ing } = makeIngestor();

  // 批次 1：只有孙子 + 子节点，父（根）还没到
  ing.submitBatch([
    span({ spanId: 'child', parentSpanId: 'root', startTime: 110, endTime: 190 }),
    span({ spanId: 'grandchild', parentSpanId: 'child', startTime: 120, endTime: 130 }),
  ]);

  let view = ing.getTrace('t1');
  assert.equal(view.state, 'buffering');
  assert.equal(view.roots.length, 0, '父节点缺失时不得把任何 span 提升为 root');
  assert.equal(view.orphans.length, 1, '只有 child 是孤儿，grandchild 挂在 child 下');
  assert.equal(view.orphans[0].spanId, 'child');
  assert.equal(view.orphans[0].children[0].spanId, 'grandchild');
  assert.deepEqual(view.missingParents, ['root']);

  // 批次 2：根 span 最后到达
  ing.submitBatch([
    span({ spanId: 'root', parentSpanId: null, startTime: 100, endTime: 200 }),
  ]);

  view = ing.getTrace('t1');
  assert.equal(view.state, 'complete');
  assert.equal(view.roots.length, 1);
  assert.equal(view.roots[0].spanId, 'root');
  assert.equal(view.orphans.length, 0);
  const child = view.roots[0].children[0];
  assert.equal(child.spanId, 'child');
  assert.equal(child.children[0].spanId, 'grandchild');
});

test('重复 span：失败批次重试不产生双份节点', () => {
  const { ing } = makeIngestor();

  ing.submitBatch([span({ spanId: 'a' }), span({ spanId: 'b', parentSpanId: 'a' })]);
  // 调用方未收到确认，原样重试同一批
  const res = ing.submitBatch([
    span({ spanId: 'a' }),
    span({ spanId: 'b', parentSpanId: 'a' }),
  ]);

  assert.equal(res.accepted, 0);
  assert.equal(res.duplicates, 2);
  const view = ing.getTrace('t1');
  assert.equal(view.spanCount, 2, '重试不得生成双份节点');
  assert.equal(view.duplicateCount, 2);
});

test('重复 span：相同 batchId 的批次幂等 no-op', () => {
  const { ing } = makeIngestor();

  ing.submitBatch([span({ spanId: 'a' })], { batchId: 'batch-1' });
  const res = ing.submitBatch([span({ spanId: 'a' })], { batchId: 'batch-1' });

  assert.equal(res.duplicate, true);
  assert.equal(ing.getTrace('t1').spanCount, 1);
});

test('重复 span：批内重复只保留一份', () => {
  const { ing } = makeIngestor();
  const res = ing.submitBatch([span({ spanId: 'a' }), span({ spanId: 'a' })]);
  assert.equal(res.accepted, 1);
  assert.equal(res.duplicates, 1);
  assert.equal(ing.getTrace('t1').spanCount, 1);
});

test('in-flight span 后补 endTime：合并而非新建节点', () => {
  const { ing } = makeIngestor();

  ing.submitBatch([span({ spanId: 'a', endTime: null })]); // 尚未结束
  let view = ing.getTrace('t1');
  assert.equal(view.state, 'buffering');
  assert.equal(view.endTime, null);

  ing.submitBatch([span({ spanId: 'a', endTime: 250 })]); // 结束事件后补
  view = ing.getTrace('t1');
  assert.equal(view.spanCount, 1);
  assert.equal(view.state, 'complete');
  assert.equal(view.roots[0].endTime, 250);
  assert.equal(view.roots[0].durationMs, 150);
});

test('失败批次：任一 span 非法则整批拒绝，索引保持不变', () => {
  const { ing } = makeIngestor();

  ing.submitBatch([span({ spanId: 'good' })]);
  const before = ing.getTrace('t1');

  assert.throws(
    () =>
      ing.submitBatch([
        span({ spanId: 'new-good' }), // 这条本身合法
        span({ spanId: '' }), // 非法：空 spanId
      ]),
    BatchValidationError
  );

  const after = ing.getTrace('t1');
  assert.deepEqual(after, before, '失败批次不得部分落库');
  assert.equal(after.spanCount, 1);
  assert.equal(ing.getTrace('t1').roots[0].spanId, 'good');
});

test('索引可见性：查询只能看到已提交批次', () => {
  const { ing } = makeIngestor();

  assert.equal(ing.getTrace('t1'), null, '提交前不可见');
  ing.submitBatch([span({ spanId: 'a' })]);
  assert.equal(ing.getTrace('t1').spanCount, 1);
  // 失败批次之后可见性不变
  assert.throws(() => ing.submitBatch([span({ traceId: '' })]), BatchValidationError);
  assert.equal(ing.getTrace('t1').spanCount, 1);
});

test('超大 trace：超过 maxSpansPerTrace 的批次被原子拒绝', () => {
  const { ing } = makeIngestor({ maxSpansPerTrace: 3 });

  ing.submitBatch([span({ spanId: 'a' }), span({ spanId: 'b', parentSpanId: 'a' })]);

  assert.throws(
    () =>
      ing.submitBatch([
        span({ spanId: 'c', parentSpanId: 'b' }),
        span({ spanId: 'd', parentSpanId: 'b' }), // 2 + 2 = 4 > 3
      ]),
    TraceTooLargeError
  );
  assert.equal(ing.getTrace('t1').spanCount, 2, '超限批次不得部分应用');

  // 恰好在限额内则允许
  ing.submitBatch([span({ spanId: 'c', parentSpanId: 'b' })]);
  assert.equal(ing.getTrace('t1').spanCount, 3);
});

test('时钟漂移：容忍范围内钳位并打标记，超出容忍整批拒绝', () => {
  const { ing } = makeIngestor({ maxClockSkewMs: 50 });

  // endTime 比 startTime 早 30ms（<= 50ms 容忍）：钳位
  ing.submitBatch([span({ spanId: 'skewed', startTime: 100, endTime: 70 })]);
  const node = ing.getTrace('t1').roots[0];
  assert.equal(node.endTime, 100);
  assert.equal(node.durationMs, 0);
  assert.equal(node.flags.clockSkewClamped, true);

  // 漂移 5000ms，远超容忍：整批拒绝
  assert.throws(
    () => ing.submitBatch([span({ spanId: 'bad', startTime: 10_000, endTime: 5_000 })]),
    BatchValidationError
  );
  assert.equal(ing.getTrace('t1').spanCount, 1);
});

test('partial trace：根 span 始终未到，超过等待窗口后输出 partial', () => {
  const { ing, clock } = makeIngestor({ waitWindowMs: 1_000 });
  const partials = [];
  ing.on('partial', (view) => partials.push(view));

  ing.submitBatch([
    span({ spanId: 'child', parentSpanId: 'never-arrives', startTime: 100, endTime: 200 }),
  ]);
  assert.equal(ing.flushExpired().length, 0, '窗口未到期不得输出');

  clock.advance(1_001);
  const flushed = ing.flushExpired();
  assert.equal(flushed.length, 1);
  assert.equal(partials.length, 1, '应触发 partial 事件');

  const view = flushed[0];
  assert.equal(view.state, 'partial');
  assert.equal(view.roots.length, 0, 'partial 中孤儿仍不得提升为 root');
  assert.equal(view.orphans.length, 1);
  assert.deepEqual(view.missingParents, ['never-arrives']);

  // 已输出的 partial 不会重复输出
  clock.advance(10_000);
  assert.equal(ing.flushExpired().length, 0);
});

test('partial trace：窗口期内到齐则不输出 partial', () => {
  const { ing, clock } = makeIngestor({ waitWindowMs: 1_000 });
  const partials = [];
  ing.on('partial', (view) => partials.push(view));

  ing.submitBatch([span({ spanId: 'child', parentSpanId: 'root' })]);
  clock.advance(500);
  ing.submitBatch([span({ spanId: 'root' })]); // 窗口内补齐
  clock.advance(10_000);

  assert.equal(ing.flushExpired().length, 0);
  assert.equal(partials.length, 0);
  assert.equal(ing.getTrace('t1').state, 'complete');
});

test('complete 事件：trace 闭合时触发', () => {
  const { ing } = makeIngestor();
  const completed = [];
  ing.on('complete', (view) => completed.push(view));

  ing.submitBatch([span({ spanId: 'a' }), span({ spanId: 'b', parentSpanId: 'a' })]);
  assert.equal(completed.length, 1);
  assert.equal(completed[0].traceId, 't1');
  assert.equal(completed[0].spanCount, 2);
});

test('保留时间：complete/partial trace 超过 retentionMs 后被清理', () => {
  const { ing, clock } = makeIngestor({ waitWindowMs: 1_000, retentionMs: 5_000 });

  ing.submitBatch([span({ spanId: 'a' })]); // 立即 complete
  clock.advance(4_999);
  assert.deepEqual(ing.purgeExpired().purgedTraces, []);
  assert.notEqual(ing.getTrace('t1'), null);

  clock.advance(1);
  const { purgedTraces } = ing.purgeExpired();
  assert.deepEqual(purgedTraces, ['t1']);
  assert.equal(ing.getTrace('t1'), null, '保留期过后不可再查询');
});

test('保留时间：buffering 中的 trace 不受保留期清理影响', () => {
  const { ing, clock } = makeIngestor({ waitWindowMs: 60_000, retentionMs: 5_000 });

  ing.submitBatch([span({ spanId: 'a', endTime: null })]); // 一直 buffering
  clock.advance(10_000);
  assert.deepEqual(ing.purgeExpired().purgedTraces, []);
  assert.notEqual(ing.getTrace('t1'), null);
});

test('非法输入：空批次与非数组直接拒绝', () => {
  const { ing } = makeIngestor();
  assert.throws(() => ing.submitBatch([]), TypeError);
  assert.throws(() => ing.submitBatch(null), TypeError);
});
