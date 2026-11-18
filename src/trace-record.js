'use strict';

/**
 * 单条 trace 的聚合状态。
 * 只保存已提交批次中的 span；查询视图在读取时即时组装，
 * 因此索引永远只反映已提交的数据。
 */
class TraceRecord {
  constructor(traceId, now) {
    this.traceId = traceId;
    /** @type {Map<string, object>} spanId -> span */
    this.spans = new Map();
    this.state = 'buffering'; // 'buffering' | 'complete' | 'partial'
    this.firstSeenAt = now;
    this.lastActivityAt = now;
    this.closedAt = null; // 进入 complete / partial 的时间，用于保留期清理
    this.duplicateCount = 0;
  }

  /**
   * @returns {'added'|'merged'|'duplicate'}
   *   added     —— 新 span
   *   merged    —— 已存在的 in-flight span（无 endTime）补上了结束时间
   *   duplicate —— 完全重复上报，忽略
   */
  addSpan(span, now) {
    this.lastActivityAt = now;
    const existing = this.spans.get(span.spanId);
    if (!existing) {
      this.spans.set(span.spanId, span);
      return 'added';
    }
    if (existing.endTime == null && span.endTime != null) {
      existing.endTime = span.endTime;
      existing.status = span.status;
      if (span.flags.clockSkewClamped) existing.flags.clockSkewClamped = true;
      return 'merged';
    }
    this.duplicateCount += 1;
    return 'duplicate';
  }

  hasRoot() {
    for (const span of this.spans.values()) {
      if (span.parentSpanId == null) return true;
    }
    return false;
  }

  /**
   * 完整 = 根 span 已到、所有 span 都有 endTime、且没有缺失的父节点。
   */
  isComplete() {
    if (!this.hasRoot()) return false;
    for (const span of this.spans.values()) {
      if (span.endTime == null) return false;
      if (span.parentSpanId != null && !this.spans.has(span.parentSpanId)) {
        return false;
      }
    }
    return true;
  }

  /**
   * 组装查询视图。父节点未到达的 span 放入 orphans，绝不提升为 root。
   */
  toView() {
    const nodes = new Map();
    for (const span of this.spans.values()) {
      nodes.set(span.spanId, {
        spanId: span.spanId,
        parentSpanId: span.parentSpanId,
        startTime: span.startTime,
        endTime: span.endTime,
        durationMs: span.endTime != null ? span.endTime - span.startTime : null,
        status: span.status,
        flags: { ...span.flags },
        children: [],
      });
    }

    const roots = [];
    const orphans = [];
    const missingParents = new Set();
    for (const node of nodes.values()) {
      if (node.parentSpanId == null) {
        roots.push(node);
        continue;
      }
      const parent = nodes.get(node.parentSpanId);
      if (parent) {
        parent.children.push(node);
      } else {
        orphans.push(node);
        missingParents.add(node.parentSpanId);
      }
    }

    const byStart = (a, b) => a.startTime - b.startTime;
    roots.sort(byStart);
    orphans.sort(byStart);
    for (const node of nodes.values()) node.children.sort(byStart);

    let startTime = Infinity;
    let endTime = -Infinity;
    let allEnded = true;
    for (const span of this.spans.values()) {
      if (span.startTime < startTime) startTime = span.startTime;
      if (span.endTime == null) {
        allEnded = false;
      } else if (span.endTime > endTime) {
        endTime = span.endTime;
      }
    }

    return {
      traceId: this.traceId,
      state: this.state,
      spanCount: this.spans.size,
      duplicateCount: this.duplicateCount,
      startTime: this.spans.size ? startTime : null,
      endTime: allEnded && this.spans.size ? endTime : null,
      roots,
      orphans,
      missingParents: [...missingParents],
    };
  }
}

module.exports = { TraceRecord };
