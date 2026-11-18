'use strict';

const { EventEmitter } = require('node:events');
const { TraceRecord } = require('./trace-record');

const VALID_STATUS = new Set(['ok', 'error', 'unset']);

class BatchValidationError extends Error {
  constructor(errors) {
    super(
      `batch rejected: ${errors.length} invalid span(s); first: ` +
        `[${errors[0].index}] ${errors[0].message}`
    );
    this.name = 'BatchValidationError';
    this.errors = errors;
  }
}

class TraceTooLargeError extends Error {
  constructor(traceId, size, max) {
    super(
      `batch rejected: trace ${traceId} would grow to ${size} spans, ` +
        `exceeding maxSpansPerTrace=${max}`
    );
    this.name = 'TraceTooLargeError';
    this.traceId = traceId;
  }
}

/**
 * 校验并规范化单个 span。校验失败抛错（由批次收集后统一拒绝）。
 * 时钟漂移：endTime 略小于 startTime（在 maxClockSkewMs 内）时钳位并打标记。
 */
function normalizeSpan(raw, maxClockSkewMs) {
  if (raw == null || typeof raw !== 'object') {
    throw new Error('span must be an object');
  }
  const { traceId, spanId, parentSpanId, startTime, endTime } = raw;
  const status = raw.status == null ? 'unset' : raw.status;

  if (typeof traceId !== 'string' || traceId.length === 0) {
    throw new Error('traceId must be a non-empty string');
  }
  if (typeof spanId !== 'string' || spanId.length === 0) {
    throw new Error('spanId must be a non-empty string');
  }
  if (parentSpanId != null && typeof parentSpanId !== 'string') {
    throw new Error('parentSpanId must be a string or null');
  }
  if (parentSpanId === spanId) {
    throw new Error('span cannot be its own parent');
  }
  if (!Number.isFinite(startTime) || startTime < 0) {
    throw new Error('startTime must be a finite non-negative number');
  }
  if (endTime != null && !Number.isFinite(endTime)) {
    throw new Error('endTime must be a finite number or null');
  }
  if (!VALID_STATUS.has(status)) {
    throw new Error(`status must be one of: ${[...VALID_STATUS].join(', ')}`);
  }

  const flags = { clockSkewClamped: false };
  let finalEnd = endTime == null ? null : endTime;
  if (finalEnd != null && finalEnd < startTime) {
    if (startTime - finalEnd > maxClockSkewMs) {
      throw new Error(
        `endTime ${finalEnd} is ${startTime - finalEnd}ms before startTime ` +
          `${startTime}, beyond maxClockSkewMs=${maxClockSkewMs}`
      );
    }
    finalEnd = startTime; // 漂移在容忍范围内：钳位，时长记 0
    flags.clockSkewClamped = true;
  }

  return {
    traceId,
    spanId,
    parentSpanId: parentSpanId == null ? null : parentSpanId,
    startTime,
    endTime: finalEnd,
    status,
    flags,
  };
}

/**
 * Trace 接入器。
 *
 * - submitBatch 原子提交：任一 span 校验失败或触发大小限制，整批拒绝，
 *   索引（已提交存储）保持不变。
 * - 乱序 span 先落缓冲：父节点未到的 span 记为 orphan，绝不提升层级。
 * - 超过 waitWindowMs 未完整的 trace 由 flushExpired() 输出为 partial。
 * - complete / partial 的 trace 在 retentionMs 后被 purgeExpired() 清理。
 *
 * 事件：
 *   'complete' (traceView) —— trace 完整闭合
 *   'partial'  (traceView) —— 等待窗口到期，输出为 partial
 */
class TraceIngestor extends EventEmitter {
  constructor({
    waitWindowMs = 30_000,
    retentionMs = 3_600_000,
    maxSpansPerTrace = 10_000,
    maxClockSkewMs = 5_000,
    now = Date.now,
  } = {}) {
    super();
    this.waitWindowMs = waitWindowMs;
    this.retentionMs = retentionMs;
    this.maxSpansPerTrace = maxSpansPerTrace;
    this.maxClockSkewMs = maxClockSkewMs;
    this._now = now;
    /** @type {Map<string, TraceRecord>} 已提交 span 的唯一存储（即索引） */
    this._traces = new Map();
    /** @type {Map<string, number>} batchId -> 提交时间，用于失败批次幂等重试 */
    this._batchIds = new Map();
  }

  /**
   * 原子提交一个批次。
   * @param {Array} rawSpans
   * @param {{ batchId?: string }} [opts] 提供 batchId 时，重复提交同一批次为幂等 no-op
   * @returns {{ committed: true, duplicate: boolean, accepted: number, duplicates: number }}
   * @throws {BatchValidationError | TraceTooLargeError} 整批拒绝，索引不变
   */
  submitBatch(rawSpans, opts = {}) {
    if (!Array.isArray(rawSpans) || rawSpans.length === 0) {
      throw new TypeError('submitBatch requires a non-empty array of spans');
    }
    const { batchId = null } = opts;
    if (batchId != null && this._batchIds.has(batchId)) {
      return { committed: true, duplicate: true, accepted: 0, duplicates: 0 };
    }

    // 1) 全量校验（失败则整批拒绝，此时未触碰任何状态）
    const normalized = [];
    const errors = [];
    rawSpans.forEach((raw, index) => {
      try {
        normalized.push(normalizeSpan(raw, this.maxClockSkewMs));
      } catch (err) {
        errors.push({ index, message: err.message });
      }
    });
    if (errors.length > 0) throw new BatchValidationError(errors);

    // 2) 批内去重（先到先赢）
    const seen = new Set();
    const unique = [];
    let duplicates = 0;
    for (const span of normalized) {
      const key = `${span.traceId} ${span.spanId}`;
      if (seen.has(key)) {
        duplicates += 1;
        continue;
      }
      seen.add(key);
      unique.push(span);
    }

    // 3) 大小限制预检（在任何变更之前）
    const newPerTrace = new Map();
    for (const span of unique) {
      const rec = this._traces.get(span.traceId);
      if (rec && rec.spans.has(span.spanId)) continue; // 已存在，不占新额度
      newPerTrace.set(span.traceId, (newPerTrace.get(span.traceId) || 0) + 1);
    }
    for (const [traceId, added] of newPerTrace) {
      const rec = this._traces.get(traceId);
      const total = (rec ? rec.spans.size : 0) + added;
      if (total > this.maxSpansPerTrace) {
        throw new TraceTooLargeError(traceId, total, this.maxSpansPerTrace);
      }
    }

    // 4) 应用：单次同步变更，对并发查询而言是原子的
    const now = this._now();
    let accepted = 0;
    const touched = new Set();
    for (const span of unique) {
      let rec = this._traces.get(span.traceId);
      if (!rec) {
        rec = new TraceRecord(span.traceId, now);
        this._traces.set(span.traceId, rec);
      }
      const result = rec.addSpan(span, now);
      if (result === 'duplicate') duplicates += 1;
      else accepted += 1;
      touched.add(rec);
    }
    if (batchId != null) this._batchIds.set(batchId, now);

    // 5) 完整性评估
    for (const rec of touched) this._reevaluate(rec, now);

    return { committed: true, duplicate: false, accepted, duplicates };
  }

  _reevaluate(rec, now) {
    if (rec.state === 'buffering' && rec.isComplete()) {
      rec.state = 'complete';
      rec.closedAt = now;
      this.emit('complete', rec.toView());
    } else if (rec.state === 'complete' && !rec.isComplete()) {
      // 完整 trace 又收到新 span：回到缓冲状态重新等待
      rec.state = 'buffering';
      rec.closedAt = null;
    }
  }

  /**
   * 查询 trace 视图；只反映已提交批次。未找到返回 null。
   */
  getTrace(traceId) {
    const rec = this._traces.get(traceId);
    return rec ? rec.toView() : null;
  }

  /**
   * 把超过等待窗口仍未完整的 trace 输出为 partial。
   * @returns {Array} 本次输出的 partial trace 视图
   */
  flushExpired() {
    const now = this._now();
    const flushed = [];
    for (const rec of this._traces.values()) {
      if (rec.state !== 'buffering') continue;
      if (now - rec.lastActivityAt >= this.waitWindowMs) {
        rec.state = 'partial';
        rec.closedAt = now;
        const view = rec.toView();
        this.emit('partial', view);
        flushed.push(view);
      }
    }
    return flushed;
  }

  /**
   * 清理超过保留期的 complete / partial trace 及过期 batchId。
   * @returns {{ purgedTraces: string[], purgedBatches: number }}
   */
  purgeExpired() {
    const now = this._now();
    const purgedTraces = [];
    for (const [traceId, rec] of this._traces) {
      if (rec.state === 'buffering') continue;
      if (now - rec.closedAt >= this.retentionMs) {
        this._traces.delete(traceId);
        purgedTraces.push(traceId);
      }
    }
    let purgedBatches = 0;
    for (const [batchId, committedAt] of this._batchIds) {
      if (now - committedAt >= this.retentionMs) {
        this._batchIds.delete(batchId);
        purgedBatches += 1;
      }
    }
    return { purgedTraces, purgedBatches };
  }

  stats() {
    let buffering = 0;
    let complete = 0;
    let partial = 0;
    for (const rec of this._traces.values()) {
      if (rec.state === 'buffering') buffering += 1;
      else if (rec.state === 'complete') complete += 1;
      else partial += 1;
    }
    return {
      traces: this._traces.size,
      buffering,
      complete,
      partial,
      trackedBatches: this._batchIds.size,
    };
  }
}

module.exports = {
  TraceIngestor,
  BatchValidationError,
  TraceTooLargeError,
};
