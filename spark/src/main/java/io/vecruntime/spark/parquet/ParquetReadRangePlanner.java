/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.vecruntime.spark.parquet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Plans the byte ranges the native Parquet scan needs out of a file and coalesces them into fewer,
 * larger GETs (#834). Pure arithmetic over {@code [offset, offset + length)} half-open ranges with
 * no I/O, so it is unit-tested on its own ({@code ParquetReadRangePlannerSuite}).
 *
 * <p>The input is the set of ranges the reader will read for a row group: the projected column
 * chunks (from {@code ColumnChunkMetaData}); and, with the column index on (#748), only the
 * selected pages plus the per-column offset/column index reads, rather than each column chunk
 * whole. Each input is a {@link Range}.
 *
 * <p>The planner follows the Analytics Accelerator's two steps, sized from AnyBlob's (PVLDB 2023)
 * measured optimal request sizes rather than the Accelerator's 8 MB defaults (issue #834):
 *
 * <ol>
 *   <li><b>coalesce</b> ({@code IOPlan.coalesce}): ranges sorted by offset whose gap is below
 *       {@code coalesceGap} are merged into one range -- the small bytes in the gap are read too,
 *       which is cheaper than a second request;
 *   <li><b>split</b> ({@code RangeOptimiser.optimizeReads}): a merged run longer than
 *       {@code targetSize} is split into parts of about {@code targetSize}, so a long run is issued
 *       as several parallel requests. A trailing part below {@code targetSize * (toleranceRatio -
 *       1)} is folded into the previous part rather than issued on its own.
 * </ol>
 *
 * <p>The planner never drops or reorders bytes: every input byte lies in exactly one output range,
 * outputs are sorted by offset and do not overlap, and their union covers the union of the inputs
 * (plus the small gaps the coalesce step swallowed). That invariant lets a buffer-backed
 * {@link BufferBackedSeekableInputStream} serve parquet-java every byte it asks for.
 */
public final class ParquetReadRangePlanner {

  /** A half-open byte range {@code [offset, offset + length)} in a file. */
  public static final class Range implements Comparable<Range> {
    public final long offset;
    public final long length;

    public Range(long offset, long length) {
      if (offset < 0) throw new IllegalArgumentException("offset < 0: " + offset);
      if (length < 0) throw new IllegalArgumentException("length < 0: " + length);
      this.offset = offset;
      this.length = length;
    }

    public long end() {
      return offset + length;
    }

    @Override
    public int compareTo(Range o) {
      int c = Long.compare(offset, o.offset);
      return c != 0 ? c : Long.compare(length, o.length);
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Range r)) return false;
      return offset == r.offset && length == r.length;
    }

    @Override
    public int hashCode() {
      return Long.hashCode(offset) * 31 + Long.hashCode(length);
    }

    @Override
    public String toString() {
      return "[" + offset + ", " + end() + ")";
    }
  }

  private final long coalesceGap;
  private final long targetSize;
  private final double toleranceRatio;

  /**
   * @param coalesceGap merge two ranges whose gap (bytes between one's end and the next's start)
   *     next) is at most this; {@code 0} merges only exactly-adjacent ranges.
   * @param targetSize split a merged run longer than this into parts of about this size;
   *     {@code <= 0} never splits (one request per coalesced run).
   * @param toleranceRatio a run up to {@code targetSize * toleranceRatio} long is left as one part
   *     rather than split; a trailing part shorter than {@code targetSize * (toleranceRatio - 1)}
   *     is folded into the previous part. Must be {@code >= 1.0}. The Accelerator uses 1.4.
   */
  public ParquetReadRangePlanner(long coalesceGap, long targetSize, double toleranceRatio) {
    if (coalesceGap < 0) throw new IllegalArgumentException("coalesceGap < 0: " + coalesceGap);
    if (toleranceRatio < 1.0) {
      throw new IllegalArgumentException("toleranceRatio < 1: " + toleranceRatio);
    }
    this.coalesceGap = coalesceGap;
    this.targetSize = targetSize;
    this.toleranceRatio = toleranceRatio;
  }

  /**
   * Plans {@code ranges} into coalesced, split output ranges. Empty and zero-length ranges are
   * dropped (nothing to read). The input is not mutated; the output is a fresh sorted list.
   */
  public List<Range> plan(List<Range> ranges) {
    if (ranges == null || ranges.isEmpty()) return new ArrayList<>();
    List<Range> sorted = new ArrayList<>(ranges.size());
    for (Range r : ranges) {
      if (r.length > 0) sorted.add(r);
    }
    if (sorted.isEmpty()) return new ArrayList<>();
    Collections.sort(sorted);

    // 1. Coalesce: walk in offset order, merging into the open run while the gap is within
    //    tolerance. Ranges may overlap (two pages sharing a region in a bad offset index); the run
    //    end is the max.
    List<Range> coalesced = new ArrayList<>();
    long runStart = sorted.get(0).offset;
    long runEnd = sorted.get(0).end();
    for (int i = 1; i < sorted.size(); i++) {
      Range r = sorted.get(i);
      long gap = r.offset - runEnd;
      if (gap <= coalesceGap) {
        if (r.end() > runEnd) runEnd = r.end();
      } else {
        coalesced.add(new Range(runStart, runEnd - runStart));
        runStart = r.offset;
        runEnd = r.end();
      }
    }
    coalesced.add(new Range(runStart, runEnd - runStart));

    if (targetSize <= 0) return coalesced;

    // 2. Split: a run longer than targetSize * toleranceRatio becomes parts of about targetSize. A
    //    last part shorter than targetSize * (toleranceRatio - 1) is folded into the one before it.
    long splitAbove = (long) Math.ceil(targetSize * toleranceRatio);
    long minTail = (long) Math.floor(targetSize * (toleranceRatio - 1.0));
    List<Range> out = new ArrayList<>();
    for (Range run : coalesced) {
      if (run.length <= splitAbove) {
        out.add(run);
        continue;
      }
      long off = run.offset;
      long remaining = run.length;
      while (remaining > 0) {
        if (remaining <= targetSize + minTail) {
          // The rest fits in one part (<= targetSize, or a short tail folded onto this part).
          out.add(new Range(off, remaining));
          remaining = 0;
        } else {
          out.add(new Range(off, targetSize));
          off += targetSize;
          remaining -= targetSize;
        }
      }
    }
    return out;
  }

  /** The number of input bytes (sum of lengths), to account a planned read against the budget. */
  public static long totalBytes(List<Range> ranges) {
    long sum = 0;
    for (Range r : ranges) sum += r.length;
    return sum;
  }
}
