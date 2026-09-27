/*
 * Copyright 2025-2026 Angel Conde and the vecruntime contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.vecruntime.kernels;

import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorOperators;

import static io.vecruntime.kernels.Species.L;

/**
 * Window frames over one sorted partition (#58, {@code RANGE} frames with value
 * offsets). The partition's rows are contiguous heap arrays the operator gathers
 * from its held batches and reuses across partitions; validity is a bitmap of
 * {@code long} words ({@code null} for a column without nulls), row {@code i}
 * valid when bit {@code i & 63} of word {@code i >>> 6} is set.
 *
 * <p>{@link #rangeBounds} is Spark's {@code SlidingWindowFunctionFrame} walk
 * ({@code WindowEvaluatorFactoryBase.createBoundOrdering}): for row {@code i}
 * with order key {@code k}, the frame holds the rows {@code j} whose key
 * compares {@code >= k + lower} and {@code <= k + upper} in the {@code
 * SortOrder}'s comparison (direction and null ordering), where the offsets
 * carry the sign Spark gave them ({@code n PRECEDING} is {@code -n}, negated
 * again for {@code DESC}). The bound is the key's own arithmetic: {@code Add}
 * wraps at the key's width unless ANSI mode makes it an overflow error, and
 * {@code DateAdd} over a date key wraps. A null key has a null bound, which
 * compares equal to a null key and before or after every value by the null
 * ordering, so a null row's frame is exactly the null peer group. Both bounds
 * move monotonically with {@code i}, and the frame is {@code [lo, hi)} with
 * {@code lo <= hi}: {@code hi} is the first row past the upper bound, {@code
 * lo} the first row of {@code [lo, hi)} not below the lower bound -- Spark's
 * buffer after its drop-then-add pass. An empty frame is {@code lo == hi}.
 *
 * <p>The aggregates re-run Spark's {@code AggregateProcessor} over each frame:
 * {@code processor.initialize} then one {@code update} per row of the buffer in
 * order. A frame whose lower bound has not moved continues the previous row's
 * accumulation (the same additions in the same order, so a double sum is
 * bit-identical to a fresh pass); one whose lower bound moved starts over. The
 * order-independent reductions -- {@code count}, an unchecked {@code bigint}
 * sum -- slide instead: rows leaving at the lower bound are subtracted, rows
 * entering at the upper bound added, exact under two's-complement wrapping. A
 * checked (ANSI) sum re-adds in order with {@code Math.addExact}, since the
 * overflow Spark raises is a property of the partial sums.
 */
public final class WindowFrameKernels {

    private WindowFrameKernels() {}

    static boolean valid(long[] validity, int i) {
        return validity == null || ((validity[i >>> 6] >>> (i & 63)) & 1L) != 0L;
    }

    /**
     * The {@code SortOrder} comparison of a row's key against a bound, either
     * of which may be null: nulls equal, and a null sorts before every value
     * when {@code nullsFirst} (Spark's default for ASC) or after it otherwise.
     */
    static int compare(boolean keyValid, long key, boolean boundValid,
                       long bound, boolean descending, boolean nullsFirst) {
        if (keyValid && boundValid) {
            int c = Long.compare(key, bound);
            return descending ? -c : c;
        }
        if (!keyValid && !boundValid) {
            return 0;
        }
        if (!keyValid) {
            return nullsFirst ? -1 : 1;
        }
        return nullsFirst ? 1 : -1;
    }

    /**
     * {@code key + offset} in the key's own width ({@code keyBits}: 8, 16, 32
     * or 64), wrapped like Spark's non-ANSI {@code Add} (and {@code DateAdd}),
     * or an {@link ArithmeticException} when {@code checked} and the sum leaves
     * the width (Spark's ANSI {@code Add}).
     */
    static long bound(long key, long offset, int keyBits,
                      boolean checked) {
        long r = key + offset;
        if (keyBits == 64) {
            if (checked && ((key ^ r) & (offset ^ r)) < 0) {
                throw new ArithmeticException("long overflow");
            }
            return r;
        }
        int shift = 64 - keyBits;
        long wrapped = (r << shift) >> shift;
        if (checked && wrapped != r) {
            throw new ArithmeticException("integer overflow");
        }
        return wrapped;
    }

    /**
     * Frame bounds of every row of a partition sorted by {@code keys} in the
     * {@code SortOrder}'s direction and null ordering: row {@code i}'s frame is
     * {@code [lo[i], hi[i])}. {@code loUnbounded} / {@code hiUnbounded} stand
     * for {@code UNBOUNDED PRECEDING} / {@code UNBOUNDED FOLLOWING}; otherwise
     * the bound is {@code key + loOffset} / {@code key + hiOffset} (see {@link
     * #bound}), {@code CURRENT ROW} being offset 0.
     *
     * @throws ArithmeticException when {@code checked} and a bound overflows
     *         the key's width
     */
    public static void rangeBounds(
            long[] keys,
            long[] validity,
            int n,
            boolean descending,
            boolean nullsFirst,
            boolean loUnbounded,
            long loOffset,
            boolean hiUnbounded,
            long hiOffset,
            int keyBits,
            boolean checked,
            int[] lo,
            int[] hi) {
        int l = 0;
        int h = 0;
        for (int i = 0; i < n; i++) {
            boolean kv = valid(validity, i);
            long k = keys[i];
            // A bound is evaluated only when there is a row to compare it with, as Spark's
            // `nextRow != null && ubound.compare(...)` / `!buffer.isEmpty && lbound.compare(...)`
            // short-circuit: past the partition's last row an overflowing bound never raises.
            if (hiUnbounded) {
                h = n;
            } else if (h < n) {
                long b = kv ? bound(k, hiOffset, keyBits, checked) : 0L;
                while (h < n && compare(valid(validity, h), keys[h], kv, b, descending,
                        nullsFirst)
                        <= 0) {
                    h++;
                }
            }
            if (loUnbounded) {
                l = 0;
            } else if (l < h) {
                long b = kv ? bound(k, loOffset, keyBits, checked) : 0L;
                while (l < h && compare(valid(validity, l), keys[l], kv, b, descending,
                        nullsFirst)
                        < 0) {
                    l++;
                }
            }
            lo[i] = l;
            hi[i] = h;
        }
    }

    /**
     * {@code count} over each frame: every row when {@code countAll} ({@code
     * count(*)}, {@code count(1)}), the valid ones otherwise. Slides: the rows
     * leaving at the lower bound are subtracted, the rows entering at the
     * upper bound added.
     */
    public static void frameCount(long[] validity, int[] lo, int[] hi,
            int n, boolean countAll, long[] out) {
        if (countAll || validity == null) {
            for (int i = 0; i < n; i++) {
                out[i] = (long) hi[i] - lo[i];
            }
            return;
        }
        int l = 0;
        int h = 0;
        long count = 0;
        for (int i = 0; i < n; i++) {
            int nl = lo[i];
            int nh = hi[i];
            if (nl > h || nh < h) {
                // The frame jumped past the old window: nothing of it is left to slide from.
                count = 0;
                l = nl;
                h = nl;
            }
            for (; l < nl; l++) {
                if (valid(validity, l)) {
                    count--;
                }
            }
            for (; h < nh; h++) {
                if (valid(validity, h)) {
                    count++;
                }
            }
            out[i] = count;
        }
    }

    /**
     * {@code sum} of a {@code bigint} (or narrower integral, widened) input over
     * each frame; null (bit cleared in {@code outValidity}) when the frame has
     * no valid row. Unchecked, the sum slides under wrapping arithmetic;
     * {@code checked} (ANSI) re-adds the frame in order with {@code
     * Math.addExact}, as Spark's own re-aggregation does, so the same partial
     * sum overflows.
     *
     * @throws ArithmeticException when {@code checked} and a partial sum
     *         overflows
     */
    public static void frameSumLong(
            long[] values,
            long[] validity,
            int[] lo,
            int[] hi,
            int n,
            boolean checked,
            long[] out,
            long[] outValidity) {
        clearValidity(outValidity, n);
        if (checked) {
            int l = 0;
            int h = 0;
            long sum = 0;
            long count = 0;
            for (int i = 0; i < n; i++) {
                int nl = lo[i];
                int nh = hi[i];
                if (nl != l || nh < h) {
                    sum = 0;
                    count = 0;
                    l = nl;
                    h = nl;
                }
                for (; h < nh; h++) {
                    if (valid(validity, h)) {
                        sum = Math.addExact(sum, values[h]);
                        count++;
                    }
                }
                out[i] = sum;
                if (count != 0) {
                    setValid(outValidity, i);
                }
            }
            return;
        }
        int l = 0;
        int h = 0;
        long sum = 0;
        long count = 0;
        for (int i = 0; i < n; i++) {
            int nl = lo[i];
            int nh = hi[i];
            if (nl > h || nh < h) {
                sum = 0;
                count = 0;
                l = nl;
                h = nl;
            }
            for (; l < nl; l++) {
                if (valid(validity, l)) {
                    sum -= values[l];
                    count--;
                }
            }
            for (; h < nh; h++) {
                if (valid(validity, h)) {
                    sum += values[h];
                    count++;
                }
            }
            out[i] = sum;
            if (count != 0) {
                setValid(outValidity, i);
            }
        }
    }

    /**
     * {@code sum} (or {@code avg} when {@code average}) of a double input over
     * each frame, added in row order from the frame's first row as Spark's
     * sliding frame re-aggregates -- bit-identical to Spark's result. A frame
     * whose lower bound has not moved continues the previous accumulation. Null
     * when the frame has no valid row.
     */
    public static void frameSumDouble(
            double[] values,
            long[] validity,
            int[] lo,
            int[] hi,
            int n,
            boolean average,
            double[] out,
            long[] outValidity) {
        clearValidity(outValidity, n);
        int l = 0;
        int h = 0;
        double sum = 0.0;
        long count = 0;
        for (int i = 0; i < n; i++) {
            int nl = lo[i];
            int nh = hi[i];
            if (nl != l || nh < h) {
                sum = 0.0;
                count = 0;
                l = nl;
                h = nl;
            }
            if (validity == null) {
                for (; h < nh; h++) {
                    sum += values[h];
                }
                count = (long) nh - l;
            } else {
                for (; h < nh; h++) {
                    if (valid(validity, h)) {
                        sum += values[h];
                        count++;
                    }
                }
            }
            if (count != 0) {
                out[i] = average ? sum / count : sum;
                setValid(outValidity, i);
            } else {
                out[i] = 0.0;
            }
        }
    }

    /**
     * {@code min} / {@code max} of an integral input (int lanes widened to
     * long) over each frame; null when the frame has no valid row. A frame
     * whose lower bound has not moved extends the previous result; one that
     * moved is re-scanned -- lane-parallel when the column has no nulls and
     * the frame is at least two vectors long.
     */
    public static void frameMinMaxLong(
            long[] values,
            long[] validity,
            int[] lo,
            int[] hi,
            int n,
            boolean isMin,
            long[] out,
            long[] outValidity) {
        clearValidity(outValidity, n);
        int lanes = L.length();
        int l = 0;
        int h = 0;
        long best = 0;
        boolean any = false;
        for (int i = 0; i < n; i++) {
            int nl = lo[i];
            int nh = hi[i];
            if (nl != l || nh < h) {
                any = false;
                l = nl;
                h = nl;
                if (validity == null && nh - nl >= 2 * lanes) {
                    LongVector acc = LongVector.broadcast(L, values[nl]);
                    int j = nl;
                    for (; j + lanes <= nh; j += lanes) {
                        LongVector v = LongVector.fromArray(L, values, j);
                        acc = isMin ? acc.min(v) : acc.max(v);
                    }
                    best = acc.reduceLanes(isMin ? VectorOperators.MIN : VectorOperators.MAX);
                    any = true;
                    h = j;
                }
            }
            for (; h < nh; h++) {
                if (valid(validity, h)) {
                    long v = values[h];
                    if (!any
                            || (isMin ? v < best : v > best)) {
                        best = v;
                        any = true;
                    }
                }
            }
            if (any) {
                out[i] = best;
                setValid(outValidity, i);
            } else {
                out[i] = 0L;
            }
        }
    }

    /**
     * {@code min} / {@code max} of a double input over each frame in Spark's
     * ordering ({@code Least} / {@code Greatest} with {@code
     * nanSafeCompareDoubles}: NaN greatest, {@code -0.0 == 0.0}, a tie keeps
     * the earlier row); null when the frame has no valid row.
     */
    public static void frameMinMaxDouble(
            double[] values,
            long[] validity,
            int[] lo,
            int[] hi,
            int n,
            boolean isMin,
            double[] out,
            long[] outValidity) {
        clearValidity(outValidity, n);
        int l = 0;
        int h = 0;
        double best = 0.0;
        boolean any = false;
        for (int i = 0; i < n; i++) {
            int nl = lo[i];
            int nh = hi[i];
            if (nl != l || nh < h) {
                any = false;
                l = nl;
                h = nl;
            }
            for (; h < nh; h++) {
                if (valid(validity, h)) {
                    double v = values[h];
                    if (!any) {
                        best = v;
                        any = true;
                    } else {
                        int c = CompareOp.nanSafeCompare(v, best);
                        if (isMin ? c < 0 : c > 0) {
                            best = v;
                        }
                    }
                }
            }
            if (any) {
                out[i] = best;
                setValid(outValidity, i);
            } else {
                out[i] = 0.0;
            }
        }
    }

    static void clearValidity(long[] validity, int n) {
        java.util.Arrays.fill(validity, 0, (n + 63) >>> 6, 0L);
    }

    static void setValid(long[] validity, int i) {
        validity[i >>> 6] |= 1L << (i & 63);
    }
}
