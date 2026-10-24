/*
 * Copyright 2026-2027 Angel Conde and the vecruntime contributors
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

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CopyCountersTest {

    @Test
    void everyOverloadCopiesLikeMemorySegmentCopy() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment a = arena.allocate(64, 8);
            for (int i = 0; i < 16; i++) {
                a.setAtIndex(ValueLayout.JAVA_INT, i, i + 1);
            }
            MemorySegment b = arena.allocate(64, 8);
            CopyCounters.copy("t.bytes", a, 8, b, 0, 16);
            assertEquals(3, b.getAtIndex(ValueLayout.JAVA_INT, 0));
            assertEquals(6, b.getAtIndex(ValueLayout.JAVA_INT, 3));

            MemorySegment c = arena.allocate(64, 8);
            CopyCounters.copy("t.layout", a, ValueLayout.JAVA_INT, 0, c, ValueLayout.JAVA_INT,
                    4, 2);
            assertEquals(1, c.getAtIndex(ValueLayout.JAVA_INT, 1));
            assertEquals(2, c.getAtIndex(ValueLayout.JAVA_INT, 2));

            int[] out = new int[3];
            CopyCounters.copy("t.toArray", a, ValueLayout.JAVA_INT, 4, out, 0,
                    3);
            assertArrayEquals(new int[] {2, 3, 4}, out);

            MemorySegment d = arena.allocate(64, 8);
            CopyCounters.copy("t.fromArray", new long[] {7, 8}, 0, d,
                    ValueLayout.JAVA_LONG, 8, 2);
            assertEquals(7L, d.getAtIndex(ValueLayout.JAVA_LONG, 1));
            assertEquals(8L, d.getAtIndex(ValueLayout.JAVA_LONG, 2));
        }
        // The property is not set in tests: nothing is counted.
        assertFalse(CopyCounters.ON);
        assertTrue(CopyCounters.snapshot()
                .isEmpty());
    }

    @Test
    void countsBytesAndCallsPerSite() {
        CopyCounters.resetForTests();
        try {
            CopyCounters.count("a", 100);
            CopyCounters.count("a", 28);
            CopyCounters.count("b", 4);
            CopyCounters.count("c", 0); // an empty copy is not a call
            var s = CopyCounters.snapshot();
            assertArrayEquals(new long[] {128, 2}, s.get("a"));
            assertArrayEquals(new long[] {4, 1}, s.get("b"));
            assertFalse(s.containsKey("c"));
        } finally {
            CopyCounters.resetForTests();
        }
    }
}
