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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import org.junit.jupiter.api.Test;

/** #840: the calloc/free arena behind an owning {@link ColumnBuilder}. */
class NativeArenaTest {

    @Test
    void allocatesZeroedAlignedSegmentsAndFreesThemOnClose() {
        NativeArena a = new NativeArena();
        for (long align : new long[] {1, 8, 64, 4096}) {
            MemorySegment s = a.allocate(10_000, align);
            assertEquals(10_000, s.byteSize());
            assertEquals(0, s.address() % align, "aligned to " + align);
            for (long i = 0; i < s.byteSize(); i++) {
                assertEquals(0, s.get(ValueLayout.JAVA_BYTE, i), "zeroed at " + i);
            }
            s.set(ValueLayout.JAVA_LONG_UNALIGNED, s.byteSize() - 8, -1L); // the last bytes are ours
        }
        MemorySegment empty = a.allocate(0, 64);
        assertEquals(0, empty.byteSize());
        a.close();
        a.close(); // idempotent
        assertThrows(IllegalStateException.class, () -> a.allocate(8, 8));
    }

    @Test
    void rejectsABadAlignment() {
        NativeArena a = new NativeArena();
        assertThrows(IllegalArgumentException.class, () -> a.allocate(8, 3));
        assertThrows(IllegalArgumentException.class, () -> a.allocate(-1, 8));
        a.close();
    }
}
