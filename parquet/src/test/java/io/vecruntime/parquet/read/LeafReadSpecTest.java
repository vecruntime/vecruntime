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
package io.vecruntime.parquet.read;

import io.vecruntime.kernels.VecType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LeafReadSpecTest {

    @Test
    void defaultsReadAsStored() {
        LeafReadSpec s = LeafReadSpec.onLane(VecType.INT64).build();
        assertEquals(VecType.INT64, s.lane);
        assertEquals(LeafReadSpec.WIDEN_NONE, s.widen);
        assertEquals(0, s.rescale);
        assertEquals(0, s.narrowBits);
        assertFalse(s.millis || s.dateLane || s.timestampLane || s.decimal || s.float32 || s.dictionaryIdsAllowed);
    }

    @Test
    void carriesEveryConversion() {
        LeafReadSpec s = LeafReadSpec.onLane(VecType.DECIMAL128)
                .widen(LeafReadSpec.WIDEN_LONG_TO_DEC128)
                .rescale(4)
                .decimal(true)
                .build();
        assertEquals(LeafReadSpec.WIDEN_LONG_TO_DEC128, s.widen);
        assertEquals(4, s.rescale);
        assertTrue(s.decimal);
        LeafReadSpec t = LeafReadSpec.onLane(VecType.INT32)
                .narrowBits(16)
                .build();
        assertEquals(16, t.narrowBits);
        assertTrue(LeafReadSpec.onLane(VecType.UTF8).dictionaryIdsAllowed(true).build().dictionaryIdsAllowed);
    }

    @Test
    void refusesWhatNoReaderDoes() {
        assertThrows(IllegalArgumentException.class, () -> LeafReadSpec.onLane(null));
        assertThrows(IllegalArgumentException.class,
                () -> LeafReadSpec.onLane(VecType.INT32).narrowBits(12));
        assertThrows(IllegalArgumentException.class,
                () -> LeafReadSpec.onLane(VecType.INT64).widen(7));
        assertThrows(IllegalArgumentException.class,
                () -> LeafReadSpec.onLane(VecType.INT64).rescale(-1));
    }
}
