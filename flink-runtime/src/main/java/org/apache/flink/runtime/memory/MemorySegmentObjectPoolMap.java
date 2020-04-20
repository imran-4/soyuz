/*
 *   Licensed to the Apache Software Foundation (ASF) under one or more
 *   contributor license agreements.  See the NOTICE file distributed with
 *   this work for additional information regarding copyright ownership.
 *   The ASF licenses this file to You under the Apache License, Version 2.0
 *   (the "License"); you may not use this file except in compliance with
 *   the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.flink.runtime.memory;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

public class MemorySegmentObjectPoolMap {

    private final ConcurrentHashMap<Object, MemorySegmentObjectPool> map = new ConcurrentHashMap<>();


    public MemorySegment getOrCreateUnpooledSegment(int size, Object owner) {
        return getForOwner(owner).getOrCreateUnpooledSegment(size, owner);
    }

    public MemorySegment getOrCreateOffHeapUnsafeMemory(int size, Object owner) {
        return getForOwner(owner).getOrCreateOffHeapUnsafeMemory(size, owner);
    }

    public void returnToPool(MemorySegment segment) {
        getForOwner(segment.getOwner()).returnToPool(segment);
    }

    public void freeAllAndClear() {
        map.values().forEach(MemorySegmentObjectPool::freeAllAndClear);
    }


    private MemorySegmentObjectPool getForOwner(Object owner) {
        return map.computeIfAbsent(owner, o -> new MemorySegmentObjectPool());
    }
}