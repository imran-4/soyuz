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
import java.util.function.BiFunction;

public class MemorySegmentObjectPool {

    private HashMap<Integer, ArrayList<MemorySegment>> onHeapPool = new HashMap<>(), offHeapPool = new HashMap<>();


    public MemorySegment getOrCreateUnpooledSegment(int size, Object owner) {
        return getOrCreate(onHeapPool, MemorySegmentFactory::allocateUnpooledSegment, size, owner);
    }

    public MemorySegment getOrCreateOffHeapUnsafeMemory(int size, Object owner) {
        return getOrCreate(offHeapPool, MemorySegmentFactory::allocateOffHeapUnsafeMemory, size, owner);
    }

    public synchronized void returnToPool(MemorySegment segment) {
        //segment.setOwner(null);
        if (segment.inPool)
            throw new RuntimeException();
        segment.inPool = true;
        if (!segment.isOffHeap()) {
            getPoolForSize(onHeapPool, segment.size()).add(segment);
        } else {
//			if (getPoolForSize(offHeapPool, segment.size()).contains(segment))
//				throw new RuntimeException(); /////////////////////////////////////////////////

            getPoolForSize(offHeapPool, segment.size()).add(segment);
        }
    }

    public void freeAllAndClear() {
        hmFreeAllAndClear(onHeapPool);
        hmFreeAllAndClear(offHeapPool);
    }

    private void hmFreeAllAndClear(HashMap<Integer, ArrayList<MemorySegment>> hm) {
        hm.values().forEach(memorySegments -> memorySegments.forEach(MemorySegment::free));
        hm.clear();
    }


    private MemorySegment getOrCreate(HashMap<Integer, ArrayList<MemorySegment>> hm, BiFunction<Integer, Object, MemorySegment> allocNew, int size, Object owner) {
        ArrayList<MemorySegment> pool = getPoolForSize(hm, size);
        if (!pool.isEmpty()) {
            synchronized (this) {
                if (!pool.isEmpty()) {
                    MemorySegment toRet = pool.remove(pool.size() - 1).setOwner(owner);
                    if (!toRet.inPool)
                        throw new RuntimeException();
                    toRet.inPool = false;
                    return toRet;
                } else {
                    return allocNew.apply(size, owner);
                }
            }
        } else {
            return allocNew.apply(size, owner);
        }
    }

    private ArrayList<MemorySegment> getPoolForSize(HashMap<Integer, ArrayList<MemorySegment>> hm, int size) {
        ArrayList<MemorySegment> pool = hm.get(size);
        if (pool == null) {
            synchronized (this) {
                return hm.computeIfAbsent(size, ignored_size -> new ArrayList<>());
            }
        } else {
            return pool;
        }
    }
}