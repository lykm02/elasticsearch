/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.fielddata.ordinals;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.cursors.IntCursor;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.*;
import org.apache.lucene.util.packed.AppendingPackedLongBuffer;
import org.apache.lucene.util.packed.GrowableWriter;
import org.apache.lucene.util.packed.MonotonicAppendingLongBuffer;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.indices.fielddata.breaker.CircuitBreakerService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 */
public class InternalGlobalOrdinalsBuilder extends AbstractIndexComponent implements GlobalOrdinalsBuilder {

    public final static String ORDINAL_MAPPING_THRESHOLD_KEY = "threshold";

    public InternalGlobalOrdinalsBuilder(Index index, @IndexSettings Settings indexSettings) {
        super(index, indexSettings);
    }

    @Override
    public IndexFieldData.WithOrdinals build(final IndexReader indexReader, IndexFieldData.WithOrdinals indexFieldData, Settings settings, CircuitBreakerService breakerService) throws IOException {
        assert indexReader.leaves().size() > 1;
        long startTime = System.currentTimeMillis();

        // It makes sense to make the overhead ratio configurable for the mapping from segment ords to global ords
        // However, other mappings are never the bottleneck and only used to get the original value from an ord, so
        // it makes sense to force COMPACT for them
        final float acceptableOverheadRatio = settings.getAsFloat("acceptable_overhead_ratio", PackedInts.FAST);
        final AppendingPackedLongBuffer globalOrdToFirstSegment = new AppendingPackedLongBuffer(PackedInts.COMPACT);
        globalOrdToFirstSegment.add(0);
        final MonotonicAppendingLongBuffer globalOrdToFirstSegmentDelta = new MonotonicAppendingLongBuffer(PackedInts.COMPACT);
        globalOrdToFirstSegmentDelta.add(0);

        FieldDataType fieldDataType = indexFieldData.getFieldDataType();
        int threshold = fieldDataType.getSettings().getAsInt(ORDINAL_MAPPING_THRESHOLD_KEY, 2048);
        OrdinalMappingSourceBuilder ordinalMappingBuilder = new OrdinalMappingSourceBuilder(
                indexReader.leaves().size(), acceptableOverheadRatio, threshold
        );

        long currentGlobalOrdinal = 0;
        final AtomicFieldData.WithOrdinals[] withOrdinals = new AtomicFieldData.WithOrdinals[indexReader.leaves().size()];
        TermIterator termIterator = new TermIterator(indexFieldData, indexReader.leaves(), withOrdinals);
        for (BytesRef term = termIterator.next(); term != null; term = termIterator.next()) {
            currentGlobalOrdinal++;
            globalOrdToFirstSegment.add(termIterator.firstReaderIndex());
            long globalOrdinalDelta = currentGlobalOrdinal - termIterator.firstLocalOrdinal();
            globalOrdToFirstSegmentDelta.add(globalOrdinalDelta);
            for (TermIterator.LeafSource leafSource : termIterator.competitiveLeafs()) {
                ordinalMappingBuilder.onOrdinal(leafSource.context.ord, leafSource.currentLocalOrd, currentGlobalOrdinal);
            }
        }

        // ram used for the globalOrd to segmentOrd and segmentOrd to firstReaderIndex lookups
        long memorySizeInBytesCounter = 0;
        globalOrdToFirstSegment.freeze();
        memorySizeInBytesCounter += globalOrdToFirstSegment.ramBytesUsed();
        globalOrdToFirstSegmentDelta.freeze();
        memorySizeInBytesCounter += globalOrdToFirstSegmentDelta.ramBytesUsed();

        final long maxOrd = currentGlobalOrdinal + 1;
        OrdinalMappingSource[] segmentOrdToGlobalOrdLookups = ordinalMappingBuilder.build(maxOrd);
        // add ram used for the main segmentOrd to globalOrd lookups
        memorySizeInBytesCounter += ordinalMappingBuilder.getMemorySizeInBytes();

        final long memorySizeInBytes = memorySizeInBytesCounter;
        breakerService.getBreaker().addWithoutBreaking(memorySizeInBytes);

        if (logger.isDebugEnabled()) {
            String implName = ordinalMappingBuilder.getClass().getName();
            logger.debug(
                    "Loading of global-ordinals[{}] loaded {} values and took: {} ms",
                    implName,
                    maxOrd,
                    (System.currentTimeMillis() - startTime)
            );
        }
        return new GlobalOrdinalsIndexFieldData(indexFieldData.index(), settings, indexFieldData.getFieldNames(),
                fieldDataType, withOrdinals, globalOrdToFirstSegment, globalOrdToFirstSegmentDelta,
                segmentOrdToGlobalOrdLookups, memorySizeInBytes
        );
    }

    public interface OrdinalMappingSource {

        Ordinals.Docs globalOrdinals(Ordinals.Docs segmentOrdinals);

    }

    private static abstract class GlobalOrdinalMapping implements Ordinals.Docs {

        protected final Ordinals.Docs segmentOrdinals;
        private final long memorySizeInBytes;
        protected final long maxOrd;

        protected long currentGlobalOrd;

        private GlobalOrdinalMapping(Ordinals.Docs segmentOrdinals, long memorySizeInBytes, long maxOrd) {
            this.segmentOrdinals = segmentOrdinals;
            this.memorySizeInBytes = memorySizeInBytes;
            this.maxOrd = maxOrd;
        }

        @Override
        public final Ordinals ordinals() {
            return new Ordinals() {
                @Override
                public long getMemorySizeInBytes() {
                    return memorySizeInBytes;
                }

                @Override
                public boolean isMultiValued() {
                    return GlobalOrdinalMapping.this.isMultiValued();
                }

                @Override
                public int getNumDocs() {
                    return GlobalOrdinalMapping.this.getNumDocs();
                }

                @Override
                public long getNumOrds() {
                    return GlobalOrdinalMapping.this.getNumOrds();
                }

                @Override
                public long getMaxOrd() {
                    return GlobalOrdinalMapping.this.getMaxOrd();
                }

                @Override
                public Docs ordinals() {
                    return GlobalOrdinalMapping.this;
                }
            };
        }

        @Override
        public final int getNumDocs() {
            return segmentOrdinals.getNumDocs();
        }

        @Override
        public final long getNumOrds() {
            return maxOrd - Ordinals.MIN_ORDINAL;
        }

        @Override
        public final long getMaxOrd() {
            return maxOrd;
        }

        @Override
        public final boolean isMultiValued() {
            return segmentOrdinals.isMultiValued();
        }

        @Override
        public final int setDocument(int docId) {
            return segmentOrdinals.setDocument(docId);
        }

        @Override
        public final long currentOrd() {
            return currentGlobalOrd;
        }

        @Override
        public final long getOrd(int docId) {
            long segmentOrd = segmentOrdinals.getOrd(docId);
            return currentGlobalOrd = getGlobalOrd(segmentOrd);
        }

        @Override
        public final LongsRef getOrds(int docId) {
            LongsRef refs = segmentOrdinals.getOrds(docId);
            for (int i = refs.offset; i < refs.length; i++) {
                refs.longs[i] = getGlobalOrd(refs.longs[i]);
            }
            return refs;
        }

        @Override
        public final long nextOrd() {
            long segmentOrd = segmentOrdinals.nextOrd();
            return currentGlobalOrd = getGlobalOrd(segmentOrd);
        }

        protected abstract long getGlobalOrd(long segmentOrd);

    }

    private final static class OrdinalMappingSourceBuilder {

        final MonotonicAppendingLongBuffer[] segmentOrdToGlobalOrdDeltas;
        final float acceptableOverheadRatio;
        final int numSegments;
        final int threshold;

        long memorySizeInBytesCounter;

        private OrdinalMappingSourceBuilder(int numSegments, float acceptableOverheadRatio, int threshold) {
            segmentOrdToGlobalOrdDeltas = new MonotonicAppendingLongBuffer[numSegments];
            for (int i = 0; i < segmentOrdToGlobalOrdDeltas.length; i++) {
                segmentOrdToGlobalOrdDeltas[i] = new MonotonicAppendingLongBuffer(acceptableOverheadRatio);
                segmentOrdToGlobalOrdDeltas[i].add(0);
            }
            this.numSegments = numSegments;
            this.acceptableOverheadRatio = acceptableOverheadRatio;
            this.threshold = threshold;
        }

        public void onOrdinal(int readerIndex, long segmentOrdinal, long globalOrdinal) {
            long delta = globalOrdinal - segmentOrdinal;
            segmentOrdToGlobalOrdDeltas[readerIndex].add(delta);
        }

        public OrdinalMappingSource[] build(long maxOrd) {
            // If we find out that there are less then predefined number of ordinals, it is better to put the the
            // segment ordinal to global ordinal mapping in a packed ints, since the amount values are small and
            // will most likely fit in the CPU caches and MonotonicAppendingLongBuffer's compression will just be
            // unnecessary.

            if (maxOrd <= threshold) {
                // Rebuilding from MonotonicAppendingLongBuffer to GrowableWriter is fast
                GrowableWriter[] newSegmentOrdToGlobalOrdDeltas = new GrowableWriter[numSegments];
                for (int i = 0; i < segmentOrdToGlobalOrdDeltas.length; i++) {
                    newSegmentOrdToGlobalOrdDeltas[i] = new GrowableWriter(1, (int) segmentOrdToGlobalOrdDeltas[i].size(), acceptableOverheadRatio);
                }

                for (int readerIndex = 0; readerIndex < segmentOrdToGlobalOrdDeltas.length; readerIndex++) {
                    MonotonicAppendingLongBuffer segmentOrdToGlobalOrdDelta = segmentOrdToGlobalOrdDeltas[readerIndex];

                    for (long ordIndex = 0; ordIndex < segmentOrdToGlobalOrdDelta.size(); ordIndex++) {
                        long ordDelta = segmentOrdToGlobalOrdDelta.get(ordIndex);
                        newSegmentOrdToGlobalOrdDeltas[readerIndex].set((int) ordIndex, ordDelta);
                    }
                }

                PackedIntOrdinalMappingSource[] sources = new PackedIntOrdinalMappingSource[numSegments];
                for (int i = 0; i < newSegmentOrdToGlobalOrdDeltas.length; i++) {
                    PackedInts.Reader segmentOrdToGlobalOrdDelta = newSegmentOrdToGlobalOrdDeltas[i].getMutable();
                    long ramUsed = segmentOrdToGlobalOrdDelta.ramBytesUsed();
                    sources[i] = new PackedIntOrdinalMappingSource(segmentOrdToGlobalOrdDelta, ramUsed, maxOrd);
                    memorySizeInBytesCounter += ramUsed;
                }
                return sources;
            } else {
                OrdinalMappingSource[] sources = new OrdinalMappingSource[segmentOrdToGlobalOrdDeltas.length];
                for (int i = 0; i < segmentOrdToGlobalOrdDeltas.length; i++) {
                    MonotonicAppendingLongBuffer segmentOrdToGlobalOrdLookup = segmentOrdToGlobalOrdDeltas[i];
                    segmentOrdToGlobalOrdLookup.freeze();
                    long ramUsed = segmentOrdToGlobalOrdLookup.ramBytesUsed();
                    sources[i] = new CompressedOrdinalMappingSource(segmentOrdToGlobalOrdLookup, ramUsed, maxOrd);
                    memorySizeInBytesCounter += ramUsed;
                }
                return sources;
            }
        }

        public long getMemorySizeInBytes() {
            return memorySizeInBytesCounter;
        }
    }

    private final static class CompressedOrdinalMappingSource implements OrdinalMappingSource {

        private final MonotonicAppendingLongBuffer globalOrdinalMapping;
        private final long memorySizeInBytes;
        private final long maxOrd;

        private CompressedOrdinalMappingSource(MonotonicAppendingLongBuffer globalOrdinalMapping, long memorySizeInBytes, long maxOrd) {
            this.globalOrdinalMapping = globalOrdinalMapping;
            this.memorySizeInBytes = memorySizeInBytes;
            this.maxOrd = maxOrd;
        }

        @Override
        public Ordinals.Docs globalOrdinals(Ordinals.Docs segmentOrdinals) {
            return new GlobalOrdinalsDocs(segmentOrdinals, globalOrdinalMapping, memorySizeInBytes, maxOrd);
        }

        private final static class GlobalOrdinalsDocs extends GlobalOrdinalMapping {

            private final MonotonicAppendingLongBuffer segmentOrdToGlobalOrdLookup;

            private GlobalOrdinalsDocs(Ordinals.Docs segmentOrdinals, MonotonicAppendingLongBuffer segmentOrdToGlobalOrdLookup, long memorySizeInBytes, long maxOrd) {
                super(segmentOrdinals, memorySizeInBytes, maxOrd);
                this.segmentOrdToGlobalOrdLookup = segmentOrdToGlobalOrdLookup;
            }

            @Override
            protected long getGlobalOrd(long segmentOrd) {
                return segmentOrd + segmentOrdToGlobalOrdLookup.get(segmentOrd);
            }
        }

    }

    private static final class PackedIntOrdinalMappingSource implements OrdinalMappingSource {

        private final PackedInts.Reader segmentOrdToGlobalOrdLookup;
        private final long memorySizeInBytes;
        private final long maxOrd;

        private PackedIntOrdinalMappingSource(PackedInts.Reader segmentOrdToGlobalOrdLookup, long memorySizeInBytes, long maxOrd) {
            this.segmentOrdToGlobalOrdLookup = segmentOrdToGlobalOrdLookup;
            this.memorySizeInBytes = memorySizeInBytes;
            this.maxOrd = maxOrd;
        }

        @Override
        public Ordinals.Docs globalOrdinals(Ordinals.Docs segmentOrdinals) {
            return new GlobalOrdinalsDocs(segmentOrdinals, memorySizeInBytes, maxOrd, segmentOrdToGlobalOrdLookup);
        }

        private final static class GlobalOrdinalsDocs extends GlobalOrdinalMapping {

            private final PackedInts.Reader segmentOrdToGlobalOrdLookup;

            private GlobalOrdinalsDocs(Ordinals.Docs segmentOrdinals, long memorySizeInBytes, long maxOrd, PackedInts.Reader segmentOrdToGlobalOrdLookup) {
                super(segmentOrdinals, memorySizeInBytes, maxOrd);
                this.segmentOrdToGlobalOrdLookup = segmentOrdToGlobalOrdLookup;
            }

            @Override
            protected long getGlobalOrd(long segmentOrd) {
                return segmentOrd + segmentOrdToGlobalOrdLookup.get((int) segmentOrd);
            }
        }

    }

    private final static class TermIterator implements BytesRefIterator {

        private final List<LeafSource> leafSources;

        private final IntArrayList sourceSlots;
        private final IntArrayList competitiveSlots;
        private BytesRef currentTerm;

        private TermIterator(IndexFieldData.WithOrdinals indexFieldData, List<AtomicReaderContext> leaves, AtomicFieldData.WithOrdinals[] withOrdinals) throws IOException {
            this.leafSources = new ArrayList<>(leaves.size());
            this.sourceSlots = IntArrayList.newInstanceWithCapacity(leaves.size());
            this.competitiveSlots = IntArrayList.newInstanceWithCapacity(leaves.size());
            for (int i = 0; i < leaves.size(); i++) {
                AtomicReaderContext leaf = leaves.get(i);
                AtomicFieldData.WithOrdinals afd = indexFieldData.load(leaf);
                withOrdinals[i] = afd;
                leafSources.add(new LeafSource(leaf, afd));
            }
        }

        public BytesRef next() throws IOException {
            if (currentTerm == null) {
                for (int slot = 0; slot < leafSources.size(); slot++) {
                    LeafSource leafSource = leafSources.get(slot);
                    if (leafSource.next() != null) {
                        sourceSlots.add(slot);
                    }
                }
            }
            if (sourceSlots.isEmpty()) {
                return null;
            }

            if (!competitiveSlots.isEmpty()) {
                for (IntCursor cursor : competitiveSlots) {
                    if (leafSources.get(cursor.value).next() == null) {
                        sourceSlots.removeFirstOccurrence(cursor.value);
                    }
                }
                competitiveSlots.clear();
            }
            BytesRef lowest = null;
            for (IntCursor cursor : sourceSlots) {
                LeafSource leafSource = leafSources.get(cursor.value);
                if (lowest == null) {
                    lowest = leafSource.currentTerm;
                    competitiveSlots.add(cursor.value);
                } else {
                    int cmp = lowest.compareTo(leafSource.currentTerm);
                    if (cmp == 0) {
                        competitiveSlots.add(cursor.value);
                    } else if (cmp > 0) {
                        competitiveSlots.clear();
                        lowest = leafSource.currentTerm;
                        competitiveSlots.add(cursor.value);
                    }
                }
            }

            if (competitiveSlots.isEmpty()) {
                return currentTerm = null;
            } else {
                return currentTerm = lowest;
            }
        }

        @Override
        public Comparator<BytesRef> getComparator() {
            return BytesRef.getUTF8SortedAsUnicodeComparator();
        }

        List<LeafSource> competitiveLeafs() throws IOException {
            List<LeafSource> docsEnums = new ArrayList<LeafSource>(competitiveSlots.size());
            for (IntCursor cursor : competitiveSlots) {
                LeafSource leafSource = leafSources.get(cursor.value);
                docsEnums.add(leafSource);
            }
            return docsEnums;
        }

        int firstReaderIndex() {
            int slot = competitiveSlots.get(0);
            return leafSources.get(slot).context.ord;
        }

        long firstLocalOrdinal() {
            int slot = competitiveSlots.get(0);
            return leafSources.get(slot).currentLocalOrd;
        }

        private static class LeafSource {

            final AtomicReaderContext context;
            final BytesValues.WithOrdinals afd;
            final long localMaxOrd;

            long currentLocalOrd = Ordinals.MISSING_ORDINAL;
            BytesRef currentTerm;

            private LeafSource(AtomicReaderContext context, AtomicFieldData.WithOrdinals afd) throws IOException {
                this.context = context;
                this.afd = afd.getBytesValues(false);
                this.localMaxOrd = this.afd.ordinals().getMaxOrd();
            }

            BytesRef next() throws IOException {
                if (++currentLocalOrd < localMaxOrd) {
                    return currentTerm = afd.getValueByOrd(currentLocalOrd);
                } else {
                    return null;
                }
            }

        }

    }
}
