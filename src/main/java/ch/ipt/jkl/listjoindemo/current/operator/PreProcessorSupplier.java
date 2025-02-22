package ch.ipt.jkl.listjoindemo.current.operator;

import com.google.common.collect.Sets;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class PreProcessorSupplier<TOuter> implements ProcessorSupplier<String, TOuter, String, TOuter> {

    private final ValueMapper<TOuter, List<TOuter>> flatMapper;
    private final Function<TOuter, String> innerIdStringExtractor;
    private final Function<TOuter, TOuter> listCleaner;

    /**
     * Create a new PreProcessorSupplier.
     *
     * @param flatMapper             flat maps the outers to each resulting outer contains exactly one inner
     * @param innerIdStringExtractor extract the id of the first inner from an outer. The outer passed to this function
     *                               is guaranteed to contain exactly one inner
     * @param listCleaner            a function that takes an outer and returns the same outer, just with an empty inner
     *                               list
     */
    public PreProcessorSupplier(
            ValueMapper<TOuter, List<TOuter>> flatMapper,
            Function<TOuter, String> innerIdStringExtractor,
            Function<TOuter, TOuter> listCleaner
    ) {
        this.flatMapper = flatMapper;
        this.innerIdStringExtractor = innerIdStringExtractor;
        this.listCleaner = listCleaner;
    }

    @Override
    public Processor<String, TOuter, String, TOuter> get() {
        return new PreProcessor<>(flatMapper, innerIdStringExtractor, listCleaner);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set<StoreBuilder<?>> stores() {
        return Set.of(
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(PreProcessor.LIST_STORE),
                        Serdes.String(),
                        Serdes.ListSerde(ArrayList.class, Serdes.String())
                )
        );
    }

    @RequiredArgsConstructor
    static class PreProcessor<TOuter> implements Processor<String, TOuter, String, TOuter> {

        // Note: this might collide if multiple instances are used
        public static final String LIST_STORE = "listStore";

        private ProcessorContext<String, TOuter> context;
        private KeyValueStore<String, List<String>> listStore;

        // the flat mapper needs to map tombstones and empty lists to an empty list
        private final ValueMapper<TOuter, List<TOuter>> flatMapper;
        // a function that returns the full id string of the first list element in the inner list
        private final Function<TOuter, String> innerIdStringExtractor;
        // a function that returns the outer with the relevant list cleared
        private final Function<TOuter, TOuter> listCleaner;

        @Override
        public void init(ProcessorContext<String, TOuter> context) {
            this.context = context;

            listStore = context.getStateStore(LIST_STORE);
        }

        /**
         * Process the given record similar to a flat map, however it keeps track of the elements returned by the flat
         * map operation. Should the same value be processed again and the output of the flatmap be different,
         * tombstones will be issued for any values removed from the list and any newly added values will be forwarded.
         * Values that occur both in the old and new list will not be forwarded as they are already included in the
         * downstream aggregate. This keeps the number of downstream join and aggregation operations to a minimum.
         * <p>
         * In addition to the flat mapped values, an "empty list" record will also be sent. It is a copy of the record,
         * except that the list (the one we are list joining on) is empty. This is needed to propagate update to fields
         * other than the list we are joining on.
         *
         * @param record the record to process
         */
        @Override
        public void process(Record<String, TOuter> record) {

            // in case of tombstone or empty list, this map is empty
            Map<String, TOuter> flatValuesMap = flatMapper.apply(record.value()).stream()
                    .collect(Collectors.toMap(
                            innerIdStringExtractor,
                            Function.identity(),
                            (_, newValue) -> newValue
                    ));

            Set<String> newIds = flatValuesMap.keySet();

            Set<String> oldIds = Optional.ofNullable(listStore.get(record.key()))
                    .map(Set::copyOf)
                    .orElse(Set.of());

            // if both new and old ids are empty we don't need to do anything and can short circuit
            if (newIds.isEmpty() && oldIds.isEmpty()) {
                return;
            }

            Sets.SetView<String> addedIds = Sets.difference(newIds, oldIds);
            for (String newId : addedIds) {
                forward(record.key(), newId, flatValuesMap.get(newId));
            }

            Sets.SetView<String> removedIds = Sets.difference(oldIds, newIds);
            for (String removedId : removedIds) {
                forward(record.key(), removedId, null);
            }

            // if the new list is empty delete the list from the store and send tombstone for empty list record
            // otherwise save new list and send empty list record
            if (newIds.isEmpty()) {
                forward(record.key(), null, null);
                listStore.put(record.key(), null);
            } else {
                forward(record.key(), null, listCleaner.apply(record.value()));
                listStore.put(record.key(), List.copyOf(newIds));
            }
        }

        /**
         * Forwards the given value out of the processor. The key of the resulting record will be a composite of the
         * form {@code "<outerKey>$$<innerKey>"} or just {@code "<outerKey>"} if {@code innerKey} is {@code null}.
         *
         * @param innerKey the key of the processed record
         * @param outerKey the key of the flat mapped element, may be null
         * @param value    the value to forward. May be null.
         */
        private void forward(@NotNull String outerKey, @Nullable String innerKey, TOuter value) {
            String key = outerKey;
            if (innerKey != null) {
                key = String.join("$$", key, innerKey);
            }
            context.forward(new Record<>(key, value, context.currentStreamTimeMs()));
        }
    }
}
