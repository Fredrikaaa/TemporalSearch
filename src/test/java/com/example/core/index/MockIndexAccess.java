package com.example.core.index;

// Note: Does NOT implement IndexAccess directly to avoid complex mocking/inheritance
// It provides a compatible API for testing purposes where an IndexAccess object is expected.
// import com.example.core.IndexAccess; 
import com.example.core.IndexAccessException;
import com.example.core.Position;
import com.example.core.PositionList;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.WriteBatch;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

// Implement the interface
import com.example.core.IndexAccessInterface; 

/**
 * A mock implementation providing an IndexAccess-like API for testing purposes.
 * Implements IndexAccessInterface.
 * Stores data in an in-memory sorted map.
 */
public class MockIndexAccess implements IndexAccessInterface {

    private final String indexType;
    private final NavigableMap<ByteArrayWrapper, byte[]> dataStore;
    private boolean closed = false;

    public MockIndexAccess(String indexType) {
        this.indexType = indexType;
        // Use ConcurrentSkipListMap for thread safety and sorting (like LevelDB)
        this.dataStore = new ConcurrentSkipListMap<>();
    }

    // Convenience constructor for common "unigram" type
    public MockIndexAccess() {
        this("unigram");
    }

    /**
     * Helper method to add test data.
     * Converts the string key to bytes and creates/serializes a PositionList.
     */
    public void addTestData(String key, int docId, int sentenceId, int begin, int end) {
        if (closed) throw new IllegalStateException("Index is closed");
        ByteArrayWrapper wrappedKey = new ByteArrayWrapper(key.getBytes(StandardCharsets.UTF_8));
        Position pos = new Position(docId, sentenceId, begin, end, LocalDate.now());

        // Retrieve existing list or create a new one
        byte[] existingData = dataStore.get(wrappedKey);
        PositionList list;
        if (existingData != null) {
            try {
                 // Note: Avoid using get() inside addTestData to avoid infinite recursion if get() throws
                 list = PositionList.deserialize(existingData);
            } catch (RuntimeException e) {
                // Handle potential deserialization error for existing data
                System.err.println("Error deserializing existing PositionList for key '" + key + "'. Starting fresh.");
                list = new PositionList();
            }
        } else {
            list = new PositionList();
        }

        // Add the new position and store the updated list
        list.add(pos);
        dataStore.put(wrappedKey, list.serialize());
    }

    /**
     * Helper method to add pre-serialized test data.
     */
    public void addTestData(String key, PositionList positionList) {
        if (closed) throw new IllegalStateException("Index is closed");
        ByteArrayWrapper wrappedKey = new ByteArrayWrapper(key.getBytes(StandardCharsets.UTF_8));
        dataStore.put(wrappedKey, positionList.serialize());
    }
    
     /**
     * Helper method to add pre-serialized test data with byte key.
     */
    public void addTestData(byte[] key, byte[] value) {
        if (closed) throw new IllegalStateException("Index is closed");
        dataStore.put(new ByteArrayWrapper(key), value);
    }

    @Override
    public Optional<PositionList> get(byte[] key) throws IndexAccessException {
        if (closed) throw new IndexAccessException("Index is closed", indexType, IndexAccessException.ErrorType.RESOURCE_ERROR);
        byte[] value = dataStore.get(new ByteArrayWrapper(key));
        if (value == null) {
            return Optional.empty();
        }
        try {
            return Optional.of(PositionList.deserialize(value));
        } catch (RuntimeException e) { // Catch potential deserialization errors
            throw new IndexAccessException(
                "Failed to deserialize PositionList for key", 
                indexType, 
                IndexAccessException.ErrorType.READ_ERROR, // Use READ_ERROR
                e // Pass the original exception as the cause
            ); 
        }
    }

    @Override
    public Optional<byte[]> getRaw(byte[] key) throws IndexAccessException {
        if (closed) throw new IndexAccessException("Index is closed", indexType, IndexAccessException.ErrorType.RESOURCE_ERROR);
        return Optional.ofNullable(dataStore.get(new ByteArrayWrapper(key)));
    }

    @Override
    public DBIterator iterator() throws IndexAccessException {
        if (closed) throw new IndexAccessException("Index is closed", indexType, IndexAccessException.ErrorType.RESOURCE_ERROR);
        // Return a simple iterator over the current snapshot of the map
        return new MockDBIterator(dataStore);
    }

    @Override
    public DBIterator iterator(ReadOptions options) throws IndexAccessException {
         if (closed) throw new IndexAccessException("Index is closed", indexType, IndexAccessException.ErrorType.RESOURCE_ERROR);
        // Ignore ReadOptions for this mock
        return iterator();
    }

    @Override
    public void put(byte[] key, byte[] value) throws IndexAccessException {
        if (closed) throw new IndexAccessException("Index is closed", indexType, IndexAccessException.ErrorType.RESOURCE_ERROR);
        dataStore.put(new ByteArrayWrapper(key), value);
    }

    @Override
    public void delete(byte[] key) throws IndexAccessException {
        if (closed) throw new IndexAccessException("Index is closed", indexType, IndexAccessException.ErrorType.RESOURCE_ERROR);
        dataStore.remove(new ByteArrayWrapper(key));
    }

    @Override
    public WriteBatch createWriteBatch() throws IndexAccessException {
        if (closed) throw new IndexAccessException("Index is closed", indexType, IndexAccessException.ErrorType.RESOURCE_ERROR);
        // WriteBatch operations are complex to mock properly, throw unsupported for now
        throw new UnsupportedOperationException("WriteBatch not supported by MockIndexAccess");
    }

    @Override
    public void write(WriteBatch batch) throws IndexAccessException {
        if (closed) throw new IndexAccessException("Index is closed", indexType, IndexAccessException.ErrorType.RESOURCE_ERROR);
        throw new UnsupportedOperationException("WriteBatch not supported by MockIndexAccess");
    }

    @Override
    public String getIndexType() {
        return indexType;
    }

    @Override
    public boolean isOpen() {
        return !closed;
    }

    @Override
    public void close() throws Exception {
        if (!closed) {
            closed = true;
            dataStore.clear(); // Clear data on close
            System.out.println("MockIndexAccess [" + indexType + "] closed.");
        }
    }

    // --- Inner Mock Iterator Class ---

    private static class MockDBIterator implements DBIterator {
        private final Iterator<Map.Entry<ByteArrayWrapper, byte[]>> iterator;
        private Map.Entry<ByteArrayWrapper, byte[]> currentEntry;

        MockDBIterator(NavigableMap<ByteArrayWrapper, byte[]> map) {
            // Create an iterator over a defensive copy to avoid ConcurrentModificationException
            this.iterator = new TreeMap<>(map).entrySet().iterator(); 
            this.currentEntry = null;
        }

        @Override
        public void seek(byte[] key) {
             throw new UnsupportedOperationException("seek not fully implemented in MockDBIterator");
             // For a full implementation, would need to find the correct starting point
        }

        @Override
        public void seekToFirst() {
            // Reset the iterator (requires recreating it over a copy)
            // This simple version doesn't fully support seeking after iteration started.
            // Re-implement if complex iteration/seeking is needed for tests.
             throw new UnsupportedOperationException("seekToFirst not fully implemented in MockDBIterator after iteration starts");
        }

        @Override
        public void seekToLast() {
            throw new UnsupportedOperationException("seekToLast not implemented in MockDBIterator");
        }

        @Override
        public Map.Entry<byte[], byte[]> peekNext() {
             if (!hasNext()) {
                throw new NoSuchElementException();
            }
            // This requires storing the next element and making hasNext/next idempotent
             throw new UnsupportedOperationException("peekNext not implemented in MockDBIterator");
        }
        
        @Override
        public boolean hasNext() {
            // If currentEntry is used by peekNext, check it first
            return iterator.hasNext();
        }

        @Override
        public Map.Entry<byte[], byte[]> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            currentEntry = iterator.next();
            // Convert ByteArrayWrapper back to byte[] for the key
            return Map.entry(currentEntry.getKey().getData(), currentEntry.getValue());
        }
        
        @Override
        public Map.Entry<byte[], byte[]> peekPrev() {
             throw new UnsupportedOperationException("peekPrev not implemented in MockDBIterator");
        }

        @Override
        public boolean hasPrev() {
            throw new UnsupportedOperationException("hasPrev not implemented in MockDBIterator");
        }

        @Override
        public Map.Entry<byte[], byte[]> prev() {
            throw new UnsupportedOperationException("prev not implemented in MockDBIterator");
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove not supported");
        }

        @Override
        public void close() throws IOException {
            // No resources to close for this simple mock iterator
        }
    }
} 