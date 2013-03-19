package edu.berkeley.thebes.hat.server.dependencies;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import edu.berkeley.thebes.common.data.DataItem;
import edu.berkeley.thebes.common.data.Version;
import edu.berkeley.thebes.common.persistence.IPersistenceEngine;
import edu.berkeley.thebes.common.thrift.ThriftDataItem;
import edu.berkeley.thebes.hat.server.antientropy.clustering.AntiEntropyServiceRouter;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import junit.framework.TestCase;

public class DependencyResolverTest extends TestCase {
    private static final short CLIENT_ID = 0;
    private AtomicLong logicalClock;
    
    private DependencyResolver resolver;
    private MockPersistenceEngine persistenceEngine;
    private MockRouter router;
    
    @Override
    public void setUp() {
        persistenceEngine = new MockPersistenceEngine();
        router = new MockRouter();
        resolver = new DependencyResolver(router, persistenceEngine);
        logicalClock = new AtomicLong(0);
    }
    
    public void testBasic() {
        Version xact1 = getTransactionId();
        router.expect("hello");
        resolver.addPendingWrite("hello",
                makeDataItem(xact1, "World!", "depKey1", "depKey2"));
        assertPending("hello", "World!", xact1);
        
        resolver.dependentWriteAcked("hello", "depKey1", xact1);
        assertPending("hello", "World!", xact1);
        
        resolver.dependentWriteAcked("hello", "depKey2", xact1);
        assertGood("hello", "World!", xact1);
    }
    
    public void testPrematureAck() {
        Version xact1 = getTransactionId();
        
        resolver.dependentWriteAcked("hello", "depKey1", xact1);

        router.expect("hello");
        resolver.addPendingWrite("hello",
                makeDataItem(xact1, "World!", "depKey1", "depKey2"));
        assertPending("hello", "World!", xact1);
        
        resolver.dependentWriteAcked("hello", "depKey2", xact1);
        assertGood("hello", "World!", xact1);
    }
    
    public void testPrematureAckAll() {
        Version xact1 = getTransactionId();
        
        resolver.dependentWriteAcked("hello", "depKey1", xact1);
        
        resolver.dependentWriteAcked("hello", "depKey2", xact1);

        router.expect("hello");
        resolver.addPendingWrite("hello",
                makeDataItem(xact1, "World!", "depKey1", "depKey2"));
        assertGood("hello", "World!", xact1);
    }
    
    public void testUnrelated() {
        Version xact1 = getTransactionId();
        Version xact2 = getTransactionId();
        router.expect("hello");
        resolver.addPendingWrite("hello",
                makeDataItem(xact1, "World!", "depKey1", "depKey2"));
        router.expect("other");
        resolver.addPendingWrite("other",
                makeDataItem(xact2, "value!", "depKey3", "depKey4"));
        assertPending("hello", "World!", xact1);
        assertPending("other", "value!", xact2);
        
        resolver.dependentWriteAcked("hello", "depKey2", xact1);
        assertPending("hello", "World!", xact1);
        assertPending("other", "value!", xact2);
        
        resolver.dependentWriteAcked("other", "depKey3", xact2);
        assertPending("hello", "World!", xact1);
        assertPending("other", "value!", xact2);
        
        resolver.dependentWriteAcked("other", "depKey4", xact2);
        assertPending("hello", "World!", xact1);
        assertGood("other", "value!", xact2);
        
        resolver.dependentWriteAcked("hello", "depKey1", xact1);
        assertGood("hello", "World!", xact1);
        assertGood("other", "value!", xact2);
    }
    
    public void testSameKey() {
        Version xact1 = getTransactionId();
        Version xact2 = getTransactionId();
        router.expect("hello");
        resolver.addPendingWrite("hello",
                makeDataItem(xact1, "World!", "depKey1", "depKey2"));
        router.expect("hello");
        resolver.addPendingWrite("hello",
                makeDataItem(xact2, "value!", "depKey3", "depKey4"));
        assertPending("hello", "World!", xact1);
        assertPending("hello", "value!", xact2);
        
        resolver.dependentWriteAcked("hello", "depKey2", xact1);
        assertPending("hello", "World!", xact1);
        assertPending("hello", "value!", xact2);
        
        resolver.dependentWriteAcked("hello", "depKey3", xact2);
        assertPending("hello", "World!", xact1);
        assertPending("hello", "value!", xact2);
        
        resolver.dependentWriteAcked("hello", "depKey4", xact2);
        assertPending("hello", "World!", xact1);
        assertGood("hello", "value!", xact2);
        
        resolver.dependentWriteAcked("hello", "depKey1", xact1);
        assertGood("hello", "value!", xact1);
        assertGood("hello", "value!", xact2);
    }
    
    public void testSameKeyPrematureAcks() {
        Version xact1 = getTransactionId();
        Version xact2 = getTransactionId();
        
        resolver.dependentWriteAcked("hello", "depKey2", xact1);
        
        resolver.dependentWriteAcked("hello", "depKey3", xact2);

        router.expect("hello");
        resolver.addPendingWrite("hello",
                makeDataItem(xact1, "World!", "depKey1", "depKey2"));
        router.expect("hello");
        resolver.addPendingWrite("hello",
                makeDataItem(xact2, "value!", "depKey3", "depKey4"));
        assertPending("hello", "World!", xact1);
        assertPending("hello", "value!", xact2);
        
        resolver.dependentWriteAcked("hello", "depKey4", xact2);
        assertPending("hello", "World!", xact1);
        assertGood("hello", "value!", xact2);
        
        resolver.dependentWriteAcked("hello", "depKey1", xact1);
        assertGood("hello", "value!", xact1);
        assertGood("hello", "value!", xact2);
    }
    
    private void assertPending(String key, String value, Version version) {
        if (resolver.retrievePendingItem(key, version) != null) {
            assertEquals(ByteBuffer.wrap(value.getBytes()),
                    resolver.retrievePendingItem(key, version).getData());
        } else {
            fail("Not in pending!");
        }
        
        if (persistenceEngine.get(key) != null) {
            assertFalse(ByteBuffer.wrap(value.getBytes()).equals(persistenceEngine.get(key).getData()));
        }
    }
    
    private void assertGood(String key, String value, Version version) {
        assertNull(resolver.retrievePendingItem(key, version));
        assertEquals(ByteBuffer.wrap(value.getBytes()), persistenceEngine.get(key).getData());
    }
    
    private DataItem makeDataItem(Version xact, String value, String ... transactionKeys) {
        DataItem di = new DataItem(ByteBuffer.wrap(value.getBytes()), xact);
        di.setTransactionKeys(Lists.newArrayList(transactionKeys));
        return di;
    }
    
    private Version getTransactionId() {
        return new Version(CLIENT_ID, logicalClock.incrementAndGet(),
                System.currentTimeMillis());
    }
    
    
    private static class MockRouter extends AntiEntropyServiceRouter {
        private String expectAnnounceKey;
        public void expect(String key) {
            assertNull(expectAnnounceKey);
            expectAnnounceKey = key;
        }
        
        @Override
        public void sendWriteToSiblings(String key, ThriftDataItem value) {

        }
        
        @Override
        public void announcePendingWrite(PendingWrite write) {
            assertEquals(write.getKey(), expectAnnounceKey);
            expectAnnounceKey = null;
        }
    }
    
    private static class MockPersistenceEngine implements IPersistenceEngine {
        private final Map<String, DataItem> data = Maps.newHashMap();
        @Override
        public boolean put(String key, DataItem value) {
            if (!data.containsKey(key)) {
                data.put(key, value);
            } else if (data.get(key).getVersion().compareTo(value.getVersion()) <= 0) {
                data.put(key, value);
            }
            
            return true;
        }

        @Override
        public DataItem get(String key) {
            return data.get(key);
        }

        @Override
        public void open() {}
        @Override
        public void close() {}        
    }
}
