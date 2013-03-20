package edu.berkeley.thebes.hat.server.dependencies;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Sets;

import edu.berkeley.thebes.common.data.DataItem;
import edu.berkeley.thebes.common.data.Version;

import java.util.Set;

public class PendingWrite implements Comparable<PendingWrite> {
    public static interface WriteReadyCallback {
        /** Called when all dependencies have been ACK'd. */
        void writeReady(PendingWrite write);
    }
    
    private final String key;
    private final DataItem value;
    private final Version transactionVersion;
    private final Set<String> pendingAckedKeys;
    private int numUnnamedAcks; // TODO: Remove either this code back or the one above.
    private final WriteReadyCallback callback;
    
    public PendingWrite(String key, DataItem value, WriteReadyCallback callback) {
        if (value.getTransactionKeys().isEmpty()) {
            throw new IllegalStateException("Pending writes must be waiting on at least one other key.");
        }
        
        this.key = key;
        this.value = value;
        this.transactionVersion = value.getVersion();
        this.pendingAckedKeys = Sets.newHashSet(value.getTransactionKeys());
        this.callback = callback;
        this.numUnnamedAcks = 0;
        
    }

    public boolean isWaitingFor(String ackedKey, Version version) {
        return version.equals(transactionVersion) &&
                (ackedKey == null || pendingAckedKeys.contains(ackedKey));
    }

    public void keyAcked(String ackedKey) {
        if (ackedKey != null) {
            pendingAckedKeys.remove(ackedKey);
        } else {
            numUnnamedAcks ++;
        }
        
        if (numUnnamedAcks == pendingAckedKeys.size() || pendingAckedKeys.isEmpty()) {
            callback.writeReady(this);
        }
    }

    public String getKey() {
        return key;
    }

    public DataItem getValue() {
        return value;
    }

    public Version getVersion() {
        return transactionVersion;
    }

    @Override
    public int compareTo(PendingWrite o) {
        return ComparisonChain.start()
                .compare(getKey(), o.getKey())
                .compare(getValue(),  o.getValue())
                .result();
    }
}