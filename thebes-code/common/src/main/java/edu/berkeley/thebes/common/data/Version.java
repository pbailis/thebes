package edu.berkeley.thebes.common.data;

import com.google.common.base.Objects;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;

import edu.berkeley.thebes.common.thrift.ThriftVersion;

public class Version implements Comparable<Version> {
    public static final Version NULL_VERSION = new Version((short) -1, -1);
    
	private final short clientID;
	private final long timestamp;
	
	public Version(short clientID, long timestamp) {
		this.clientID = clientID;
		this.timestamp = timestamp;
	}
	
	public static Version fromThrift(ThriftVersion version) {
		return new Version(version.getClientID(), version.getTimestamp());
	}
	
	public static ThriftVersion toThrift(Version version) {
		return new ThriftVersion(version.getClientID(), version.getTimestamp());
	}
	
	public short getClientID() {
		return clientID;
	}

	public long getTimestamp() {
		return timestamp;
	}

	@Override
	public int compareTo(Version other) {
		int timestampCompare = Longs.compare(timestamp, other.getTimestamp());
		int clientIdCompare = Shorts.compare(clientID, other.getClientID());
		return timestampCompare != 0 ? timestampCompare : clientIdCompare; 
	}
	
	@Override
	public int hashCode() {
		return Objects.hashCode(clientID, timestamp);
	}
	
	@Override
	public boolean equals(Object other) {
		if (! (other instanceof Version)) {
			return false;
		}
		
		Version v = (Version) other;
		return Objects.equal(getClientID(), v.getClientID()) &&
				Objects.equal(getTimestamp(), v.getTimestamp());
	}
}
