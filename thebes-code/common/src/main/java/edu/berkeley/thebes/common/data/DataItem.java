package edu.berkeley.thebes.common.data;

import java.nio.ByteBuffer;
import java.util.List;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;

import edu.berkeley.thebes.common.thrift.ThriftDataItem;

public class DataItem {
	private final ByteBuffer data;
	private Version version;
	private List<String> transactionKeys;

	public DataItem(byte[] data, Version version, List<String> transactionKeys) {
		this.data = ByteBuffer.wrap(data);
		this.version = version;
		this.transactionKeys = transactionKeys;
	}
	
	public DataItem(ByteBuffer data, Version version) {
		this.data = data;
		this.version = version;
		this.transactionKeys = Lists.newArrayList();
	}
	
	public static DataItem fromThrift(ThriftDataItem dataItem) {
		if (dataItem == null) {
			return null;
		}
		
		return new DataItem(
				dataItem.getData(), 
				Version.fromThrift(dataItem.getVersion()),
				dataItem.getTransactionKeys());
	}
	
	public static ThriftDataItem toThrift(DataItem dataItem) {
		if (dataItem == null) {
			return null;
		}
		
		ThriftDataItem thriftDI = new ThriftDataItem(dataItem.getData(),
				Version.toThrift(dataItem.getVersion()));
		if (dataItem.getTransactionKeys() != null) {
			thriftDI.setTransactionKeys(dataItem.getTransactionKeys());
		}
		return thriftDI;
	}

	public ByteBuffer getData() {
		return data;
	}

	public Version getVersion() {
		return version;
	}

    public Version setVersion(Version newVersion) {
        version = newVersion;
        return version;
    }

	public List<String> getTransactionKeys() {
		return transactionKeys;
	}

	public void setTransactionKeys(List<String> transactionKeys) {
		this.transactionKeys = transactionKeys;
	}
	
	@Override
	public int hashCode() {
		return Objects.hashCode(data, version, transactionKeys);
	}

	@Override
	public boolean equals(Object other) {
		if (! (other instanceof DataItem)) {
			return false;
		}
		
		DataItem di = (DataItem) other;
		return Objects.equal(getData(), di.getData()) &&
				Objects.equal(getVersion(), di.getVersion()) &&
				Objects.equal(getTransactionKeys(), di.getTransactionKeys());
	}
}
