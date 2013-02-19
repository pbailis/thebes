package edu.berkeley.thebes.common.thrift;

public class ThriftUtil {
    public enum VersionCompare { EARLIER, LATER, EQUAL };

    public static final Version NullVersion = new Version((short) -1, -1);

    public static VersionCompare compareVersions(Version current, Version later) {
        if(current.getTimestamp() < later.getTimestamp())
            return VersionCompare.EARLIER;
        else if(current.getTimestamp() == later.getTimestamp() &&
                current.getClientID() < later.getClientID())
            return VersionCompare.EARLIER;
        else if(current.getTimestamp() == later.getTimestamp() &&
                current.getClientID() == later.getClientID())
            return VersionCompare.EQUAL;
        else
            return VersionCompare.LATER;
    }
}