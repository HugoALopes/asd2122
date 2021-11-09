package protocols.storage;

import java.sql.Timestamp;
import java.time.LocalDateTime;

public class CacheContent {
    private byte[] content;
    private String name;
    private LocalDateTime time;

    public CacheContent(LocalDateTime time, byte[] content) {
        this.content = content;
        this.time = time;
    }

    public byte[] getContent() {
        return content;
    }

    public LocalDateTime getTime() {
        return time;
    }
}
