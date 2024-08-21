import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;

import java.util.*;
import java.util.stream.Collectors;

public class ThrottleFilter extends Filter<ILoggingEvent> {
    private final Map<String, Long> messageTimestampMap = new HashMap<>();
    private long throttleTimeMillis;
    private int removalSize;
    private List<String> mdcKeys = new ArrayList<>();

    public ThrottleFilter() {
    }

    @Override
    public FilterReply decide(ILoggingEvent event) {
        if (isNotErrorLevel(event)) {
            return FilterReply.DENY;
        }

        if (isRemovableSizeExceeded()) {
            removeOldLogs();
        }

        String message = generateMessage(event);
        long currentTime = System.currentTimeMillis();

        if (isDenyLog(message, currentTime)) {
            return FilterReply.DENY;
        }

        messageTimestampMap.put(message, currentTime);
        return FilterReply.ACCEPT;
    }

    private boolean isNotErrorLevel(ILoggingEvent event) {
        return !event.getLevel().equals(Level.ERROR);
    }

    private void removeOldLogs() {
        Iterator<String> iterator = messageTimestampMap.keySet().iterator();
        long currentTime = System.currentTimeMillis();

        while (iterator.hasNext()) {
            long elapsedTime = calculateElapsedTime(iterator.next(), currentTime);

            if (elapsedTime >= throttleTimeMillis) {
                iterator.remove();
            }
        }
    }

    private boolean isRemovableSizeExceeded() {
        return messageTimestampMap.size() >= removalSize;
    }

    private String generateMessage(ILoggingEvent event) {
        return mdcKeys.stream()
                .map(key -> event.getMDCPropertyMap().get(key))
                .collect(Collectors.joining());
    }

    private boolean isDenyLog(String message, long currentTime) {
        if (doesNotContainMessage(message)) {
            return false;
        }

        long elapsedTime = calculateElapsedTime(message, currentTime);
        if (elapsedTime <= throttleTimeMillis) {
            messageTimestampMap.put(message, currentTime);
            return true;
        }

        return false;
    }

    private boolean doesNotContainMessage(String message) {
        return !messageTimestampMap.containsKey(message);
    }

    private long calculateElapsedTime(String message, long currentTime) {
        long lastTime = messageTimestampMap.get(message);
        return currentTime - lastTime;
    }

    public void setThrottleTimeMillis(long throttleTimeMillis) {
        this.throttleTimeMillis = throttleTimeMillis;
    }

    public void setRemovalSize(int removalSize) {
        this.removalSize = removalSize;
    }

    public void setMdcKeys(String mdcKeys) {
        this.mdcKeys = List.of(mdcKeys.split(","));
    }
}
