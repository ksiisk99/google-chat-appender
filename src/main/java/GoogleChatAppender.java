import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Layout;
import ch.qos.logback.core.LayoutBase;
import ch.qos.logback.core.UnsynchronizedAppenderBase;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

public class GoogleChatAppender extends UnsynchronizedAppenderBase<ILoggingEvent> {
    private static final int HTTP_SUCCESS_STATUS = 200;
    private static final String ACCEPT_HEADER_KEY = "accept";
    private static final String ACCEPT_HEADER_VALUE = "application/json";
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final BlockingDeque<ILoggingEvent> queue = new LinkedBlockingDeque<>();
    private static final HttpClient httpClient = HttpClient.newHttpClient();

    private volatile boolean interruptedFlag;

    private String webhookUri;
    private Layout<ILoggingEvent> layout;
    private Logger logger;
    private String errorLoggerName;

    public GoogleChatAppender() {
        layout = new LayoutBase<>() {
            @Override
            public String doLayout(ILoggingEvent event) {
                return "-- [" + event.getLevel() + "]" +
                        event.getLoggerName() + " - " +
                        event.getFormattedMessage().replaceAll("\n", "\n\t");
            }
        };

        Thread workerThread = createWorkerThread();

        createShutdownHook(workerThread);
    }

    private Thread createWorkerThread() {
        Thread workerThread = new Thread(() -> {
            while (true) {
                try {
                    sendMessage(queue.take());
                } catch (InterruptedException e) {
                    interruptedFlag = true;
                } catch (Exception e) {
                    recordLog("Error send message to GoogleChat", e);
                    break;
                }

                if (isShutdown()) {
                    break;
                }
            }
        }, "google-chat-appender");
        workerThread.setDaemon(true);
        workerThread.start();
        return workerThread;
    }

    private void createShutdownHook(Thread workerThread) {
        Thread hookThread = new Thread(() -> {
            interruptedFlag = true;
            workerThread.interrupt();

            try {
                workerThread.join();
            } catch (Exception e) {
                recordLog("Error shutdown hook", e);
            }
        }, "shutdown-hook-thread");

        Runtime.getRuntime().addShutdownHook(hookThread);
    }

    private boolean isShutdown() {
        return queue.isEmpty() && interruptedFlag;
    }

    @Override
    public void start() {
        if (errorLoggerName != null) {
            LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
            logger = loggerContext.getLogger(errorLoggerName);
        }
        super.start();
    }

    @Override
    protected void append(ILoggingEvent evt) {
        try {
            if (isValidWebhookUri()) {
                queue.addLast(evt);
            }
        } catch (Exception e) {
            recordLog("Error send message to GoogleChat", e);
        }
    }

    private boolean isValidWebhookUri() {
        return webhookUri != null && !webhookUri.isBlank();
    }

    private void recordLog(String message, Exception e) {
        if (logger == null) {
            addError(message, e);
            return;
        }
        logger.error(message, e);
    }

    private void sendMessage(ILoggingEvent evt) throws Exception {
        String message = layout.doLayout(evt);
        String requestBody = createJsonPayload(message);
        HttpRequest httpRequest = createHttpRequest(requestBody);
        HttpResponse<String> response = doSend(httpRequest);
        verifyResponse(response);
    }

    private String createJsonPayload(String text) throws JsonProcessingException {
        ObjectNode jsonNode = objectMapper.createObjectNode();
        jsonNode.put("text", text);
        return objectMapper.writeValueAsString(jsonNode);
    }

    private HttpRequest createHttpRequest(String requestBody) throws URISyntaxException {
        return HttpRequest.newBuilder()
                .uri(new URI(webhookUri))
                .header(ACCEPT_HEADER_KEY, ACCEPT_HEADER_VALUE)
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();
    }

    private HttpResponse<String> doSend(HttpRequest httpRequest) throws IOException, InterruptedException {
        return httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
    }

    private void verifyResponse(HttpResponse<String> response) {
        if (isNotSuccessSendLog(response)) {
            throw new RuntimeException("Fail send to GoogleChat: " + response.body());
        }
    }

    private boolean isNotSuccessSendLog(HttpResponse<String> response) {
        return response.statusCode() != HTTP_SUCCESS_STATUS;
    }

    public void setWebhookUri(String webhookUri) {
        this.webhookUri = webhookUri;
    }

    public void setLayout(Layout<ILoggingEvent> layout) {
        this.layout = layout;
    }

    public void setErrorLoggerName(String errorLoggerName) {
        this.errorLoggerName = errorLoggerName;
    }
}

