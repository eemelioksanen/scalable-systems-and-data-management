import org.slf4j.LoggerFactory;

public class LoggingCheck {
    public static void main(String[] args) {
        String factory = LoggerFactory.getILoggerFactory().getClass().getName();
        if (!factory.equals("org.slf4j.simple.SimpleLoggerFactory")) {
            throw new IllegalStateException("Producer logging is unavailable: " + factory);
        }
        LoggerFactory.getLogger(LoggingCheck.class).info("Producer logging works");
    }
}
