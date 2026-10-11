package fi.aalto.cse4780.viz;

/** One browser connection, as the {@link Broadcaster} sees it. */
interface Client {

    /** Queues a text message without blocking. */
    void send(String text);

    /** Characters queued but not yet written to the socket. */
    long pendingChars();

    void close(String reason);
}
