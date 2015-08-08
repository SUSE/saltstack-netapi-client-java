package com.suse.saltstack.netapi.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Scanner;

/**
 * ClientUtils class unit-tests.
 */
public class ClientUtilsTest {

    @Test
    public void testCloseQuietly() throws IOException {
        /**
         * Mocked InputStream that throws IOException when closed more than once.
         */
        class MockedInputStream extends InputStream {
            private boolean closed = false;

            boolean isClosed() {
                return closed;
            }

            @Override
            public int read() throws IOException {
                return 0;
            }

            @Override
            public void close() throws IOException {
                if (closed) {
                    throw new IOException("Stream already closed");
                }
                closed = true;
            }
        }

        // Close valid stream
        MockedInputStream is = new MockedInputStream();
        ClientUtils.closeQuietly(is);
        assertTrue(is.isClosed());
    }

    @Test
    public void testStringToStream() {
        final String TEST_STRING = "SUSE";
        InputStream is = ClientUtils.stringToStream(TEST_STRING);
        try (Scanner scanner = new Scanner(is)) {
            String result = scanner.nextLine();
            assertEquals("Result doesn't match test string", TEST_STRING, result);
        }
    }

    @Test
    public void testStreamToString() {
        final String TEST_STRING = "SUSE";
        String result = ClientUtils.streamToString(
                new ByteArrayInputStream(TEST_STRING.getBytes()));
        assertEquals("Result doesn't match test string", TEST_STRING, result);
    }

}
