package com.agentflow.worker.processor;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class EchoProcessorTest {

    private final EchoProcessor processor = new EchoProcessor();

    @Test
    void echoesUppercasedTextWithLength() throws Exception {
        String out = processor.process("{\"text\":\"hello\"}");
        assertEquals("{\"echo\":\"HELLO\",\"length\":5}", out);
    }

    @Test
    void rejectsMissingText() {
        assertThrows(IllegalArgumentException.class, () -> processor.process("{}"));
    }
}
