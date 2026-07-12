package com.agentflow.server.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.transaction.annotation.Transactional;

import static org.hamcrest.Matchers.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@AutoConfigureMockMvc
@Transactional
class TaskControllerTest {

    @Autowired
    private MockMvc mockMvc;
    @Autowired
    private ObjectMapper om;
    @MockBean
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Test
    void submitThenQueryRoundTrip() throws Exception {
        String body = "{\"type\":\"ECHO_BATCH\",\"payload\":{\"items\":[\"a\",\"b\"]}}";
        String resp = mockMvc.perform(post("/api/v1/tasks")
                        .contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.taskUuid", notNullValue()))
                .andReturn().getResponse().getContentAsString();
        String uuid = om.readTree(resp).get("taskUuid").asText();

        mockMvc.perform(get("/api/v1/tasks/" + uuid))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.taskUuid", is(uuid)))
                .andExpect(jsonPath("$.type", is("ECHO_BATCH")))
                .andExpect(jsonPath("$.progress.total", is(2)))
                .andExpect(jsonPath("$.progress.done", is(0)))
                .andExpect(jsonPath("$.subtasks", hasSize(2)));
    }

    @Test
    void unknownUuidReturns404() throws Exception {
        mockMvc.perform(get("/api/v1/tasks/no-such-uuid"))
                .andExpect(status().isNotFound());
    }

    @Test
    void unknownTypeReturns400() throws Exception {
        mockMvc.perform(post("/api/v1/tasks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"type\":\"NOPE\",\"payload\":{}}"))
                .andExpect(status().isBadRequest());
    }
}
