package com.agentflow.server.course;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** 默认配置下课程接口不存在:公开 demo 用的也是这份代码。 */
@SpringBootTest
@AutoConfigureMockMvc
class CourseApiDisabledTest {

    @Autowired private MockMvc mvc;
    @MockBean private KafkaTemplate<String, Object> kafkaTemplate;

    @Test
    void courseRoutesDoNotExistUnlessSwitchedOn() throws Exception {
        mvc.perform(post("/api/v1/courses").contentType(MediaType.APPLICATION_JSON).content("{\"name\":\"x\"}"))
                .andExpect(status().isNotFound());
        mvc.perform(get("/api/v1/courses")).andExpect(status().isNotFound());
        mvc.perform(post("/api/v1/lectures/1/retry")).andExpect(status().isNotFound());
    }
}
