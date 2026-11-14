package com.agentflow.worker.media;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** 媒体服务(agentflow-media,Python/FastAPI,跑在 GPU 宿主机上)的连接配置。 */
@Getter
@Setter
@ConfigurationProperties("media")
public class MediaProperties {
    private String baseUrl = "http://localhost:8010";
    /** 转写和截帧是同步的长请求(一讲几分钟),超时要比它们长。 */
    private int timeoutMs = 1_200_000;
}
