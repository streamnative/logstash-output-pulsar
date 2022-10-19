package org.apache.pulsar.logstash.outputs;

import co.elastic.logstash.api.Configuration;
import co.elastic.logstash.api.Event;
import org.junit.Before;
import org.junit.Test;
import org.logstash.plugins.ConfigurationImpl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.logstash.plugins.codecs.Line;


public class PulsarOutputExampleTest {

    // broker
    String serviceUrl = "pulsar://127.0.0.1:6650";
    // topic
    String topic = "test";

    String token = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhZG1pbiIsImV4cCI6MTY5NDIzNjAwOX0.eFlDpXiiPlC4jeElru0z7NvjwHbmv5eF8orKlr96hSE";

    Map<String, Object> configValues = new HashMap<>();

    // 初始化配置信息
    @Before
    public void init(){
        configValues.put("serviceUrl", serviceUrl);
        configValues.put("topic", topic);
        configValues.put("enable_batching", true);
        configValues.put("enable_token", true);
        configValues.put("auth_plugin_class_name", "org.apache.pulsar.client.impl.auth.AuthenticationToken");
        configValues.put("auth_plugin_params_String", "token:"+token);
        configValues.put(Pulsar.CONFIG_CODEC.name(), "java-line");
    }

    @Test
    public void testJavaOutputExample()  {
        Configuration config = new ConfigurationImpl(configValues);
        Pulsar output = new Pulsar("test-id", config, null);

        String sourceField = "message";
        int eventCount = 5;
        Collection<Event> events = new ArrayList<>();
        for (int k = 0; k < eventCount; k++) {
            Event e = new org.logstash.Event();
            e.setField(sourceField, "message " + k);
            events.add(e);
        }

        System.out.println(events);
        //output.output(events);
    }

}
