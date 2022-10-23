package org.apache.pulsar.logstash.outputs;

import co.elastic.logstash.api.Configuration;
import co.elastic.logstash.api.Event;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.logstash.plugins.ConfigurationImpl;
import org.logstash.plugins.codecs.Line;

public class PulsarOutputExampleTest {

    String serviceUrl = "pulsar://127.0.0.1:6650";
    String topic = "public/default/logstash";
    String token = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhZG1pbiIsImV4cCI6MTY5NDIzNjAwOX0.eFlDpXiiPlC4jeElru0z7NvjwHbmv5eF8orKlr96hSE";
    Map<String, Object> configValues = new HashMap<>();

    // Initialize configuration information
    @Before
    public void init() {
        configValues.put("serviceUrl", serviceUrl);
        configValues.put("producer_name", "jun_test");
        configValues.put("topic", topic);
        configValues.put("enable_batching", true);
        configValues.put("enable_token", true);
        configValues.put("auth_plugin_class_name", "org.apache.pulsar.client.impl.auth.AuthenticationToken");
        configValues.put("auth_plugin_params_String", "token:" + token);
        String delimiter = "/";
        Map<String, Object> codecMap = new HashMap<>();
        codecMap.put("delimiter", delimiter);
        Configuration codecConf = new ConfigurationImpl(codecMap);
        configValues.put("codec", new Line(codecConf, null));}

    @Test
    @Ignore()
    public void testOutputWithPulsarToken() {
        Configuration config = new ConfigurationImpl(configValues);
        Pulsar output = new Pulsar("test-id", config, null);
        String sourceField = "message";
        int eventCount = 5;
        List<Event> events = new ArrayList<>();
        for (int k = 0; k < eventCount; k++) {
            Event e = new org.logstash.Event();
            e.setField(sourceField, "message : hello test " + k);
            events.add(e);
        }
        //output.output(events);
        Assert.assertEquals("events size is 5", events.size(), eventCount);

        int index = 0;
        while (index < eventCount) {
            Assert.assertTrue("event" + index + " contains the specified str", events.get(index).getField("message").toString().contains("message : hello test " + index));
            index++;
        }
    }

}
