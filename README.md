# Logstash Output Pulsar Plugin

This is a Java plugin for [Logstash](https://github.com/elastic/logstash).

It is fully free and fully open source. The license is Apache 2.0, meaning you are free to use it however you want.

Write events to a pulsar topic.

This plugin uses Pulsar Client 2.9.0. For broker compatibility, see the official Pulsar compatibility reference. If the compatibility wiki is not up-to-date, please contact Pulsar support/community to confirm compatibility.

If you require features not yet available in this plugin (including client version upgrades), please file an issue with details about what you need.

# Pulsar Output Configuration Options
This plugin supports these configuration options. 

| Settings    | Output type     | Required  |
| ------------- |:-------------:| -----:|
| serviceUrl      | string | No |
| topic      | string | Yes |
| compression_type      | string, one of["NONE","LZ4","ZLIB","ZSTD","SNAPPY"] | No |
| block_if_queue_full | bool, default is true | No|
| enable_batching | bool, default is true | No |


# Example

```
output{
  pulsar{
    topic => "persistent://public/default/%{topic_name}"
    serviceUrl => "pulsar://127.0.0.1:6650"
    enable_batching => true
  }
}
```


# Installation

1. Get the latest zip file from release page.
https://github.com/streamnative/logstash-output-pulsar/releases

2. Install this plugin using logstash preoffline command.

```
bin/logstash-plugin install file://{PATH_TO}/logstash-output-pulsar-2.7.1.zip
```
