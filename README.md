# Logstash Output Pulsar Plugin

This is a Java plugin for [Logstash](https://github.com/elastic/logstash).

It is fully free and fully open source. The license is Apache 2.0, meaning you are free to use it however you want.

Write events to a pulsar topic.

This plugin uses Pulsar Client 2.9.0. For broker compatibility, see the official Pulsar compatibility reference. If the compatibility wiki is not up-to-date, please contact Pulsar support/community to confirm compatibility.

If you require features not yet available in this plugin (including client version upgrades), please file an issue with details about what you need.

# Pulsar Output Configuration Options
This plugin supports these configuration options. 

| Settings                          |                                  Output type                                  |   Required |
|-----------------------------------|:-----------------------------------------------------------------------------:|-----------:|
| serviceUrl                        |                                    string                                     |         No |
| topic                             |                                    string                                     |        Yes |
| producer_name                     |                                    string                                     |        Yes |
| compression_type                  |              string, one of["NONE","LZ4","ZLIB","ZSTD","SNAPPY"]              |         No |
| block_if_queue_full               |                             bool, default is true                             |         No |
| enable_batching                   |                             bool, default is true                             |         No |
| enable_tls                        |                boolean, one of [true, false]. default is false                |         No |
| tls_trust_store_path              |                 string, required if enable_tls is set to true                 |         No |
| tls_trust_store_password          |                           string, default is empty                            |         No |
| enable_tls_hostname_verification  |                boolean, one of [true, false]. default is false                |         No |
| protocols                         |                    array, ciphers list. default is TLSv1.2                    |         No |
| allow_tls_insecure_connection     |                boolean, one of [true, false].default is false                 |         No |
| auth_plugin_class_name            |                                    string                                     |         No |
| ciphers                           |                              array, ciphers list                              |         No |
| enable_token                      |                boolean, one of [true, false]. default is false                |         No |
| auth_plugin_params_String         |                                    string                                     |         No |
| enableAsync                      |                boolean, one of [true, false]. default is false                |         No |


## Sync vs Async

Sync is slower because it requires verification that a message is received. Sync supports exactly/effectively once messaging. This means there is a big network latency increase here.

Async is faster because it sends messages out and doesn't care if the message was received/processed or not. Async should only be used if you want "at most once" messaging and don't care if messages are lost.

# Example
pulsar without tls & token 
```
output{
  pulsar{
    serviceUrl => "pulsar://127.0.0.1:6650"
    topic => "persistent://public/default/%{topic_name}"
    producer_name => "%{producer_name}"
    enable_batching => true
  }
}
```
pulsar with token
```
output {
  pulsar{
    serviceUrl => "pulsar://localhost:6650"
    topic => "persistent://public/default/%{topic_name}"
    producer_name => "%{producer_name}"
    enable_batching => true
    enable_token => true
    auth_plugin_class_name => "org.apache.pulsar.client.impl.auth.AuthenticationToken"
    auth_plugin_params_String => "token:%{token}"
  }
}
```


# Installation

1. Get the latest zip file from release page.
https://github.com/streamnative/logstash-output-pulsar/releases

2. Install this plugin using logstash preoffline command.

```
bin/logstash-plugin install file://{PATH_TO}/logstash-output-pulsar-2.10.0.0.zip
```
