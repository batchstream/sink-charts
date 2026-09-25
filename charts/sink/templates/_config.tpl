{{/* Values use camelCase; only application config keys are translated. */}}
{{- define "sink.configFields" -}}
{{- $names := dict
  "batchSize" "batch_size"
  "executionQueue" "execution_queue"
  "exportTimeout" "export_timeout"
  "flushInterval" "flush_interval"
  "groupId" "group_id"
  "highWatermarkPercent" "high_watermark_percent"
  "idleTimeout" "idle_timeout"
  "includeFailureBody" "failure_body"
  "kafkaConsumer" "consumer"
  "kafkaProducer" "producer"
  "lowWatermarkPercent" "low_watermark_percent"
  "maxAttempts" "max_attempts"
  "maxBackoff" "max_backoff"
  "maxBodyBytes" "max_body_bytes"
  "maxBufferedBytes" "max_buffered_bytes"
  "maxBytes" "max_bytes"
  "maxCachedPrograms" "max_cached_programs"
  "maxConnections" "max_connections"
  "maxFanout" "max_fanout"
  "maxInstructions" "max_instructions"
  "maxOperations" "max_operations"
  "maxPollRecords" "max_poll_records"
  "maxReceiveMessageBytes" "max_receive_message_bytes"
  "maxResultBytes" "max_result_bytes"
  "maxSendMessageBytes" "max_send_message_bytes"
  "maxSourceBytes" "max_source_bytes"
  "maxTasks" "max_tasks"
  "maxWait" "max_wait"
  "processingTimeout" "processing_timeout"
  "queueSize" "queue_size"
  "requestLimits" "request"
  "shutdownTimeout" "shutdown_timeout"
-}}
{{- $output := dict -}}
{{- range $key, $value := . -}}
{{- if and (kindIs "map" $value) (ne $key "labels") -}}
{{- $value = include "sink.configFields" $value | fromJson -}}
{{- end -}}
{{- $_ := set $output (default $key (get $names $key)) $value -}}
{{- end -}}
{{- toJson $output -}}
{{- end -}}
