{{/* Only references enter Helm state. Kubernetes projects selected Secret keys. */}}
{{- define "sink.secretSources" -}}
{{- $sources := list -}}
{{- $refs := dict -}}
{{- if eq .storage.driver "mongodb" -}}
{{- $_ := set $refs "mongodb-uri" .storage.mongodb.uriSecretRef -}}
{{- else -}}
{{- range $field, $path := dict "usernameSecretRef" "search-username" "passwordSecretRef" "search-password" "apiKeySecretRef" "search-api-key" -}}
{{- with get $.storage.search $field -}}{{- $_ := set $refs $path . -}}{{- end -}}
{{- end -}}
{{- end -}}
{{- range $path, $ref := $refs -}}
{{- $sources = append $sources (dict "secret" (dict "name" $ref.name "optional" false "items" (list (dict "key" $ref.key "path" $path)))) -}}
{{- end -}}
{{- toJson $sources -}}
{{- end -}}

{{- define "sink.storeConfig" -}}
{{- $storage := dict "name" .store "driver" .storage.driver -}}
{{- if eq .storage.driver "mongodb" -}}
{{- $mongo := dict "metadata_field" (default "__sink" .storage.mongodb.metadataField) "max_concurrent_writes" (default 64 .storage.mongodb.maxConcurrentWrites) "max_concurrent_groups" (default 16 .storage.mongodb.maxConcurrentGroups) -}}
{{- $_ := set $mongo "uri_file" "/etc/sink-secrets/mongodb-uri" -}}
{{- $_ := set $storage "mongodb" $mongo -}}
{{- else -}}
{{- $search := omit .storage.search "usernameSecretRef" "passwordSecretRef" "apiKeySecretRef" -}}
{{- range $field, $path := dict "username" "search-username" "password" "search-password" "apiKey" "search-api-key" -}}
{{- if hasKey $.storage.search (printf "%sSecretRef" $field) -}}
{{- $target := $field -}}{{- if eq $field "apiKey" -}}{{- $target = "api_key" -}}{{- end -}}
{{- $_ := set $search (printf "%s_file" $target) (printf "/etc/sink-secrets/%s" $path) -}}
{{- end -}}
{{- end -}}
{{- $_ := set $storage "search" $search -}}
{{- end -}}
{{- if .kafka -}}
{{- $kafka := deepCopy (default dict .kafka.runtime) -}}
{{- $managed := dict "enabled" true "brokers" .kafka.brokers "topic" (dict "name" .kafka.topic "partitions" .kafka.partitions "replication_factor" (default 3 .kafka.replicationFactor) "min_insync_replicas" (default 2 .kafka.minInSyncReplicas)) "consumer" (dict "group_id" .kafka.consumerGroup) -}}
{{- $_ := set $storage "kafka" (mergeOverwrite $kafka $managed) -}}
{{- end -}}
{{- $service := mergeOverwrite (deepCopy .settings.runtime.service) (dict "request" (dict "timeout" (printf "%ds" (int .root.Values.requestTimeoutSeconds)))) -}}
{{- $grpc := mergeOverwrite (deepCopy .settings.runtime.grpc) (dict "address" ":8080") -}}
{{- $config := (dict "mode" .role "storage" $storage "service" $service "grpc" $grpc "health" (dict "address" ":8081") "prometheus" (dict "enabled" .root.Values.metrics.enabled "address" ":9090") "shutdown_timeout" (printf "%ds" (int .settings.pod.shutdownTimeoutSeconds))) -}}
{{- with .settings.runtime.memory -}}{{- $_ := set $config "memory" . -}}{{- end -}}
{{- toYaml $config -}}
{{- end -}}
