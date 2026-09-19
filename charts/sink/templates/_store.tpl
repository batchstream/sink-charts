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
{{- $storage := dict "driver" .storage.driver -}}
{{- if eq .storage.driver "mongodb" -}}
{{- $mongo := dict "metadata_field" (default "__sink" .storage.mongodb.metadataField) -}}
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
{{- $config := dict "name" .store "storage" $storage -}}
{{- if .kafka -}}
{{- $kafka := deepCopy (default dict .kafka.runtime) -}}
{{- $managed := dict "enabled" true "brokers" .kafka.brokers "partitions" .kafka.partitions "replication_factor" (default 3 .kafka.replicationFactor) "min_insync_replicas" (default 1 .kafka.minInSyncReplicas) "topic" (dict "name" .kafka.topic) -}}
{{- $_ := set $config "kafka" (mergeOverwrite $kafka $managed) -}}
{{- end -}}
{{- toYaml $config -}}
{{- end -}}

{{- define "sink.roleConfig" -}}
{{- $config := deepCopy .settings.runtime -}}
{{- $_ := set $config "mode" .role -}}
{{- $_ := set $config "health" (dict "address" ":8081") -}}
{{- $_ := set $config "prometheus" (dict "enabled" .root.Values.metrics.enabled "address" ":9090") -}}
{{- $_ := set $config "shutdown_timeout" (printf "%ds" (int .settings.pod.shutdownTimeoutSeconds)) -}}
{{- if eq .role "engine" -}}
{{- $_ := set $config "grpc" (mergeOverwrite (default dict $config.grpc) (dict "address" ":8080")) -}}
{{- end -}}
{{- range $key, $value := $config -}}{{- if empty $value -}}{{- $_ := unset $config $key -}}{{- end -}}{{- end -}}
{{- toYaml $config -}}
{{- end -}}
