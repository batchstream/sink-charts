{{- define "sink.name" -}}
{{- $raw := .raw -}}
{{- if gt (len $raw) 63 -}}{{ printf "%s-%s" (trunc 52 $raw | trimSuffix "-") (sha256sum $raw | trunc 10) }}{{- else -}}{{ $raw }}{{- end -}}
{{- end -}}
{{- define "sink.base" -}}
{{- default (printf "%s-%s" .Release.Name (default .Chart.Name .Values.nameOverride)) .Values.fullnameOverride -}}
{{- end -}}
{{- define "sink.componentName" -}}
{{- $raw := printf "%s-%s" (include "sink.base" .root) .role -}}
{{- if .store -}}{{- $raw = printf "%s-%s-%s" (include "sink.base" .root) .store .role -}}{{- end -}}
{{- include "sink.name" (dict "raw" $raw) -}}
{{- end -}}
{{- define "sink.selector" -}}
app.kubernetes.io/instance: {{ .root.Release.Name | quote }}
app.kubernetes.io/name: sink
app.kubernetes.io/component: {{ .role }}
{{ if .store }}sink.batchstream.io/store: {{ .store | quote }}{{ end }}
{{- end -}}
{{- define "sink.labels" -}}
{{ include "sink.selector" . }}
app.kubernetes.io/managed-by: {{ .root.Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .root.Chart.Name .root.Chart.Version | quote }}
{{- end -}}
{{- define "sink.components" -}}
{{- $items := list -}}
{{- $active := false -}}
{{- range $store, $settings := .Values.stores -}}
{{- if eq $settings.phase "active" -}}{{- $active = true -}}{{- end -}}
{{- $engine := mergeOverwrite (deepCopy $.Values.defaults.engine) (default dict $settings.engine) -}}
{{- $worker := mergeOverwrite (deepCopy $.Values.defaults.worker) (default dict $settings.worker) -}}
{{- $items = append $items (dict "role" "engine" "store" $store "settings" $engine "storage" $settings.storage "credentialRevision" (default "" $settings.credentialRevision) "kafka" (default dict $settings.kafka)) -}}
{{- if $worker.enabled -}}{{- $items = append $items (dict "role" "worker" "store" $store "settings" $worker "storage" $settings.storage "credentialRevision" (default "" $settings.credentialRevision) "kafka" (default dict $settings.kafka)) -}}{{- end -}}
{{- end -}}
{{- if $active -}}{{- $items = prepend $items (dict "role" "gateway" "store" "" "settings" .Values.gateway "kafka" dict) -}}{{- end -}}
{{- toJson $items -}}
{{- end -}}
{{- define "sink.discoverySeconds" -}}
{{- $d := .Values.cluster.discovery -}}
{{- add $d.endpointPropagationSeconds $d.dnsCacheTTLSeconds $d.dnsRefreshSeconds $d.dnsRetryBackoffSeconds -}}
{{- end -}}
{{- define "sink.preStop" -}}
{{- $minimum := 0 -}}
{{- if eq .role "engine" -}}{{- $minimum = add (include "sink.discoverySeconds" .root | int) .root.Values.cluster.discovery.safetyMarginSeconds -}}{{- end -}}
{{- if eq .role "gateway" -}}{{- $minimum = add .root.Values.cluster.discovery.clientEndpointRemovalSeconds .root.Values.cluster.discovery.safetyMarginSeconds -}}{{- end -}}
{{- $seconds := $minimum -}}
{{- if ne .settings.rollout.preStopDelaySeconds nil -}}{{- $seconds = int .settings.rollout.preStopDelaySeconds -}}{{- end -}}
{{- if lt (int $seconds) (int $minimum) -}}{{- fail (printf "%s/%s preStopDelaySeconds must be >= %d (discovery + margin)" .role .store $minimum) -}}{{- end -}}
{{- $seconds -}}
{{- end -}}
{{- define "sink.grace" -}}
{{- $minimum := add (include "sink.preStop" . | int) .settings.rollout.shutdownBudgetSeconds .root.Values.cluster.discovery.safetyMarginSeconds -}}
{{- $seconds := $minimum -}}
{{- if ne .settings.rollout.terminationGracePeriodSeconds nil -}}{{- $seconds = int .settings.rollout.terminationGracePeriodSeconds -}}{{- end -}}
{{- if lt (int $seconds) (int $minimum) -}}{{- fail (printf "%s/%s terminationGracePeriodSeconds must be >= %d" .role .store $minimum) -}}{{- end -}}
{{- $seconds -}}
{{- end -}}
{{- define "sink.memoryBytes" -}}
{{- $value := toString . -}}
{{- $number := regexFind "^[0-9]+" $value | int64 -}}
{{- $suffix := regexFind "[A-Za-z]+$" $value -}}
{{- $multipliers := dict "" 1 "Ki" 1024 "Mi" 1048576 "Gi" 1073741824 "Ti" 1099511627776 "K" 1000 "M" 1000000 "G" 1000000000 "T" 1000000000000 -}}
{{- mul $number (get $multipliers $suffix | int64) -}}
{{- end -}}
{{- define "sink.gatewayConfig" -}}
{{- $routes := list -}}
{{- range $store, $settings := .Values.stores -}}
{{- if eq $settings.phase "active" -}}
{{- $name := include "sink.componentName" (dict "root" $ "role" "engine" "store" $store) -}}
{{- $routes = append $routes (dict "store" $store "target" (printf "dns:///%s.%s.svc.%s:8080" $name $.Release.Namespace $.Values.cluster.domain) "tls" (dict "insecure" true)) -}}
{{- end -}}
{{- end -}}
{{- $config := include "sink.configFields" .Values.gateway.config | fromJson -}}
{{- $_ := set $config "forwarding" (mergeOverwrite (default dict $config.forwarding) (dict "routes" $routes "dns_refresh_interval" (printf "%ds" (int .Values.cluster.discovery.dnsRefreshSeconds)))) -}}
{{- $_ := set $config "grpc" (mergeOverwrite (default dict $config.grpc) (dict "address" ":8080")) -}}
{{- $_ := set $config "mode" "gateway" -}}
{{- $_ := set $config "health" (dict "address" ":8081") -}}
{{- $_ := set $config "prometheus" (dict "enabled" .Values.metrics.enabled "address" ":9090") -}}
{{- $_ := set $config "shutdown_timeout" (printf "%ds" (int .Values.gateway.rollout.shutdownTimeoutSeconds)) -}}
{{- range $key, $value := $config -}}{{- if empty $value -}}{{- $_ := unset $config $key -}}{{- end -}}{{- end -}}
{{- toYaml $config -}}
{{- end -}}
{{- define "sink.behavior" -}}
{{ toYaml .behavior }}
{{- end -}}

{{- define "sink.cpuMillis" -}}
{{- $value := toString . -}}
{{- if hasSuffix "m" $value -}}{{ trimSuffix "m" $value | float64 }}{{- else -}}{{ mulf ($value | float64) 1000 }}{{- end -}}
{{- end -}}
