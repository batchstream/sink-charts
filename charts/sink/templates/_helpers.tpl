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
{{- if eq $settings.state "active" -}}{{- $active = true -}}{{- end -}}
{{- $engine := mergeOverwrite (deepCopy $.Values.engineDefaults) (default dict $settings.engine) -}}
{{- $worker := mergeOverwrite (deepCopy $.Values.workerDefaults) (default dict $settings.worker) -}}
{{- $items = append $items (dict "role" "engine" "store" $store "settings" $engine "storage" $settings.storage "credentialRevision" (default "" $settings.credentialRevision) "kafka" (default dict $settings.kafka)) -}}
{{- if $worker.enabled -}}{{- $items = append $items (dict "role" "worker" "store" $store "settings" $worker "storage" $settings.storage "credentialRevision" (default "" $settings.credentialRevision) "kafka" (default dict $settings.kafka)) -}}{{- end -}}
{{- end -}}
{{- if $active -}}{{- $items = prepend $items (dict "role" "gateway" "store" "" "settings" .Values.gateway "kafka" dict) -}}{{- end -}}
{{- toJson $items -}}
{{- end -}}
{{- define "sink.componentImage" -}}
{{- $layers := list .root.Values.image -}}
{{- if eq .role "gateway" -}}
{{- $layers = append $layers (default dict .root.Values.gateway.image) -}}
{{- else -}}
{{- $defaults := index .root.Values (printf "%sDefaults" .role) -}}
{{- $store := index .root.Values.stores .store -}}
{{- $layers = append $layers (default dict $defaults.image) -}}
{{- $layers = append $layers (dig .role "image" dict $store) -}}
{{- end -}}
{{- $image := dict -}}
{{- range $layer := $layers -}}
{{- if and (hasKey $layer "tag") (not (hasKey $layer "digest")) -}}
{{- $_ := set $image "digest" "" -}}
{{- end -}}
{{- $image = mergeOverwrite $image (deepCopy $layer) -}}
{{- end -}}
{{- toJson $image -}}
{{- end -}}
{{- define "sink.discoverySeconds" -}}
{{- $d := .Values.discovery -}}
{{- add $d.endpointPublicationSeconds $d.dnsCacheSeconds $d.dnsRefreshSeconds $d.lookupBackoffSeconds -}}
{{- end -}}
{{- define "sink.preStop" -}}
{{- $minimum := 0 -}}
{{- if eq .role "engine" -}}{{- $minimum = add (include "sink.discoverySeconds" .root | int) .root.Values.requestTimeoutSeconds .root.Values.discovery.safetyMarginSeconds -}}{{- end -}}
{{- if eq .role "gateway" -}}{{- $minimum = add .root.Values.discovery.clientWithdrawalSeconds .root.Values.requestTimeoutSeconds .root.Values.discovery.safetyMarginSeconds -}}{{- end -}}
{{- $seconds := $minimum -}}
{{- if ne .settings.pod.preStopSeconds nil -}}{{- $seconds = int .settings.pod.preStopSeconds -}}{{- end -}}
{{- if lt (int $seconds) (int $minimum) -}}{{- fail (printf "%s/%s preStopSeconds must be >= %d (discovery + request + margin)" .role .store $minimum) -}}{{- end -}}
{{- $seconds -}}
{{- end -}}
{{- define "sink.grace" -}}
{{- $minimum := add (include "sink.preStop" . | int) .settings.pod.shutdownBudgetSeconds .root.Values.discovery.safetyMarginSeconds -}}
{{- $seconds := $minimum -}}
{{- if ne .settings.pod.terminationGracePeriodSeconds nil -}}{{- $seconds = int .settings.pod.terminationGracePeriodSeconds -}}{{- end -}}
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
{{- if eq $settings.state "active" -}}
{{- $name := include "sink.componentName" (dict "root" $ "role" "engine" "store" $store) -}}
{{- $routes = append $routes (dict "store" $store "target" (printf "dns:///%s.%s.svc.%s:8080" $name $.Release.Namespace $.Values.clusterDomain) "tls" (dict "insecure" true)) -}}
{{- end -}}
{{- end -}}
{{- $gateway := mergeOverwrite (deepCopy .Values.gateway.runtime.gateway) (dict "routes" $routes "dns_refresh_interval" (printf "%ds" (int .Values.discovery.dnsRefreshSeconds))) -}}
{{- $service := mergeOverwrite (deepCopy .Values.gateway.runtime.service) (dict "request" (dict "timeout" (printf "%ds" (int .Values.requestTimeoutSeconds)))) -}}
{{- $grpc := mergeOverwrite (deepCopy .Values.gateway.runtime.grpc) (dict "address" ":8080") -}}
{{- $config := (dict "mode" "gateway" "gateway" $gateway "service" $service "grpc" $grpc "health" (dict "address" ":8081") "prometheus" (dict "enabled" .Values.metrics.enabled "address" ":9090") "shutdown_timeout" (printf "%ds" (int .Values.gateway.pod.shutdownTimeoutSeconds))) -}}
{{- with .Values.gateway.runtime.memory -}}{{- $_ := set $config "memory" . -}}{{- end -}}
{{- toYaml $config -}}
{{- end -}}
{{- define "sink.behavior" -}}
{{ toYaml .behavior }}
{{- end -}}

{{- define "sink.cpuMillis" -}}
{{- $value := toString . -}}
{{- if hasSuffix "m" $value -}}{{ trimSuffix "m" $value | float64 }}{{- else -}}{{ mulf ($value | float64) 1000 }}{{- end -}}
{{- end -}}
