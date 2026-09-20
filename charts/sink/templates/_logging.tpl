{{/* Called only after the schema has checked Go duration syntax. */}}
{{- define "sink.loggingDurationSeconds" -}}
{{- $seconds := 0.0 -}}
{{- $units := dict "ns" 0.000000001 "us" 0.000001 "µs" 0.000001 "μs" 0.000001 "ms" 0.001 "s" 1.0 "m" 60.0 "h" 3600.0 -}}
{{- range $part := regexFindAll "([0-9]+(\\.[0-9]*)?|\\.[0-9]+)(ns|us|µs|μs|ms|s|m|h)" . -1 -}}
{{- $number := regexFind "^[0-9.]+" $part | float64 -}}
{{- $unit := regexFind "[^0-9.]+$" $part -}}
{{- $seconds = addf $seconds (mulf $number (get $units $unit)) -}}
{{- end -}}
{{- $seconds -}}
{{- end -}}

{{- define "sink.validateLogging" -}}
{{- $logging := default dict .config.logging -}}
{{- $console := mergeOverwrite (dict "enabled" true) (deepCopy (default dict $logging.console)) -}}
{{- $otlp := mergeOverwrite (dict "enabled" false "queueSize" 1024 "batchSize" 128 "flushInterval" "1s" "exportTimeout" "3s" "shutdownTimeout" "5s") (deepCopy (default dict $logging.otlp)) -}}
{{- if and (not $console.enabled) (not $otlp.enabled) -}}{{- fail "logging requires console or OTLP output" -}}{{- end -}}
{{- if and $otlp.enabled (not $otlp.endpoint) -}}{{- fail "logging.otlp.endpoint is required when OTLP is enabled" -}}{{- end -}}
{{- with $otlp.endpoint -}}
{{- $port := regexFind "[0-9]+$" . | int -}}
{{- if or (lt $port 1) (gt $port 65535) -}}{{- fail "logging.otlp.endpoint port must be between 1 and 65535" -}}{{- end -}}
{{- end -}}
{{- if gt (int $otlp.batchSize) (int $otlp.queueSize) -}}{{- fail "logging.otlp.batchSize must not exceed queueSize" -}}{{- end -}}
{{- range $field, $maximum := dict "flushInterval" 60.0 "exportTimeout" 30.0 "shutdownTimeout" 30.0 -}}
{{- $seconds := include "sink.loggingDurationSeconds" (get $otlp $field) | float64 -}}
{{- if or (lt $seconds 0.000000001) (gt $seconds $maximum) -}}{{- fail (printf "logging.otlp.%s must be positive and at most %.0fs" $field $maximum) -}}{{- end -}}
{{- end -}}
{{- with $logging.maxBodyBytes -}}
{{- $bytes := . | float64 -}}
{{- if kindIs "string" . -}}
{{- $units := dict "B" 1.0 "KB" 1000.0 "MB" 1000000.0 "GB" 1000000000.0 "TB" 1000000000000.0 "KiB" 1024.0 "MiB" 1048576.0 "GiB" 1073741824.0 "TiB" 1099511627776.0 -}}
{{- $bytes = mulf (regexFind "^[0-9.]+" . | float64) (get $units (regexFind "[A-Za-z]+$" .)) -}}
{{- end -}}
{{- if or (lt $bytes 1024.0) (gt $bytes 65536.0) (ne $bytes (floor $bytes)) -}}{{- fail "logging.maxBodyBytes must be a whole byte count between 1KiB and 64KiB" -}}{{- end -}}
{{- end -}}
{{- range $logging.labels -}}{{- if gt (len .) 256 -}}{{- fail "logging.labels values must be at most 256 UTF-8 bytes" -}}{{- end -}}{{- end -}}
{{- if $otlp.enabled -}}
{{- $flush := include "sink.loggingDurationSeconds" $otlp.shutdownTimeout | float64 | ceil | int -}}
{{- $minimum := add (mul 4 .rollout.shutdownTimeoutSeconds) $flush -}}
{{- if lt (int .rollout.shutdownBudgetSeconds) (int $minimum) -}}{{- fail (printf "shutdownBudgetSeconds must cover four shutdown timeouts plus logging.otlp.shutdownTimeout (%d seconds)" $minimum) -}}{{- end -}}
{{- end -}}
{{- end -}}
