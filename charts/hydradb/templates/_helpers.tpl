{{- define "hydradb.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "hydradb.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "hydradb.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "hydradb.labels" -}}
helm.sh/chart: {{ include "hydradb.chart" . }}
app.kubernetes.io/name: {{ include "hydradb.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: hydradb
{{- end -}}

{{- define "hydradb.selectorLabels" -}}
app.kubernetes.io/name: {{ include "hydradb.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "hydradb.componentLabels" -}}
{{ include "hydradb.selectorLabels" .root }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{- define "hydradb.nodeServiceAccountName" -}}
{{- default (printf "%s-node" (include "hydradb.fullname" .)) .Values.node.serviceAccount.name -}}
{{- end -}}

{{- define "hydradb.indexerServiceAccountName" -}}
{{- default (printf "%s-indexer" (include "hydradb.fullname" .)) .Values.indexer.serviceAccount.name -}}
{{- end -}}

{{- define "hydradb.authSecretName" -}}
{{- if .Values.auth.existingSecret -}}
{{- .Values.auth.existingSecret -}}
{{- else if .Values.auth.externalSecret.enabled -}}
{{- default (printf "%s-client-auth" (include "hydradb.fullname" .)) .Values.auth.externalSecret.targetSecretName -}}
{{- else -}}
{{- printf "%s-client-auth" (include "hydradb.fullname" .) -}}
{{- end -}}
{{- end -}}

{{- define "hydradb.publicTlsSecretName" -}}
{{- default (printf "%s-server-tls" (include "hydradb.fullname" .)) .Values.tls.public.secretName -}}
{{- end -}}

{{- /* The Secret whose ca.crt buddy WAL followers trust, when one is mounted
for them (runtime.buddyWal, TLS on, tls.public.peerCaSecretName set); empty
otherwise, including when the CA comes from the public TLS secret. */ -}}
{{- define "hydradb.peerCaSecretName" -}}
{{- if and .Values.runtime.buddyWal .Values.tls.public.enabled .Values.tls.public.peerCaSecretName -}}
{{- .Values.tls.public.peerCaSecretName -}}
{{- end -}}
{{- end -}}

{{- define "hydradb.nodeAddresses" -}}
{{- include "hydradb.nodeAddressesOn" (dict "root" . "port" 7687) -}}
{{- end -}}

{{- /* Every node as id=host:port on `port`, the ids hydradb.nodeAddresses uses. */ -}}
{{- define "hydradb.nodeAddressesOn" -}}
{{- $addresses := list -}}
{{- $fullname := include "hydradb.fullname" .root -}}
{{- $namespace := .root.Release.Namespace -}}
{{- $port := .port -}}
{{- range $index := until (int .root.Values.node.replicaCount) -}}
{{- $addresses = append $addresses (printf "%s-node-%d=%s-node-%d.%s-node-headless.%s.svc.cluster.local:%v" $fullname $index $fullname $index $fullname $namespace $port) -}}
{{- end -}}
{{- join "," $addresses -}}
{{- end -}}

{{- define "hydradb.proxyServiceName" -}}
{{- default (printf "%s-proxy" (include "hydradb.fullname" .)) .Values.proxy.service.name -}}
{{- end -}}

{{- define "hydradb.proxyImage" -}}
{{- if .Values.proxy.image.digest -}}
{{- printf "%s@%s" .Values.proxy.image.repository .Values.proxy.image.digest -}}
{{- else -}}
{{- printf "%s:%s" .Values.proxy.image.repository (default .Chart.AppVersion .Values.proxy.image.tag) -}}
{{- end -}}
{{- end -}}

{{- define "hydradb.advertisedBoltAddress" -}}
{{- if .Values.service.advertisedBoltAddress -}}
{{- .Values.service.advertisedBoltAddress -}}
{{- else -}}
{{- printf "%s-bolt.%s.svc.cluster.local:%v" (include "hydradb.fullname" .) .Release.Namespace .Values.service.bolt.port -}}
{{- end -}}
{{- end -}}

{{- define "hydradb.image" -}}
{{- if .Values.image.digest -}}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest -}}
{{- else -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) -}}
{{- end -}}
{{- end -}}

{{- define "hydradb.decimalInteger" -}}
{{- $kind := kindOf . -}}
{{- if kindIs "string" . -}}
  {{- if not (regexMatch "^[0-9]+$" .) -}}
    {{- fail (printf "runtime integer %q must be an unsigned decimal string" .) -}}
  {{- end -}}
  {{- $normalized := regexReplaceAll "^0+" . "" -}}
  {{- if eq $normalized "" -}}
    {{- $normalized = "0" -}}
  {{- end -}}
  {{- $overflow := gt (len $normalized) 20 -}}
  {{- if and (eq (len $normalized) 20) (gt $normalized "18446744073709551615") -}}
    {{- $overflow = true -}}
  {{- end -}}
  {{- if $overflow -}}
    {{- fail (printf "runtime integer %q exceeds the unsigned 64-bit maximum 18446744073709551615" .) -}}
  {{- end -}}
  {{- . -}}
{{- else if or (kindIs "float32" .) (kindIs "float64" .) -}}
  {{- $value := float64 . -}}
  {{- if or (lt $value 0.0) (ne $value (floor $value)) -}}
    {{- fail (printf "runtime integer %v must be a non-negative whole number" .) -}}
  {{- end -}}
  {{- if gt $value (float64 "9007199254740991") -}}
    {{- fail (printf "runtime integer %v exceeds exact YAML numeric precision; provide it as a quoted unsigned decimal string" .) -}}
  {{- end -}}
  {{- printf "%.0f" $value -}}
{{- else if or (kindIs "int" .) (kindIs "int8" .) (kindIs "int16" .) (kindIs "int32" .) (kindIs "int64" .) (kindIs "uint" .) (kindIs "uint8" .) (kindIs "uint16" .) (kindIs "uint32" .) (kindIs "uint64" .) -}}
  {{- $value := toString . -}}
  {{- if not (regexMatch "^[0-9]+$" $value) -}}
    {{- fail (printf "runtime integer %v must be non-negative" .) -}}
  {{- end -}}
  {{- $value -}}
{{- else -}}
  {{- fail (printf "runtime integer has unsupported value type %s" $kind) -}}
{{- end -}}
{{- end -}}

{{- define "hydradb.objectStoreEndpointPort" -}}
{{- $endpoint := .Values.objectStore.aws.endpoint -}}
{{- $parsed := urlParse $endpoint -}}
{{- $scheme := get $parsed "scheme" -}}
{{- $host := get $parsed "host" -}}
{{- if or (empty $scheme) (empty $host) -}}
{{- fail "objectStore.aws.endpoint must be an absolute HTTP or HTTPS URL" -}}
{{- end -}}
{{- $explicitPort := regexFind ":[0-9]+$" $host -}}
{{- $port := 0 -}}
{{- if $explicitPort -}}
{{- $port = int (trimPrefix ":" $explicitPort) -}}
{{- else if eq $scheme "https" -}}
{{- $port = 443 -}}
{{- else if eq $scheme "http" -}}
{{- $port = 80 -}}
{{- else -}}
{{- fail "objectStore.aws.endpoint must use the http or https scheme" -}}
{{- end -}}
{{- if or (lt $port 1) (gt $port 65535) -}}
{{- fail "objectStore.aws.endpoint port must be between 1 and 65535" -}}
{{- end -}}
{{- $port -}}
{{- end -}}
