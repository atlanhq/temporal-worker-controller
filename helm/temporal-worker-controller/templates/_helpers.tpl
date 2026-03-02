{{/*
Expand the name of the chart.
Always resolves to "temporal-worker-controller" unless fullnameOverride is set.
This ensures predictable resource names when deployed as a subchart.
*/}}
{{- define "temporal-worker-controller.fullname" -}}
{{- .Values.fullnameOverride | default "temporal-worker-controller" | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{/*
Common labels
Applied to all resources
*/}}
{{- define "temporal-worker-controller.labels" -}}
{{ include "temporal-worker-controller.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
Used for matchLabels (Deployments, Services, affinities, etc.)
*/}}
{{- define "temporal-worker-controller.selectorLabels" -}}
app.kubernetes.io/name: temporal-worker-controller
app.kubernetes.io/instance: {{ include "temporal-worker-controller.fullname" . }}
{{- end }}
