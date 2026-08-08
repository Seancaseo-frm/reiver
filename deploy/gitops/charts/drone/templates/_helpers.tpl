{{/*
Expand the name of the chart.
*/}}
{{- define "drone.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
NOTES.txt expects drone.fullname (server service name). Alias to serverFullname.
*/}}
{{- define "drone.fullname" -}}
{{- include "drone.serverFullname" . }}
{{- end }}

{{/*
Create a default fully qualified server app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "drone.serverFullname" -}}
{{- if .Values.fullnameOverride }}
{{- printf "server-%s" .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- printf "server-%s" .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "server-%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create a default fully qualified runner app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "drone.runnerFullname" -}}
{{- if .Values.fullnameOverride }}
{{- printf "runner-%s" .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- printf "runner-%s" .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "runner-%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "drone.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Server Common labels
*/}}
{{- define "drone.serverLabels" -}}
helm.sh/chart: {{ include "drone.chart" . }}
{{ include "drone.serverSelectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Runner Common labels
*/}}
{{- define "drone.runnerLabels" -}}
helm.sh/chart: {{ include "drone.chart" . }}
{{ include "drone.runnerSelectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Server Selector labels
*/}}
{{- define "drone.serverSelectorLabels" -}}
app: drone-server
app.kubernetes.io/name: server-{{ include "drone.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Server Selector labels
*/}}
{{- define "drone.runnerSelectorLabels" -}}
app: drone-runner
app.kubernetes.io/name: runner-{{ include "drone.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the drone server service account to use
*/}}
{{- define "drone.serverServiceAccountName" -}}
{{- if .Values.server.serviceAccount.create }}
{{- default (include "drone.serverFullname" .) .Values.server.serviceAccount.name }}
{{- else }}
{{- default "serverdefault" .Values.server.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Create the name of the drone runner service account to use
*/}}
{{- define "drone.runnerServiceAccountName" -}}
{{- if .Values.kubeRunner.serviceAccount.create }}
{{- default (include "drone.runnerFullname" .) .Values.kubeRunner.serviceAccount.name }}
{{- else }}
{{- default "runnerdefault" .Values.kubeRunner.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Default provider handler
*/}}
{{- define "drone.defaultProvider" }}
{{- $github := hasKey .Values.server.secrets "DRONE_GITHUB_CLIENT_ID" }}
{{- $gitlab := hasKey .Values.server.secrets "DRONE_GITLAB_CLIENT_ID" }}
{{- $bitbucket := hasKey .Values.server.secrets "DRONE_BITBUCKET_CLIENT_ID" }}
{{- $gitee := hasKey .Values.server.secrets "DRONE_GITEE_CLIENT_ID" }}
{{- $giteaSrv := hasKey .Values.server.secrets "DRONE_GITEA_SERVER" }}
{{- $gitea := hasKey .Values.server.secrets "DRONE_GITEE_CLIENT_ID" }}
{{- $gogs := hasKey .Values.server.secrets "DRONE_GOGS_SERVER" }}
{{- if not (or $github $gitlab $bitbucket $gitee $giteaSrv $gitea $gogs ) }}
- name: DRONE_GOGS_SERVER
  value: http://gogs.fakecompany.com
{{- end }}
{{- end }}
