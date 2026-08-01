{{- define "coredrop.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "coredrop.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := include "coredrop.name" . -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "coredrop.labels" -}}
app.kubernetes.io/name: {{ include "coredrop.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/component: coredrop
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{- define "coredrop.selectorLabels" -}}
app.kubernetes.io/name: {{ include "coredrop.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "coredrop.serviceAccountName" -}}
{{ include "coredrop.fullname" . }}
{{- end -}}

{{/*
The post-delete cleanup worker: a DaemonSet that removes the handler binary,
runtime config/state, and hostPath directories from every node.
Rendered here (rather than inline in cleanup-hook.yaml) so it can be shipped
as a ConfigMap and applied by the cleanup Job via kubectl - see
cleanup-hook.yaml for why a Job has to be the thing driving it.
*/}}
{{- define "coredrop.cleanupWorkerManifest" -}}
{{- $binDir := clean .Values.corePattern.hostBinDir -}}
{{- $runDir := clean .Values.corePattern.hostRunDir -}}
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: {{ include "coredrop.fullname" . }}-cleanup-worker
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "coredrop.labels" . | nindent 4 }}
spec:
  selector:
    matchLabels:
      {{- include "coredrop.selectorLabels" . | nindent 6 }}
      coredrop.io/cleanup: "true"
  template:
    metadata:
      labels:
        {{- include "coredrop.selectorLabels" . | nindent 8 }}
        coredrop.io/cleanup: "true"
    spec:
      hostPID: true
      {{- with .Values.nodeSelector }}
      nodeSelector:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with .Values.tolerations }}
      tolerations:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      containers:
        - name: cleanup
          image: "{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}"
          imagePullPolicy: {{ .Values.image.pullPolicy }}
          securityContext:
            privileged: true
          command:
            - sh
            - -c
            - |
              set -e
              # The parent of each directory is what gets mounted, not the
              # directory itself: a hostPath mounted at its own path is a mount
              # point inside this container, and rmdir on a mount point always
              # fails with EBUSY - the files would go but the directories would
              # stay. Reaching them through their parent keeps them ordinary
              # subdirectories, so rmdir works.
              bin_dir=/host/bin-parent/{{ base $binDir }}
              run_dir=/host/run-parent/{{ base $runDir }}

              rm -f "$bin_dir/coredrop"
              rm -f "$run_dir/handler.json"
              rm -f "$run_dir/events.sock"
              rm -f "$run_dir/recent.json"
              rmdir "$bin_dir" 2>/dev/null || true
              rmdir "$run_dir" 2>/dev/null || true

              # Safety net for ungraceful daemon shutdowns: if core_pattern still
              # points at our handler, restore a sane default. The original value
              # is unknown at this point, so we fall back to the kernel default.
              pattern=$(cat /proc/sys/kernel/core_pattern)
              case "$pattern" in
                *"{{ $binDir }}/coredrop"*)
                  printf 'core' > /proc/sys/kernel/core_pattern
                  ;;
              esac

              # Keep the pod alive so the orchestrator Job can observe the
              # DaemonSet's rollout as ready - cleanup already ran above by
              # the time this is reached. Cleanup is idempotent.
              exec sleep infinity
          volumeMounts:
            # Fixed mount paths (not the configured ones) so that two configured
            # directories sharing a parent still yield two distinct mountPaths.
            - name: host-bin-parent
              mountPath: /host/bin-parent
            - name: host-run-parent
              mountPath: /host/run-parent
      volumes:
        - name: host-bin-parent
          hostPath:
            path: {{ dir $binDir }}
            type: DirectoryOrCreate
        - name: host-run-parent
          hostPath:
            path: {{ dir $runDir }}
            type: DirectoryOrCreate
{{- end -}}
