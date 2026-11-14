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
Render .value, a size, as the string the daemon parses. values.schema.json
allows a whole number of bytes, or a whole number with a Kubernetes quantity
suffix. YAML numbers reach the template as float64, which toYaml would render
in exponent form, so a number is rendered as an int64.
*/}}
{{- define "coredrop.size" -}}
{{- if kindIs "string" .value -}}
{{- .value -}}
{{- else -}}
{{- int64 .value -}}
{{- end -}}
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
{{- /* Fixed: the kernel-exec'd handler reads its config only here. */ -}}
{{- $runDir := "/run/coredrop" -}}
{{- $spoolDir := clean .Values.spool.hostDir -}}
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
            {{- toYaml .Values.securityContext | nindent 12 }}
          # The configured paths reach the script as environment variables,
          # never as script text, so no character in them is shell syntax.
          env:
            - name: BIN_DIR
              value: {{ $binDir | quote }}
            - name: BIN_BASE
              value: {{ base $binDir | quote }}
            - name: RUN_BASE
              value: {{ base $runDir | quote }}
            - name: SPOOL_BASE
              value: {{ base $spoolDir | quote }}
          command:
            - sh
            - -c
            - |
              set -e
              # For a daemon killed without a clean shutdown: if core_pattern still
              # points at our handler, reset it to the kernel default. The original
              # value is unknown at this point. This runs before the binary is
              # removed, so no crash is routed to a missing handler.
              pattern=$(cat /host/sysctl/core_pattern)
              case "$pattern" in
                *"$BIN_DIR/coredrop"*)
                  printf 'core' > /host/sysctl/core_pattern
                  # The daemon raised core_pipe_limit too; put back the
                  # kernel default.
                  printf '0' > /host/sysctl/core_pipe_limit
                  ;;
              esac

              # The parent of each directory is what gets mounted, not the
              # directory itself: a hostPath mounted at its own path is a mount
              # point inside this container, and rmdir on a mount point always
              # fails with EBUSY - the files would go but the directories would
              # stay. Reaching them through their parent keeps them ordinary
              # subdirectories, so rmdir works.
              bin_dir="/host/bin-parent/$BIN_BASE"
              run_dir="/host/run-parent/$RUN_BASE"
              spool_dir="/host/spool-parent/$SPOOL_BASE"

              rm -f "$bin_dir/coredrop"
              rm -f "$run_dir/handler.json"
              rm -f "$run_dir/events.sock"
              rm -f "$run_dir/recent.json"
              # Only the spool's own subdirectories, never the configured
              # directory's other contents: spool.hostDir is user input.
              rm -rf "$spool_dir/incoming" "$spool_dir/ready"
              rmdir "$bin_dir" 2>/dev/null || true
              rmdir "$run_dir" 2>/dev/null || true
              rmdir "$spool_dir" 2>/dev/null || true

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
            - name: host-spool-parent
              mountPath: /host/spool-parent
            # The node's core_pattern and core_pipe_limit sysctls, writable;
            # /proc/sys stays read-only.
            - name: host-core-pattern
              mountPath: /host/sysctl/core_pattern
            - name: host-core-pipe-limit
              mountPath: /host/sysctl/core_pipe_limit
      volumes:
        - name: host-bin-parent
          hostPath:
            path: {{ dir $binDir | quote }}
            type: DirectoryOrCreate
        - name: host-run-parent
          hostPath:
            path: {{ dir $runDir | quote }}
            type: DirectoryOrCreate
        - name: host-spool-parent
          hostPath:
            path: {{ dir $spoolDir | quote }}
            type: DirectoryOrCreate
        - name: host-core-pattern
          hostPath:
            path: /proc/sys/kernel/core_pattern
            type: File
        - name: host-core-pipe-limit
          hostPath:
            path: /proc/sys/kernel/core_pipe_limit
            type: File
{{- end -}}
