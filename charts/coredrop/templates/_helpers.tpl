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
The object-store option keys the daemon forwards to the handler. Source of
truth is ALLOWED_STORE_OPTS in src/upload.rs; the `helpers_store_opts_match_allowlist`
test there fails if this copy drifts from it.
*/}}
{{- define "coredrop.allowedStoreOpts" -}}
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
AWS_SESSION_TOKEN
AWS_REGION
AWS_DEFAULT_REGION
AWS_ENDPOINT
AWS_ALLOW_HTTP
AWS_VIRTUAL_HOSTED_STYLE_REQUEST
AWS_ROLE_ARN
AWS_WEB_IDENTITY_TOKEN_FILE
GOOGLE_SERVICE_ACCOUNT
GOOGLE_SERVICE_ACCOUNT_KEY
AZURE_STORAGE_ACCOUNT_NAME
AZURE_STORAGE_ACCESS_KEY
AZURE_STORAGE_CLIENT_ID
AZURE_STORAGE_CLIENT_SECRET
AZURE_STORAGE_TENANT_ID
AZURE_CLIENT_ID
AZURE_TENANT_ID
AZURE_FEDERATED_TOKEN_FILE
AZURE_AUTHORITY_HOST
{{- end -}}

{{/*
Fail rendering on an unrecognized objectStore key. Every key in these two maps
is declared as a store option, so an unknown one is a typo; the daemon's allowlist
filter would drop it silently at runtime, leaving no error anywhere.
*/}}
{{- define "coredrop.validateStoreOpts" -}}
{{- $allowed := regexSplit "\\s+" (include "coredrop.allowedStoreOpts" . | trim) -1 -}}
{{- range $field := (list "config" "credentials") -}}
{{- range $key, $_ := (get $.Values.capture.objectStore $field) -}}
{{- if not (has $key $allowed) -}}
{{- fail (printf "capture.objectStore.%s: unknown key %q. Valid keys: %s" $field $key (join ", " $allowed)) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Fail rendering on a spool directory that is not an absolute path below the
root. The handler only accepts absolute paths, and the cleanup worker mounts
the directory's parent.
*/}}
{{- define "coredrop.validateSpool" -}}
{{- $dir := clean .Values.spool.hostDir -}}
{{- if or (not (isAbs $dir)) (eq $dir "/") -}}
{{- fail (printf "spool.hostDir must be an absolute path below /: %q" .Values.spool.hostDir) -}}
{{- end -}}
{{- include "coredrop.validateInt" (dict "name" "spool.maxBytes" "value" .Values.spool.maxBytes "min" 0) -}}
{{- include "coredrop.validateInt" (dict "name" "spool.minFreePercent" "value" .Values.spool.minFreePercent "min" 0 "max" 100) -}}
{{- end -}}

{{/*
Fail rendering when .value is not a whole number in [.min, .max]; .max is
optional. The daemon parses these values as unsigned integers and exits on
anything else. YAML numbers reach the template as float64, --set numbers as
int64, so a float is accepted only when it has no fraction.
*/}}
{{- define "coredrop.validateInt" -}}
{{- $v := .value -}}
{{- $whole := or (kindIs "int" $v) (kindIs "int64" $v) (and (kindIs "float64" $v) (eq $v (floor $v))) -}}
{{- if not $whole -}}
{{- fail (printf "%s must be a whole number: %v" .name $v) -}}
{{- end -}}
{{- $n := int64 $v -}}
{{- if lt $n (int64 .min) -}}
{{- fail (printf "%s must be at least %d: %d" .name (int64 .min) $n) -}}
{{- end -}}
{{- if and (hasKey . "max") (gt $n (int64 .max)) -}}
{{- fail (printf "%s must be at most %d: %d" .name (int64 .max) $n) -}}
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
{{- $runDir := clean .Values.corePattern.hostRunDir -}}
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
            runAsUser: 0
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities:
              drop: ["ALL"]
            seccompProfile:
              type: RuntimeDefault
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
            # The node's core_pattern sysctl, writable; /proc/sys stays read-only.
            - name: host-core-pattern
              mountPath: /host/sysctl/core_pattern
      volumes:
        - name: host-bin-parent
          hostPath:
            path: {{ dir $binDir }}
            type: DirectoryOrCreate
        - name: host-run-parent
          hostPath:
            path: {{ dir $runDir }}
            type: DirectoryOrCreate
        - name: host-spool-parent
          hostPath:
            path: {{ dir $spoolDir }}
            type: DirectoryOrCreate
        - name: host-core-pattern
          hostPath:
            path: /proc/sys/kernel/core_pattern
            type: File
{{- end -}}
