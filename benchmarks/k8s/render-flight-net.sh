#!/usr/bin/env bash
# Renders the cross-node Flight transport benchmark (FlightNetBench): a server pod serving
# FlightBench's map files on ports 47470-47471 behind a headless Service, and a client Job on a
# DIFFERENT node (pod anti-affinity) driving reduce tasks against it. Prints YAML; pipe into kubectl.
#
#   benchmarks/k8s/render-flight-net.sh <image> "<server params>" "<client params>" | kubectl apply -f -
#
#   image          the cluster image (benchmarks/k8s/Dockerfile): benchmarks.jar and the Flight jars are
#                  in /opt/spark/jars, the sorted class path in /opt/spark/aot/classpath
#   server params  e.g. "partitions=200 maps=16 rowsPerMap=1000000 threads=10 chunkBytes=4m"
#   client params  e.g. "partitions=200 maps=16 rowsPerMap=1000000 concurrency=5 seconds=60 runs=3
#                  channelsPerPeer=1,2,4 clientWindow=0,4m,16m"
#   The data parameters (partitions, maps, rowsPerMap, strings, compression) must match on both sides;
#   the client checks every row comes back before it measures.
#
# Environment: NAME (flight-net) NAMESPACE (bench) SERVICE_ACCOUNT (sfi-engine)
#   NODE_SELECTOR (workload=spark-r8gd; key=value, empty for none) CPU (5, one executor's cores)
#   MEM (40Gi) HEAP (8g) DIRECT_MEM (24g)
# Results: the client's log, one JSON line per run (kubectl logs job/<NAME>-client); the server's log
# has its cumulative CPU every 10 s to diff against a run's startMs/endMs. Delete both when done:
#   kubectl -n bench delete job/<NAME>-client pod/<NAME>-server svc/<NAME>
set -euo pipefail
IMAGE="${1:?image}"; SERVER_PARAMS="${2:?server params}"; CLIENT_PARAMS="${3:?client params}"
NAME="${NAME:-flight-net}"; NAMESPACE="${NAMESPACE:-bench}"; SERVICE_ACCOUNT="${SERVICE_ACCOUNT:-sfi-engine}"
NODE_SELECTOR="${NODE_SELECTOR-workload=spark-r8gd}"; CPU="${CPU:-5}"; MEM="${MEM:-40Gi}"
HEAP="${HEAP:-8g}"; DIRECT_MEM="${DIRECT_MEM:-24g}"

# The node selector at the pod's indentation and at the Job template's.
pod_selector=""; job_selector=""
if [ -n "$NODE_SELECTOR" ]; then
  line="nodeSelector: { ${NODE_SELECTOR%%=*}: \"${NODE_SELECTOR#*=}\" }"
  pod_selector="  $line"; job_selector="      $line"
fi
# The JVM options the executors run with (spark-application.yaml), minus Spark's own.
JAVA_CMD="exec /opt/java/openjdk/bin/java -Xmx$HEAP -XX:MaxDirectMemorySize=$DIRECT_MEM -XX:+UseG1GC \
--add-modules=jdk.incubator.vector --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow \
--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED \
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED \
-Dio.netty.tryReflectionSetAccessible=true -Dlog4j2.level=warn \
-cp \$(cat /opt/spark/aot/classpath) org.apache.spark.sql.vecruntime.bench.FlightNetBench"

cat <<EOF
apiVersion: v1
kind: Service
metadata: { name: $NAME, namespace: $NAMESPACE }
spec:
  clusterIP: None
  selector: { app: $NAME, role: server }
  ports:
  - { name: flight-0, port: 47470 }
  - { name: flight-1, port: 47471 }
---
apiVersion: v1
kind: Pod
metadata:
  name: $NAME-server
  namespace: $NAMESPACE
  labels: { app: $NAME, role: server }
spec:
  serviceAccountName: $SERVICE_ACCOUNT
  restartPolicy: Never
$pod_selector
  containers:
  - name: server
    image: $IMAGE
    command: ["/bin/bash", "-c", "$JAVA_CMD server port=47470 $SERVER_PARAMS"]
    resources:
      requests: { cpu: "$CPU", memory: $MEM }
      limits: { cpu: "$CPU", memory: $MEM }
    readinessProbe:
      tcpSocket: { port: 47471 }
      periodSeconds: 5
---
apiVersion: batch/v1
kind: Job
metadata: { name: $NAME-client, namespace: $NAMESPACE }
spec:
  backoffLimit: 0
  template:
    metadata:
      labels: { app: $NAME, role: client }
    spec:
      serviceAccountName: $SERVICE_ACCOUNT
      restartPolicy: Never
$job_selector
      affinity:
        podAntiAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
          - labelSelector: { matchLabels: { app: $NAME, role: server } }
            topologyKey: kubernetes.io/hostname
      initContainers:
      # Wait for the server: it writes its map files before it listens.
      - name: wait
        image: $IMAGE
        command: ["/bin/bash", "-c", "until (exec 3<>/dev/tcp/$NAME.$NAMESPACE.svc/47471) 2>/dev/null; do sleep 5; done"]
      containers:
      - name: client
        image: $IMAGE
        command: ["/bin/bash", "-c", "$JAVA_CMD client host=$NAME.$NAMESPACE.svc port=47470 $CLIENT_PARAMS"]
        resources:
          requests: { cpu: "$CPU", memory: $MEM }
          limits: { cpu: "$CPU", memory: $MEM }
EOF
