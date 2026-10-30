import json
import os
import sys
import paho.mqtt.client as mqtt
import psycopg2
from psycopg2.extras import Json

# OpenTelemetry 関連モジュール
from opentelemetry import trace
from opentelemetry.trace import StatusCode
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.resources import Resource
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

# --- 接続設定（環境変数） ---
BROKER_HOST = os.getenv("MQTT_BROKER", "emqx-service.mqtt.svc.cluster.local")
BROKER_PORT = int(os.getenv("MQTT_PORT", "1883"))
TOPIC = "devices/+/telemetry"

DB_HOST = os.getenv("DB_HOST", "postgres-service.mqtt.svc.cluster.local")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "iot_data")
DB_USER = os.getenv("DB_USER", "mqtt_user")
DB_PASS = os.getenv("DB_PASS", "mqtt_password")

OTEL_COLLECTOR_ENDPOINT = os.getenv(
    "OTEL_EXPORTER_OTLP_ENDPOINT", 
    "opentelemetry-collector.monitoring.svc.cluster.local:4317"
)

# --- 1. OpenTelemetry トレーサーの初期化 ---
resource = Resource.create(attributes={
    "service.name": "k8s-mqtt-worker",
    "service.namespace": "iot-pipeline",
    "k8s.component": "worker"
})
provider = TracerProvider(resource=resource)

# K8s クラスタ内部の OTel Collector (ClusterIP) 宛に gRPC 転送
otlp_exporter = OTLPSpanExporter(
    endpoint=OTEL_COLLECTOR_ENDPOINT,
    insecure=True
)
provider.add_span_processor(BatchSpanProcessor(otlp_exporter))
trace.set_tracer_provider(provider)

tracer = trace.get_tracer("k8s.mqtt.worker", "1.0.0")
propagator = TraceContextTextMapPropagator()


def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )


def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print(f"[Worker] Connected to MQTT Broker. Subscribing to: {TOPIC}", flush=True)
        client.subscribe(TOPIC)
    else:
        print(f"[Worker] Connection failed with code {rc}", flush=True)


def on_message(client, userdata, msg):
    topic = msg.topic
    payload_str = msg.payload.decode('utf-8')
    parts = topic.split('/')
    device_id = parts[1] if len(parts) >= 2 else "unknown"

    print(f"[Worker] Received from {device_id} on {topic}", flush=True)

    extracted_context = None
    data = None

    try:
        data = json.loads(payload_str)
        
        # Raspi 5 が付与した traceparent を抽出して親コンテキストを復元
        carrier = {}
        if isinstance(data, dict) and "traceparent" in data:
            carrier["traceparent"] = data["traceparent"]
            extracted_context = propagator.extract(carrier=carrier)

    except json.JSONDecodeError:
        print(f"  -> [Warning] Failed to parse JSON: {payload_str}", flush=True)
        return

    # 親コンテキスト（Raspi 5 のスパン）を引き継いでワーカー処理スパンを開始
    with tracer.start_as_current_span("worker_process_telemetry", context=extracted_context) as span:
        span.set_attribute("messaging.system", "mqtt")
        span.set_attribute("messaging.destination", topic)
        span.set_attribute("device.id", device_id)

        try:
            # PostgreSQL 保存処理（子スパンとして計測）
            save_to_db(device_id, topic, data)
            print(f"  -> Successfully saved to PostgreSQL! Data: {data}", flush=True)
        except Exception as e:
            span.record_exception(e)
            span.set_status(StatusCode.ERROR, str(e))
            print(f"  -> [Error] DB Insert failed: {e}", flush=True)


def save_to_db(device_id, topic, data):
    with tracer.start_as_current_span("postgres_insert") as db_span:
        db_span.set_attribute("db.system", "postgresql")
        db_span.set_attribute("db.name", DB_NAME)
        db_span.set_attribute("db.operation", "INSERT")
        db_span.set_attribute("db.sql.table", "telemetry")

        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO telemetry (device_id, topic, payload) VALUES (%s, %s, %s);",
            (device_id, topic, Json(data))
        )
        conn.commit()
        cur.close()
        conn.close()


if __name__ == "__main__":
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message

    print(f"[Worker] Connecting to {BROKER_HOST}:{BROKER_PORT}...", flush=True)
    client.connect(BROKER_HOST, BROKER_PORT, 60)
    
    try:
        client.loop_forever()
    except KeyboardInterrupt:
        print("[Worker] Stopping...", flush=True)
        client.disconnect()
        provider.shutdown()
