import os
import json
import time
from pathlib import Path
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse
import paho.mqtt.client as mqtt

app = FastAPI(title="Multi-Cloud IoT Storage Gateway")

STORAGE_BACKEND = os.getenv("STORAGE_BACKEND", "local")
LOCAL_STORAGE_DIR = Path(os.getenv("LOCAL_STORAGE_DIR", "/data"))
MQTT_BROKER = os.getenv("MQTT_BROKER", "emqx-service.mqtt.svc.cluster.local")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))

# ディレクトリ初期作成
(LOCAL_STORAGE_DIR / "firmware").mkdir(parents=True, exist_ok=True)
(LOCAL_STORAGE_DIR / "camera").mkdir(parents=True, exist_ok=True)

# EMQX 接続初期化（アップロード完了通知用）
mqtt_client = mqtt.Client()
try:
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
    mqtt_client.loop_start()
    print(f"[Gateway] Connected to MQTT broker: {MQTT_BROKER}", flush=True)
except Exception as e:
    print(f"[Gateway] MQTT connection warning: {e}", flush=True)

@app.get("/healthz")
def health_check():
    return {
        "status": "healthy",
        "backend": STORAGE_BACKEND,
        "storage_dir": str(LOCAL_STORAGE_DIR)
    }

# ==========================================
# 1. OTA (ファームウェア配信)
# ==========================================
@app.get("/ota/{device_type}/version")
def get_latest_version(device_type: str):
    """マイコンがアップデート前に最新バージョンを確認するエンドポイント"""
    version_file = LOCAL_STORAGE_DIR / "firmware" / device_type / "version.json"
    if version_file.exists():
        try:
            return json.loads(version_file.read_text())
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to read version file: {e}")
    return {"device_type": device_type, "version": "1.0.0", "binary": "firmware.bin"}

@app.api_route("/ota/{device_type}/{filename}", methods=["GET", "HEAD"])
def download_firmware(device_type: str, filename: str):
    """マイコンがバイナリ(.bin)をストリーミング取得するエンドポイント"""
    file_path = LOCAL_STORAGE_DIR / "firmware" / device_type / filename
    if not file_path.exists() or not file_path.is_file():
        raise HTTPException(status_code=404, detail="Firmware binary not found")

    file_size = file_path.stat().st_size

    # マイコンの OTA 処理に必須な Content-Length ヘッダーを正確に付与
    headers = {
        "Content-Disposition": f'attachment; filename="{filename}"',
        "Content-Length": str(file_size)
    }
    return FileResponse(path=file_path, media_type="application/octet-stream", headers=headers)

# ==========================================
# 2. カメラ画像アップロード
# ==========================================
@app.put("/camera/{device_id}/{filename}")
@app.post("/camera/{device_id}/{filename}")
async def upload_camera_image(device_id: str, filename: str, request: Request):
    """マイコン(ESP32-CAM等)がJPEG画像を直接送信するエンドポイント"""
    body = await request.body()
    if not body:
        raise HTTPException(status_code=400, detail="Empty payload")

    save_dir = LOCAL_STORAGE_DIR / "camera" / device_id
    save_dir.mkdir(parents=True, exist_ok=True)
    target_path = save_dir / filename

    with open(target_path, "wb") as f:
        f.write(body)

    file_size = len(body)
    print(f"[Gateway] Saved image from {device_id}: {filename} ({file_size} bytes)", flush=True)

    # 撮影完了イベントを EMQX へ Publish して既存パイプラインへ連携
    event_payload = {
        "device_id": device_id,
        "filename": filename,
        "size_bytes": file_size,
        "storage_path": f"/camera/{device_id}/{filename}",
        "timestamp": int(time.time()),
        "backend": STORAGE_BACKEND
    }
    try:
        mqtt_client.publish(f"devices/{device_id}/camera", json.dumps(event_payload))
    except Exception as e:
        print(f"[Gateway] Failed to publish MQTT event: {e}", flush=True)

    return {
        "status": "success",
        "device_id": device_id,
        "filename": filename,
        "size": file_size
    }
