"""All settings come from the environment; nothing else reads os.environ."""
import os
from pathlib import Path

MEDIA_ROOT = Path(os.environ.get("MEDIA_ROOT", Path(__file__).resolve().parent.parent / "data")).resolve()
ASR_MODEL = os.environ.get("MEDIA_ASR_MODEL", "large-v3")
ASR_DEVICE = os.environ.get("MEDIA_ASR_DEVICE", "cuda")
ASR_COMPUTE = os.environ.get("MEDIA_ASR_COMPUTE", "float16")
ASR_BATCH = int(os.environ.get("MEDIA_ASR_BATCH", "16"))
ALLOWED_HOSTS = {"www.youtube.com", "youtube.com", "m.youtube.com", "youtu.be"}
