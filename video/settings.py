"""视频管线的路径配置。

全部可以用环境变量覆盖；不设就用仓库内的相对路径。
"""
import os

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)

# 数据目录（缓存视频、抽帧临时文件）
DATA_DIR = os.environ.get("FURINA_DATA_DIR") or os.path.join(ROOT, "data")
CACHE_DIR = os.environ.get("FURINA_CACHE_DIR") or os.path.join(DATA_DIR, "bili")
FRAMES_TMP = os.environ.get("FURINA_FRAMES_TMP") or os.path.join(DATA_DIR, "_frames")
ASR_TMP = os.environ.get("FURINA_ASR_TMP") or os.path.join(DATA_DIR, "_asr")

# 凭据文件（默认去 DSH 的凭据库拿；也可以用自己的 yaml）
CREDS = os.environ.get("FURINA_CREDENTIALS") or os.path.expanduser("~/.dsh/.credentials.yaml")

# ffmpeg / ffprobe：默认走 PATH，找不到再改环境变量指绝对路径
FFMPEG = os.environ.get("FFMPEG_BIN") or "ffmpeg"
FFPROBE = os.environ.get("FFPROBE_BIN") or "ffprobe"


def ensure_dirs():
    """把需要的目录建出来。"""
    for d in (DATA_DIR, CACHE_DIR, FRAMES_TMP, ASR_TMP):
        os.makedirs(d, exist_ok=True)
