# -*- coding: utf-8 -*-
"""视频语音转写：抽音频 → 千问 ASR（跟 bot 里同一套接口）。

限制：千问这个模型单次最长 180 秒。视频超过就分段转写再拼。
"""
import base64
import json
import os
import re
import subprocess
import time
import urllib.request

from settings import (FFMPEG, FFPROBE, CREDS, ASR_TMP as TMP)




ASR_URL = ("https://dashscope.aliyuncs.com/api/v1/services/"
           "aigc/multimodal-generation/generation")
MAX_SEC = 170          # 留 10 秒余量（上限 180）


def _cred(name):
    raw = open(CREDS, encoding="utf-8").read()
    m = re.search(r"(?m)^\s*%s:\s*(.+?)\s*$" % name, raw)
    if not m:
        raise RuntimeError("找不到 " + name)
    return m.group(1).strip().strip('"\'')


def probe_duration(video: str) -> float:
    r = subprocess.run(
        [FFPROBE, "-v", "error", "-show_entries", "format=duration",
         "-of", "default=nw=1:nk=1", video],
        capture_output=True, text=True, encoding="utf-8", errors="replace")
    try:
        return float(r.stdout.strip())
    except Exception:
        return 0.0


def extract_audio(video: str, start: float = 0.0, secs: float = None,
                  out: str = None) -> str:
    """抽 16k 单声道 wav。"""
    os.makedirs(TMP, exist_ok=True)
    if out is None:
        out = os.path.join(TMP, "%s_%.0f.wav"
                           % (re.sub(r"[^\w.-]", "_", os.path.basename(video))[:30], start))
    cmd = [FFMPEG, "-y", "-ss", "%.2f" % start]
    if secs:
        cmd += ["-t", "%.2f" % secs]
    cmd += ["-i", video, "-vn", "-ac", "1", "-ar", "16000", "-f", "wav", out]
    subprocess.run(cmd, capture_output=True)
    return out


def transcribe_wav(wav_path: str) -> dict:
    """wav → {"text", "emotion", "language", "secs", "usage"}"""
    wav = open(wav_path, "rb").read()
    b64 = base64.b64encode(wav).decode()
    body = {
        "model": "qwen3-asr-flash",
        "input": {"messages": [{"role": "user", "content": [
            {"audio": "data:audio/wav;base64," + b64},
        ]}]},
        "parameters": {"asr_options": {"language": "zh"}},
    }
    req = urllib.request.Request(
        ASR_URL,
        data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
        headers={"Authorization": "Bearer " + _cred("QWEN_API_KEY"),
                 "Content-Type": "application/json"})
    t0 = time.time()
    with urllib.request.urlopen(req, timeout=300) as r:
        j = json.loads(r.read().decode("utf-8"))
    dt = time.time() - t0
    msg = (((j.get("output") or {}).get("choices") or [{}])[0].get("message") or {})
    text = "".join(str(p.get("text") or "") for p in (msg.get("content") or [])).strip()
    ann = (msg.get("annotations") or [{}])[0] or {}
    return {
        "text": text,
        "emotion": str(ann.get("emotion") or "").strip().lower(),
        "language": str(ann.get("language") or "").strip(),
        "secs": dt,
        "usage": j.get("usage") or {},
    }


def transcribe(video: str) -> dict:
    """视频 → 全片语音转写（超过 170 秒自动分段拼接）。"""
    dur = probe_duration(video)
    if dur <= 0:
        return {"text": "", "emotion": "", "secs": 0.0, "usage": {}, "chunks": 0}

    if dur <= MAX_SEC:
        wav = extract_audio(video)
        r = transcribe_wav(wav)
        r["chunks"] = 1
        try:
            os.remove(wav)
        except Exception:
            pass
        return r

    # 分段
    parts = []
    total_secs = 0.0
    usage = {}
    n = int(dur // MAX_SEC) + 1
    for i in range(n):
        start = i * MAX_SEC
        if start >= dur:
            break
        wav = extract_audio(video, start, MAX_SEC, out=os.path.join(TMP, "part_%02d.wav" % i))
        r = transcribe_wav(wav)
        if r["text"]:
            parts.append(r["text"])
        total_secs += r["secs"]
        for k, v in (r["usage"] or {}).items():
            if isinstance(v, (int, float)):
                usage[k] = usage.get(k, 0) + v
        try:
            os.remove(wav)
        except Exception:
            pass
    return {"text": "".join(parts), "emotion": "", "secs": total_secs,
            "usage": usage, "chunks": n}


if __name__ == "__main__":
    import sys
    v = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("TEST_VIDEO") or "test.mp4"
    print("视频:", v)
    r = transcribe(v)
    print("分段: %d   耗时 %.2fs" % (r["chunks"], r["secs"]))
    print("用量:", json.dumps(r["usage"], ensure_ascii=False))
    print("转写 %d 字:" % len(r["text"]))
    print(r["text"][:500])
