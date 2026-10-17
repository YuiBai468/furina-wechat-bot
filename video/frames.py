# -*- coding: utf-8 -*-
"""抽帧 + 拼图：每 5 秒抽 9 帧，一张九宫格。

用法：
    from frames import process_video
    grids = process_video(r"D:\path\video.mp4")
    for g in grids:
        print(g["sheet"], g["timestamps"])

产物：
    视频 0~5s   → 一张九宫格（9 帧，约 0.55 秒一帧）
    视频 5~10s  → 第二张
    …
"""
import math
import os
import re
import subprocess

from PIL import Image, ImageDraw, ImageFont

from settings import (FFMPEG, FFPROBE, FRAMES_TMP as WORK_TMP)



SEGMENT_SEC  = 5.0    # 默认每段时长（视频 ≤1 分钟时用）
PER_SEGMENT  = 9      # 每段抽几帧
MAX_SEGMENTS = 60     # 上限只为防超长片
# 一格 1280：720P 源不缩放。云端图片 token 封顶，
# 拼图放大到 4608×4608 也还是 992 token，所以格子越大越赚。
CELL         = 1280
COLS         = 3

# 帧间隔按视频长度分三档；清晰度统一 720P（token 封顶，720P 不加钱）
#   0~2 分钟   5 秒/张
#   2~4 分钟   7.5 秒/张
#   4~6 分钟   10 秒/张
SEGMENT_RULES = [
    (120.0, 5.0),      # 0~2 分钟   → 5 秒/张   → 24 张
    (240.0, 7.5),      # 2~4 分钟   → 7.5 秒/张 → 32 张
    (360.0, 10.0),     # 4~6 分钟   → 10 秒/张  → 36 张
    (480.0, 12.5),     # 6~8 分钟   → 12.5 秒/张→ 39 张
    (600.0, 15.0),     # 8~10 分钟  → 15 秒/张  → 40 张
]
CELL = 1280          # 720P 长边，源帧不缩放
MAX_GRIDS = 40       # 一次请求最多带多少张（实测 40 张 @72 质量 = 40.7MB，能发）
QN_ALWAYS = 64       # 一律 720P


def segment_for(duration: float) -> float:
    """每段多长。"""
    for limit, seg in SEGMENT_RULES:
        if duration <= limit:
            return seg
    return SEGMENT_RULES[-1][1]


def quality_for(duration: float) -> int:
    """下载清晰度：一律 720P（qn=64）。"""
    return QN_ALWAYS


def cell_for(duration: float) -> int:
    """九宫格每格多大：720P 长边 1280。"""
    return CELL


FONT_CANDIDATES = [
    r"C:\Windows\Fonts\msyh.ttc",
    r"C:\Windows\Fonts\simhei.ttf",
    r"C:\Windows\Fonts\arial.ttf",
]


def _font(size=26):
    for p in FONT_CANDIDATES:
        if os.path.exists(p):
            try:
                return ImageFont.truetype(p, size)
            except Exception:
                continue
    return ImageFont.load_default()


def probe_duration(video: str) -> float:
    r = subprocess.run(
        [FFPROBE, "-v", "error", "-show_entries", "format=duration",
         "-of", "default=nw=1:nk=1", video],
        capture_output=True, text=True, encoding="utf-8", errors="replace")
    try:
        return float(r.stdout.strip())
    except Exception:
        return 0.0


def _fmt(t: float) -> str:
    m, s = divmod(int(round(t)), 60)
    return "%d:%02d" % (m, s)


def _grab_segment(video: str, outdir: str, start: float, secs: float, n: int, cell: int = CELL):
    """从 [start, start+secs) 里均匀抽 n 帧，返回 [(绝对时间, 路径)]。"""
    fps = n / secs
    tag = "seg%05.1f" % start
    pattern = os.path.join(outdir, tag + "_%02d.jpg")
    cmd = [FFMPEG, "-y",
           "-ss", "%.3f" % start, "-t", "%.3f" % secs,
           "-i", video,
           "-vf", "fps=%.4f,scale=%d:%d:force_original_aspect_ratio=decrease"
                  % (fps, cell, cell),
           "-q:v", "2", pattern]
    subprocess.run(cmd, capture_output=True, text=True,
                   encoding="utf-8", errors="replace")
    files = sorted(f for f in os.listdir(outdir)
                   if f.startswith(tag + "_") and f.endswith(".jpg"))
    out = []
    for i, f in enumerate(files[:n]):
        # 第 i 帧对应段内 (i+0.5)*secs/n
        t = start + (i + 0.5) * secs / max(n, 1)
        out.append((t, os.path.join(outdir, f)))
    return out


def make_grid(frames, timestamps, out_path: str, cols=COLS, cell=None) -> str:
    cell = cell or CELL
    rows = (len(frames) + cols - 1) // cols
    canvas = Image.new("RGB", (cols * cell, rows * cell), (20, 20, 20))
    draw = ImageDraw.Draw(canvas)
    font = _font(26)
    for i, (fp, t) in enumerate(zip(frames, timestamps)):
        try:
            im = Image.open(fp).convert("RGB")
        except Exception:
            continue
        im.thumbnail((cell, cell))
        gx, gy = (i % cols) * cell, (i // cols) * cell
        canvas.paste(im, (gx + (cell - im.width) // 2, gy + (cell - im.height) // 2))
        label = "t=" + _fmt(t)
        bx, by = gx + 6, gy + 6
        bb = draw.textbbox((bx, by), label, font=font)
        draw.rectangle([bb[0] - 3, bb[1] - 2, bb[2] + 3, bb[3] + 2], fill=(0, 0, 0))
        draw.text((bx, by), label, fill=(255, 235, 60), font=font)
        draw.rectangle([gx, gy, gx + cell - 1, gy + cell - 1], outline=(70, 70, 70))
    canvas.save(out_path, quality=72)
    return out_path


def process_video(video: str, outdir: str = None,
                  segment: float = None,
                  per_segment: int = PER_SEGMENT,
                  max_segments: int = MAX_SEGMENTS) -> dict:
    """视频 → 若干张九宫格。

    segment=None 时按视频长度自适应（1分钟5秒/张 … 5分钟15秒/张）。
    返回 {"grids": [{"sheet","timestamps","start","end"}], "duration", "segments"}
    """
    if outdir is None:
        base = os.path.join(WORK_TMP, re.sub(r"[^\w.-]", "_", os.path.basename(video))[:40])
        os.makedirs(WORK_TMP, exist_ok=True)
        outdir = base
    os.makedirs(outdir, exist_ok=True)

    if not os.path.isfile(video):
        raise FileNotFoundError('视频不存在: %s' % video)
    duration = probe_duration(video)
    if duration <= 0:
        raise RuntimeError('读不出视频时长（文件可能损坏）: %s' % video)

    cell = cell_for(duration)
    if segment is None:
        segment = segment_for(duration)
    else:
        try:
            segment = float(segment)
        except Exception:
            segment = SEGMENT_SEC

    n_seg = int(math.ceil(duration / segment))
    # 超过 MAX_GRIDS 就拉大间隔，保证一次请求塞得下
    if n_seg > MAX_GRIDS:
        segment = duration / MAX_GRIDS
        n_seg = MAX_GRIDS
    n_seg = max(1, min(n_seg, max_segments))

    grids = []
    for i in range(n_seg):
        start = i * segment
        secs = min(segment, max(duration - start, 0.5))
        picked = _grab_segment(video, outdir, start, secs, per_segment, cell)
        if not picked:
            continue
        frames = [p for _, p in picked]
        stamps = [t for t, _ in picked]
        sheet = os.path.join(outdir, "sheet_%02d.jpg" % i)
        make_grid(frames, stamps, sheet, cell=cell)
        grids.append({
            "sheet": sheet,
            "timestamps": stamps,
            "start": start,
            "end": start + secs,
        })

    return {"grids": grids, "duration": duration, "segments": n_seg, "outdir": outdir}


if __name__ == "__main__":
    import sys
    v = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("TEST_VIDEO") or "test.mp4"
    print("视频:", v)
    r = process_video(v)
    print("时长: %.1fs   段数: %d" % (r["duration"], r["segments"]))
    for i, g in enumerate(r["grids"]):
        print("  段%d  %.1f~%.1fs  时间戳 %s"
              % (i, g["start"], g["end"],
                 " ".join("%.1f" % t for t in g["timestamps"])))
        print("        拼图: %s  %.0f KB"
              % (g["sheet"], os.path.getsize(g["sheet"]) / 1024))
