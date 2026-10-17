# -*- coding: utf-8 -*-
"""B站：搜索 / 过滤 / 下载。不需要登录，用游客 cookie。

命令行测试：
    python bili.py "猫咪 搞笑"          # 搜索并列出候选
    python bili.py "猫咪 搞笑" --pick 0 # 下载第0个
"""
import json
import os
import sys
import time
import urllib.parse
import urllib.request

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
REFERER = "https://www.bilibili.com/"
from settings import CACHE_DIR as CACHE

# 过滤规则
MIN_SEC, MAX_SEC = 10, 600
MIN_PLAY = 10000
BAD_WORDS = ["合集", "直播回放", "P1", "P2", "P3", "完整版", "全集", "付费", "充电"]

_cookie = None
_cookie_at = 0


def _headers(cookie=None):
    h = {"User-Agent": UA, "Referer": REFERER}
    if cookie:
        h["Cookie"] = cookie
    return h


def _get(url, cookie=None, retries=2):
    last = None
    for i in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers=_headers(cookie))
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.loads(r.read().decode("utf-8"))
        except Exception as e:
            last = e
            if i < retries:
                time.sleep(1.2 + i)
    raise last


def guest_cookie(force=False):
    """领一个游客 cookie（buvid3/buvid4）。缓存 30 分钟。"""
    global _cookie, _cookie_at
    if not force and _cookie and time.time() - _cookie_at < 1800:
        return _cookie
    j = _get("https://api.bilibili.com/x/frontend/finger/spi")
    d = j.get("data") or {}
    if not d.get("b_3"):
        raise RuntimeError("领游客cookie失败: %s" % j)
    _cookie = "buvid3=%s; buvid4=%s" % (d["b_3"], d["b_4"])
    _cookie_at = time.time()
    return _cookie


def _secs(dur: str) -> int:
    try:
        p = [int(x) for x in str(dur).split(":")]
        return p[0] * 60 + p[1] if len(p) == 2 else (p[0] * 3600 + p[1] * 60 + p[2] if len(p) == 3 else 0)
    except Exception:
        return 0


def search(keyword: str, page: int = 1) -> list:
    """搜索视频，返回原始结果列表。"""
    ck = guest_cookie()
    q = urllib.parse.quote(keyword)
    url = ("https://api.bilibili.com/x/web-interface/search/type?"
           "search_type=video&keyword=%s&page=%d" % (q, page))
    try:
        j = _get(url, ck)
    except Exception:
        # 风控：换 cookie 再试一次
        ck = guest_cookie(force=True)
        j = _get(url, ck)
    if j.get("code") != 0:
        raise RuntimeError("搜索失败 code=%s msg=%s" % (j.get("code"), j.get("message")))
    return (j.get("data") or {}).get("result") or []


def _ok(it: dict) -> bool:
    secs = _secs(it.get("duration"))
    if not (MIN_SEC <= secs <= MAX_SEC):
        return False
    try:
        play = int(it.get("play") or 0)
    except Exception:
        play = 0
    if play < MIN_PLAY:
        return False
    title = (it.get("title") or "").replace('<em class="keyword">', "").replace("</em>", "")
    if any(b in title for b in BAD_WORDS):
        return False
    if not it.get("bvid"):
        return False
    return True


def find(keyword: str, limit: int = 5) -> list:
    """搜索 + 过滤，返回干净候选 [{bvid,title,secs,play,author}]。"""
    out, seen = [], set()
    for page in (1, 2):
        try:
            raw = search(keyword, page)
        except Exception:
            break
        for it in raw:
            if not _ok(it):
                continue
            bv = it["bvid"]
            if bv in seen:
                continue
            seen.add(bv)
            out.append({
                "bvid": bv,
                "title": (it.get("title") or "").replace('<em class="keyword">', "").replace("</em>", ""),
                "secs": _secs(it.get("duration")),
                "play": int(it.get("play") or 0),
                "author": it.get("author") or "",
            })
            if len(out) >= limit:
                return out
        time.sleep(1.0)
    return out


def video_info(bvid: str) -> dict:
    j = _get("https://api.bilibili.com/x/web-interface/view?bvid=" + bvid)
    if j.get("code") != 0:
        raise RuntimeError("取视频信息失败 code=%s" % j.get("code"))
    d = j.get("data") or {}
    pages = d.get("pages") or [{}]
    return {
        "bvid": bvid,
        "title": d.get("title"),
        "duration": d.get("duration"),
        "cid": pages[0].get("cid"),
        "view": (d.get("stat") or {}).get("view"),
    }


def quality_for(duration: float = 0) -> int:
    """一律 720P（不登录能拿到的最高档）。"""
    return 64


def download(bvid: str, outdir: str = CACHE, qn: int = 0) -> str:
    """下完整 MP4（durl，一个文件，音画都有）。

    qn=0 时按视频长度自动选清晰度（见 quality_for）。
    """
    os.makedirs(outdir, exist_ok=True)
    path = os.path.join(outdir, bvid + ".mp4")
    if os.path.exists(path):
        return path

    info = video_info(bvid)
    if not qn:
        qn = quality_for(info.get("duration") or 0)
    ck = guest_cookie()
    url = ("https://api.bilibili.com/x/player/playurl?"
           "bvid=%s&cid=%s&fnval=1&qn=%d" % (bvid, info["cid"], qn))
    j = _get(url, ck)
    if j.get("code") != 0:
        raise RuntimeError("取播放地址失败 code=%s msg=%s" % (j.get("code"), j.get("message")))
    durl = (j.get("data") or {}).get("durl") or []
    if not durl:
        raise RuntimeError("没有可下载的流（可能需要登录）")
    src = durl[0].get("url")
    expect = int(durl[0].get("size") or 0)

    req = urllib.request.Request(src, headers=_headers(ck))
    tmp = path + ".part"
    with urllib.request.urlopen(req, timeout=180) as r, open(tmp, "wb") as f:
        while True:
            chunk = r.read(256 * 1024)
            if not chunk:
                break
            f.write(chunk)
    got = os.path.getsize(tmp)
    if expect and got != expect:
        os.remove(tmp)
        raise RuntimeError("下载不完整 %d/%d" % (got, expect))
    os.replace(tmp, path)
    return path


if __name__ == "__main__":
    kw = sys.argv[1] if len(sys.argv) > 1 else "猫咪 搞笑"
    print("搜索:", kw)
    cands = find(kw)
    if not cands:
        print("  没有合格候选")
        sys.exit(0)
    for i, c in enumerate(cands):
        print("  [%d] %-14s %3ds  播放%-8d %s"
              % (i, c["bvid"], c["secs"], c["play"], c["title"][:36]))
    if "--pick" in sys.argv:
        idx = int(sys.argv[sys.argv.index("--pick") + 1])
        bv = cands[idx]["bvid"]
        print("\n下载:", bv)
        t0 = time.time()
        p = download(bv)
        print("  %s  %.2f MB  %.1fs" % (p, os.path.getsize(p) / 1024 / 1024, time.time() - t0))
