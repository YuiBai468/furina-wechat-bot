# -*- coding: utf-8 -*-
"""云端总结：把多张九宫格 + 语音转写 → 一段中文视频描述。

隔离原则：这里不带任何聊天历史，只装这个视频的原料。
"""
import base64
import json
import re
import time
import urllib.request

DS_URL = "https://api.deepseek.com/chat/completions"
from settings import CREDS

HINT = (
    "\n\n【人物识别】如果画面里是原神角色，对照一下再写名字：\n"
    "· 芙宁娜：白蓝短发、蓝色礼帽、深蓝礼服\n"
    "· 那维莱特：白色长发、身形高大、深蓝白相间长礼服\n"
    "· 旅行者（空）：金发、长发编成辫子、深色上衣配白金色装饰\n"
    "· 旅行者（荧）：金发、金色短发、白色衣裙\n"
    "认不准就只描述外貌（发色、穿着），不要硬猜名字。"
)

PROMPT = (
    "下面是一个视频的若干张截图拼图，每张是 3×3 九宫格，"
    "每格左上角有绝对时间戳（t=分:秒）。视频按时间顺序切成 5 秒一段，"
    "每段一张图。\n\n"
    "请用中文写一段这个视频的内容描述，要求：\n"
    "· 500 字以内，一段话，不要分点、不要小标题\n"
    "· 【必须】每一张图都认真看完，不要只看前几张就开始写\n"
    "· 注意图里的小字、字幕、界面文字 —— 这些往往是关键信息，必须读出来\n"
    "· 按时间顺序讲发生了什么，前因后果连贯，不要逐格罗列\n"
    "· 不要用「这个视频」「本视频」开头，直接说内容\n"
    "· 不要评价好坏，只描述"
    + HINT
)

PROMPT_WITH_VOICE = (
    "下面是一个视频的语音转写，以及若干张截图拼图（每张 3×3，"
    "每格左上角有绝对时间戳，视频按 5 秒一段切分）。\n\n"
    "【语音转写】\n%s\n\n"
    "请用中文写一段这个视频的内容描述，要求：\n"
    "· 500 字以内，一段话，不要分点、不要小标题\n"
    "· 【必须】每一张图都认真看完，不要只看前几张就开始写\n"
    "· 注意图里的小字、字幕、界面文字 —— 这些往往是关键信息，必须读出来\n"
    "· 按时间顺序讲，前因后果连贯，不要逐段复述语音\n"
    "· 语音内容为主，画面描述用来补充场景\n"
    "· 不要用「这个视频」「本视频」开头，直接说内容\n"
    "· 不要评价好坏，只描述"
    + HINT
)


def _key(env="DEEPSEEK_API_KEY"):
    raw = open(CREDS, encoding="utf-8").read()
    m = re.search(r"(?m)^\s*%s:\s*(.+?)\s*$" % env, raw)
    if not m:
        raise RuntimeError("找不到 %s" % env)
    return m.group(1).strip().strip('"\'')


def _b64(path):
    return base64.b64encode(open(path, "rb").read()).decode()


def summarize(grid_paths, transcript=None, max_tokens=1000) -> dict:
    """九宫格列表 + 可选语音转写 → 中文描述。

    返回 {"text": 描述, "usage": {...}, "cost_cny": float, "secs": float}
    """
    if not grid_paths:
        raise RuntimeError("没有图，拒绝调用（空输入会让模型编造内容）")

    prompt = PROMPT_WITH_VOICE % (transcript or "（无）") if transcript else PROMPT
    content = [{"type": "text", "text": prompt}]
    for p in grid_paths:
        content.append({
            "type": "image_url",
            "image_url": {"url": "data:image/jpeg;base64," + _b64(p)},
        })

    body = {
        "model": "deepseek-flash",
        "max_tokens": max_tokens,
        # 总结不需要思考，开着会把 token 全烧在推理上、正文空白
        "thinking": {"type": "disabled"},
        "messages": [{"role": "user", "content": content}],
    }
    req = urllib.request.Request(
        DS_URL,
        data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
        headers={"Authorization": "Bearer " + _key(),
                 "Content-Type": "application/json"},
        method="POST")
    t0 = time.time()
    with urllib.request.urlopen(req, timeout=300) as r:
        j = json.loads(r.read().decode("utf-8"))
    secs = time.time() - t0
    u = j.get("usage") or {}
    pin = u.get("prompt_tokens", 0)
    pout = u.get("completion_tokens", 0)
    cost = pin * 0.30 / 1e6 + pout * 1.20 / 1e6
    msg = j["choices"][0]["message"]
    return {
        "text": (msg.get("content") or "").strip(),
        "usage": u,
        "cost_cny": cost * 7.25,
        "secs": secs,
        "finish": j["choices"][0].get("finish_reason"),
    }


if __name__ == "__main__":
    import sys
    import os
    import frames as F

    v = os.environ.get("TEST_VIDEO") or "test.mp4"
    for a in sys.argv[1:]:
        if os.path.isfile(a):
            v = a
            break
    print("视频:", v)
    fr = F.process_video(v)
    print("时长 %.1fs → %d 张九宫格" % (fr["duration"], len(fr["grids"])))
    paths = [g["sheet"] for g in fr["grids"]]
    r = summarize(paths)
    print("\n耗时 %.2fs   输入 %d tok  输出 %d tok   成本 ¥%.4f"
          % (r["secs"], r["usage"].get("prompt_tokens", 0),
             r["usage"].get("completion_tokens", 0), r["cost_cny"]))
    print("finish:", r["finish"])
    print("=" * 60)
    print(r["text"])
