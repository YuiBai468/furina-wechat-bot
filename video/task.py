# -*- coding: utf-8 -*-
"""她自己去 B 站挑一个视频看。

    from task import pick_and_watch
    r = pick_and_watch("猫咪 搞笑")
    print(r["bvid"], r["summary"])

三层隔离：
    ① 搜索候选 → 她选（只给标题，不带别的上下文）
    ② 视频 → 语音 + 画面（只装这个视频的原料）
    ③ 返回结果（上层拿去生成分享，不带视频原材料）

任何一步失败都静默放弃，不发半截。
"""
import json
import random
import re
import time
import urllib.request

import bili
import frames
import pipeline as P
import summarize as S

MAX_CANDIDATES = 6
FALLBACK_KEYWORDS = ["搞笑 短片", "可爱 动物", "沙雕 日常", "治愈 动画"]


def _ds_call(prompt, max_tokens=200):
    body = {
        "model": "deepseek-flash",
        "max_tokens": max_tokens,
        "thinking": {"type": "disabled"},
        "messages": [{"role": "user", "content": prompt}],
    }
    req = urllib.request.Request(
        "https://api.deepseek.com/chat/completions",
        data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
        headers={"Authorization": "Bearer " + S._key(),
                 "Content-Type": "application/json"},
        method="POST")
    with urllib.request.urlopen(req, timeout=120) as r:
        return json.loads(r.read().decode("utf-8"))


def ask_her_to_pick(cands, keyword):
    """把候选发给她，让她选一个。只回答序号。"""
    listing = "\n".join(
        "%d. %s（%d 秒，%d 播放）" % (i + 1, c["title"][:40], c["secs"], c["play"])
        for i, c in enumerate(cands))
    prompt = (
        "你刚在B站搜了「%s」，下面是搜出来的视频：\n\n%s\n\n"
        "选一个你最想看的。只回答一个数字序号（1~%d），不要别的字。"
        % (keyword, listing, len(cands))
    )
    try:
        j = _ds_call(prompt, max_tokens=16)
        txt = (j["choices"][0]["message"].get("content") or "").strip()
        m = re.search(r"\d+", txt)
        if m:
            idx = int(m.group()) - 1
            if 0 <= idx < len(cands):
                return cands[idx], {"raw": txt, "how": "llm"}
    except Exception as e:
        pass
    # 兜底：随机挑
    return random.choice(cands), {"raw": "", "how": "random"}


def judge_share(title: str, summary: str) -> dict:
    """她看完之后判断：这个值不值得发给他。

    判据不是「内容健不健康」，是「这东西拿不拿得出手」——按她的品味来。
    返回 {"worth": bool, "raw": str, "secs": float}
    判断失败时默认「发」——宁可发一条普通的，也别白看一场。
    """
    prompt = (
        "你刚在B站看完一个视频，正在犹豫要不要发给他看。\n\n"
        "标题：%s\n你看到的内容：%s\n\n"
        "发之前先问自己一句：这东西拿得出手吗？\n\n"
        "你见过的戏、看过的场面太多了。有些东西你根本不会往他那儿转——\n"
        "不是因为它「不健康」，是因为它**太掉价**：擦边的、low的、\n"
        "让人替它尴尬的、看完只想说「这什么玩意儿」的。\n"
        "这种东西你转过去，丢的是你自己的脸。\n\n"
        "但也有另一种情况：东西本身一般，可你知道他会笑、会喜欢、\n"
        "或者会正好戳中他。那种就值得发。\n\n"
        "只回答两个字：发 或 不发"
        % (title or "", (summary or "")[:1200])
    )
    t = time.time()
    try:
        j = _ds_call(prompt, max_tokens=16)
        txt = (j["choices"][0]["message"].get("content") or "").strip()
        # 兼容「发/不发」「值得/不值得」两种回答
        ans = txt.replace(" ", "")
        worth = not ("不发" in ans or "不值得" in ans)
        return {"worth": worth, "raw": txt, "secs": time.time() - t}
    except Exception as e:
        return {"worth": True, "raw": "", "secs": time.time() - t,
                "error": str(e)[:120]}


def search_with_fallback(keyword):
    """搜索；0 条就换通用词。返回 (候选列表, 实际用的词)。"""
    cands = bili.find(keyword, limit=MAX_CANDIDATES)
    if cands:
        return cands, keyword
    for kw in FALLBACK_KEYWORDS:
        try:
            cands = bili.find(kw, limit=MAX_CANDIDATES)
        except Exception:
            continue
        if cands:
            return cands, kw
        time.sleep(0.8)
    return [], keyword


def pick_and_watch(keyword: str, verbose: bool = True, max_tries: int = 3) -> dict:
    """她搜 → 她选 → 下载 → 看 → 自己判断值不值得分享 → 不值得就重走。

    最多看 max_tries 个，全不满意就放弃（静默，不发半截）。
    返回 {"ok", "bvid", "title", "url", "summary", "cost_cny", "secs", "tries"}
    """
    t_all = time.time()
    out = {"ok": False, "keyword": keyword, "tries": 0, "seen": []}
    watched = set()
    total_cost = 0.0
    FALLBACK = ["搞笑 短片", "可爱 动物", "沙雕 日常", "治愈 动画", "离谱 瞬间"]
    fb_i = 0

    for attempt in range(max_tries):
        # ① 搜索
        try:
            cands, used_kw = search_with_fallback(keyword)
        except Exception as e:
            out["error"] = "搜索失败: " + str(e)[:100]
            break
        cands = [c for c in cands if c["bvid"] not in watched]
        # 候选都看过了 → 换个通用词再搜
        if not cands:
            while fb_i < len(FALLBACK):
                try:
                    cands, used_kw = search_with_fallback(FALLBACK[fb_i])
                except Exception:
                    cands = []
                fb_i += 1
                cands = [c for c in cands if c["bvid"] not in watched]
                if cands:
                    break
            if not cands:
                out["error"] = "没有没看过的候选了"
                break
        out["tries"] = attempt + 1
        if verbose:
            print("  [第%d次] 搜到 %d 个新候选（用词：%s）" % (attempt + 1, len(cands), used_kw))

        # ② 她选
        chosen, how = ask_her_to_pick(cands, used_kw)
        watched.add(chosen["bvid"])
        out["seen"].append(chosen["bvid"])
        if verbose:
            print("      她选了: [%s] %s (%d秒)" % (chosen["bvid"], chosen["title"][:30], chosen["secs"]))

        # ③ 下载
        try:
            t = time.time()
            path = bili.download(chosen["bvid"])
            if verbose:
                print("      下载 %.1f MB / %.1fs" % (__import__("os").path.getsize(path) / 1024 / 1024, time.time() - t))
        except Exception as e:
            if verbose:
                print("      下载失败，换下一个: %s" % str(e)[:60])
            continue

        # ④ 语音 + 画面 + 总结
        try:
            r = P.watch(path)
        except Exception as e:
            if verbose:
                print("      处理失败，换下一个: %s" % str(e)[:60])
            continue
        if r.get("error") or not r.get("summary"):
            if verbose:
                print("      总结失败，换下一个")
            continue
        total_cost += r.get("cost_cny") or 0.0

        # ⑤ 她自己判断值不值得发
        verdict = judge_share(chosen["title"], r["summary"])
        out["last_verdict"] = verdict
        if verbose:
            print("      她觉得: %s（%s）"
                  % ("值得发" if verdict["worth"] else "不值得，换一个",
                     (verdict.get("raw") or "?")[:16]))
        if not verdict["worth"]:
            continue

        # ⑥ 通过
        out.update({
            "ok": True,
            "bvid": chosen["bvid"],
            "title": chosen["title"],
            "url": "https://b23.tv/" + chosen["bvid"],
            "summary": r["summary"],
            "duration": r.get("duration", 0),
            "n_grids": len(r.get("grids") or []),
            "cost_cny": round(total_cost, 4),
            "secs": round(time.time() - t_all, 1),
        })
        return out

    out["cost_cny"] = round(total_cost, 4)
    out["secs"] = round(time.time() - t_all, 1)
    if not out.get("error"):
        out["error"] = "看了 %d 个都不满意" % out["tries"]
    return out


if __name__ == "__main__":
    import sys
    kw = sys.argv[1] if len(sys.argv) > 1 else "猫咪 搞笑"
    print("她说想搜:", kw)
    print("=" * 66)
    r = pick_and_watch(kw)
    print("=" * 66)
    if not r.get("ok"):
        print("放弃:", r.get("error"))
        raise SystemExit(1)
    print("标题   ", r["title"])
    print("链接   ", r["url"])
    print("时长   %.1f 秒   九宫格 %d 张" % (r["duration"], r["n_grids"]))
    print("成本   ¥%.4f" % r["cost_cny"])
    print("总耗时 %.1f 秒" % r["secs"])
    print("=" * 66)
    print(r["summary"])
