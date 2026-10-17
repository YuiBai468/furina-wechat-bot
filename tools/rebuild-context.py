# -*- coding: utf-8 -*-
"""从 wechat-bot.log 重建「主人」私聊的完整历史，写回 contexts.json。

之前在跑的是 12 条滚动窗口，早于最近的对话**已经被删掉了**；
日志里还留着原文，所以能重建。

用法：
    python rebuild-context.py            # 只预览，不写
    python rebuild-context.py --write    # 写回（自动备份）
"""
import json, os, re, shutil, sys, time

LOG = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs", "wechat-bot.log")
CTX = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "contexts.json")
CHAT = "主人"

LINE_TS = re.compile(r"^\[(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):\d{2}\]")
INBOUND = re.compile(r"入站 \[私聊\] " + CHAT + r" / [^:]+: (.*)$")
OUTBOUND = re.compile(r"出站 -> " + CHAT + r"（\d+ 个气泡")
BUBBLE = re.compile(r"\[(\d+)/(\d+)\]\s+(.*?)\s+->\s+\{")


def mmddhhmm(ts):
    return f"[{ts[5:10]} {ts[11:16]}]"


def main():
    write = "--write" in sys.argv
    msgs = []
    cur = []          # 正在收集的一轮出站气泡
    total = 0
    with open(LOG, encoding="utf-8") as f:
        for line in f:
            total += 1
            m = LINE_TS.match(line)
            if not m:
                continue
            ts = m.group(0)[1:-1]

            mi = INBOUND.search(line)
            if mi:
                if cur:
                    msgs.append(("assistant", "\n\n".join(cur), cur_ts))
                    cur = []
                msgs.append(("user", mi.group(1).strip(), ts))
                continue

            if OUTBOUND.search(line):
                if cur:
                    msgs.append(("assistant", "\n\n".join(cur), cur_ts))
                cur = []
                cur_ts = ts
                continue

            mb = BUBBLE.search(line)
            if mb and cur is not None:
                text = mb.group(3).strip()
                if text:
                    cur.append(text)
                continue

    if cur:
        msgs.append(("assistant", "\n\n".join(cur), cur_ts))

    # 去掉连续重复（重试会打两条一样的）
    dedup = []
    for role, text, ts in msgs:
        if dedup and dedup[-1][0] == role and dedup[-1][1] == text:
            continue
        dedup.append((role, text, ts))

    # 合并连续同角色（一条入站后跟多条出站之类）—— 保持交替更稳
    merged = []
    for role, text, ts in dedup:
        if merged and merged[-1][0] == role:
            merged[-1][1] += "\n\n" + text
        else:
            merged.append([role, text, ts])

    final = []
    for role, text, ts in merged:
        prefix = mmddhhmm(ts) if role == "user" else f"[{ts[11:16]}]"
        final.append({"role": role, "content": f"{prefix} {text}"})

    chars = sum(len(x["content"]) for x in final)
    n_user = sum(1 for x in final if x["role"] == "user")
    n_asst = sum(1 for x in final if x["role"] == "assistant")

    print(f"日志行数     : {total}")
    print(f"重建消息     : {len(final)} 条（他 {n_user} / 她 {n_asst}）")
    print(f"总字符       : {chars:,}")
    print(f"首条         : {final[0]['content'][:60] if final else '-'}")
    print(f"末条         : {final[-1]['content'][:60] if final else '-'}")
    print()
    print("最后 3 条预览：")
    for x in final[-3:]:
        print(f"   {x['role']:9} {x['content'][:80]!r}")

    if not write:
        print("\n（预览模式，没写。要写回加 --write）")
        return

    shutil.copy2(CTX, CTX + f".bak-{time.strftime('%Y%m%d-%H%M%S')}-rebuild")
    with open(CTX, encoding="utf-8") as f:
        data = json.load(f)
    old = len(data.get(CHAT, []))
    data[CHAT] = final
    tmp = CTX + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=1)
    os.replace(tmp, CTX)
    print(f"\n已写回：{CHAT} {old} 条 → {len(final)} 条（其余会话未动）")


if __name__ == "__main__":
    main()
