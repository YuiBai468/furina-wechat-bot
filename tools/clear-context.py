# -*- coding: utf-8 -*-
"""清空对话上下文（她失忆，从头开始）。

用法：
    python clear-context.py              # 清所有会话的对话历史
    python clear-context.py --all        # 连长期记忆 memory.json 一起清
    python clear-context.py --chat 主人   # 只清某一个会话

⚠️ 一定要**先停 bot 再清** —— 否则下一条消息会把清掉的内容又写回去。
   脚本会检查 bot 是否在跑并提醒。
⚠️ 全程用 Python 写文件，不用 PowerShell 的 Set-Content：
   那个会写 UTF-8 BOM，而 bot 的 json.load 读不了（实测报
   "Unexpected UTF-8 BOM"）。
"""
import json, os, shutil, sys, time

# ⚠️ Windows 控制台默认是 GBK，打印 emoji / 特殊符号会直接 UnicodeEncodeError
#    崩掉（实测崩过一次，导致"看似清了其实没清"）。这里强制走 UTF-8。
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

DIR = os.path.dirname(os.path.abspath(__file__))
CTX = os.path.join(DIR, "contexts.json")
MEM = os.path.join(DIR, "memory.json")
STAMP = time.strftime("%Y%m%d-%H%M%S")


def bot_running():
    """判断 bot 在不在跑。

    ⚠️ 只看"日志 mtime 是否新"会误报 —— 刚停掉 bot 的时候日志 mtime
    也就是几秒前，会一直提示"可能正在跑"。改成**看日志还会不会继续长**。
    """
    log = os.path.normpath(os.path.join(DIR, "..", "..", "logs", "wechat-bot.log"))
    try:
        first = os.path.getsize(log)
    except OSError:
        return False
    time.sleep(3)
    try:
        return os.path.getsize(log) != first
    except OSError:
        return False


def backup(path):
    if not os.path.exists(path):
        return None
    dst = f"{path}.bak-{STAMP}-clear"
    shutil.copy2(path, dst)
    return dst


def main():
    only = None
    if "--chat" in sys.argv:
        i = sys.argv.index("--chat")
        if i + 1 < len(sys.argv):
            only = sys.argv[i + 1]
    clear_mem = "--all" in sys.argv

    if bot_running():
        print("⚠️  日志 2 分钟内还在写 —— bot 可能正在跑。")
        print("    先停 bot 再清，否则下一条消息会把内容写回去。")
        print("    （要照做：先 Stop-Process，再跑本脚本，最后重启）")
        print()

    b = backup(CTX)
    print(f"备份 contexts.json : {b}" if b else "contexts.json 不存在，跳过")

    data = {}
    if os.path.exists(CTX):
        try:
            with open(CTX, encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            print(f"  读取失败（当成空的处理）：{e}")
            data = {}

    if only:
        n = len(data.get(only, []))
        data.pop(only, None)
        print(f"只清「{only}」：{n} 条 → 0（其余会话保留）")
    else:
        for k in list(data.keys()):
            print(f"  {k}: {len(data[k])} 条 → 0")
        data = {}

    with open(CTX, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=1)
    raw = open(CTX, "rb").read()
    print(f"已写入 contexts.json：{raw!r}  BOM={raw[:3] == b'\xef\xbb\xbf'}")

    if clear_mem:
        if os.path.exists(MEM):
            bm = backup(MEM)
            print(f"备份 memory.json   : {bm}")
            os.remove(MEM)
            print("长期记忆已清")
        else:
            print("memory.json 本来就没有")
    elif os.path.exists(MEM):
        print(f"注意：memory.json 还在（{os.path.getsize(MEM)} 字节）。"
              f"要一起清就加 --all")

    print("\n清完了。记得**重启 bot** 让内存里的旧窗口也丢掉。")


if __name__ == "__main__":
    main()
