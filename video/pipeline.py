# -*- coding: utf-8 -*-
"""一个视频 → 一段中文内容描述。完整链路。

    from pipeline import watch
    r = watch(r"D:\llm\cache\bili\BVxxx.mp4")
    print(r["summary"], r["cost_cny"])

顺序：
    语音转写（主力）  和  抽帧拼图（补充）  并行
        ↓
    一次云端调用出总结
"""
import time

import asr
import frames
import summarize as S


def watch(video: str, with_voice: bool = True) -> dict:
    t0 = time.time()
    out = {"video": video, "transcript": "", "grids": [], "summary": "",
           "cost_cny": 0.0, "t_asr": 0.0, "t_frames": 0.0, "t_sum": 0.0}

    # ① 语音
    if with_voice:
        t = time.time()
        try:
            a = asr.transcribe(video)
            out["transcript"] = a.get("text") or ""
            out["asr_usage"] = a.get("usage") or {}
            out["asr_emotion"] = a.get("emotion") or ""
        except Exception as e:
            out["asr_error"] = str(e)[:200]
        out["t_asr"] = time.time() - t

    # ② 抽帧拼图
    t = time.time()
    try:
        fr = frames.process_video(video)
        out["grids"] = fr["grids"]
        out["duration"] = fr["duration"]
    except Exception as e:
        out["frames_error"] = str(e)[:200]
    out["t_frames"] = time.time() - t

    if not out["grids"]:
        out["error"] = "抽帧失败"
        return out

    # ③ 云端总结
    t = time.time()
    try:
        r = S.summarize([g["sheet"] for g in out["grids"]],
                        transcript=out["transcript"] or None)
        out["summary"] = r["text"]
        out["sum_usage"] = r["usage"]
        out["cost_cny"] = r["cost_cny"]
        out["sum_finish"] = r["finish"]
    except Exception as e:
        out["sum_error"] = str(e)[:200]
    out["t_sum"] = time.time() - t
    out["total_secs"] = time.time() - t0
    return out


if __name__ == "__main__":
    import sys
    v = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("TEST_VIDEO") or "test.mp4"
    print("视频:", v)
    print("=" * 64)
    r = watch(v)
    print("时长      %.1f 秒" % r.get("duration", 0))
    print("语音      %d 字  (%.2fs)   %s"
          % (len(r["transcript"]), r["t_asr"],
             "emotion=" + r.get("asr_emotion", "") if r.get("asr_emotion") else ""))
    print("九宫格    %d 张  (%.2fs)" % (len(r["grids"]), r["t_frames"]))
    print("总结      %d 字  (%.2fs)" % (len(r["summary"]), r["t_sum"]))
    if r.get("sum_usage"):
        u = r["sum_usage"]
        print("token     输入 %d / 输出 %d" % (u.get("prompt_tokens", 0), u.get("completion_tokens", 0)))
    print("成本      ¥%.4f" % r["cost_cny"])
    print("总耗时    %.1fs" % r["total_secs"])
    print("=" * 64)
    print(r["summary"])
