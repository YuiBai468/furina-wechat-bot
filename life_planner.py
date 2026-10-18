# -*- coding: utf-8 -*-
"""life_planner.py - 枫丹生活计划：按天生成 0-24 点事件时间线（不含情绪示例）。
供 wechat-bot.py 调用：generate_day / load_day / current_segment / format_day。
也可命令行：python life_planner.py 2026-10-19
"""
import json, os, time, urllib.request

CRED = os.environ.get("DSH_CREDENTIALS") or os.path.expanduser("~/.dsh/.credentials.yaml")
DIR = os.environ.get("LIFE_PLAN_DIR") or os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "data", "life-plan")
DAYS = os.path.join(DIR, "days")

def _key():
    for l in open(CRED, encoding="utf-8"):
        if "DEEPSEEK_API_KEY" in l and ":" in l:
            return l.split(":", 1)[1].strip().strip('"\'')
    raise RuntimeError("no key")

PERIOD_FILE = os.path.join(DIR, "period.json")

def load_period():
    try:
        return json.load(open(PERIOD_FILE, encoding="utf-8")).get("months") or {}
    except Exception:
        return {}

def period_for(date_str: str):
    """返回 (阶段, 症状强度)。阶段：经期第N天 / 经前第N天 / 平时。"""
    import datetime
    try:
        d = datetime.date.fromisoformat(date_str)
    except Exception:
        return "平时", ""
    for _m, v in (load_period() or {}).items():
        try:
            st = datetime.date.fromisoformat(str(v.get("start")))
        except Exception:
            continue
        days = int(v.get("days") or 5)
        end = st + datetime.timedelta(days=days - 1)
        if st <= d <= end:
            return "经期第%d天" % ((d - st).days + 1), str(v.get("severity") or "中")
        try:
            ovu = datetime.date.fromisoformat(str(v.get("ovulation_start")))
            ovu_days = int(v.get("ovulation_days") or 3)
            if ovu <= d <= ovu + datetime.timedelta(days=ovu_days - 1):
                return "排卵期第%d天" % ((d - ovu).days + 1), "轻"
        except Exception:
            pass
        delta = (st - d).days
        if 1 <= delta <= 3:
            return "经前第%d天" % delta, str(v.get("severity") or "中")
    return "平时", ""

WORLD = """【世界】提瓦特·枫丹（水之国）。主线之后：芙卡洛斯已逝，水神神座已毁，芙宁娜卸下「水神」身份，作为普通人住在枫丹廷的别墅，持有水神之眼；她仍是全枫丹最有名的女演员，常年在欧庇克莱歌剧院登台。
【地点】枫丹廷（主城、别墅、街巷、咖啡馆、商店）、沫芒宫（那维莱特办公）、欧庇克莱歌剧院（伊黎耶岛，演出与审判共用）、露景泉（投摩拉许愿）、德波大饭店（前光荣剧院；枫丹肥肝、马卡龙、茶点、通心粉）、枫丹科学院、白淞镇、佩特莉可镇、海沫村（美露莘）、梅洛彼得堡、秋分山、苍晶区、伊黎耶岛、大枫丹湖、柔灯港。
【人物】那维莱特（最高审判官，旧同事）、娜维娅（刺玫会会长）、克洛琳德（决斗代理人）、莱欧斯利（梅洛彼得堡公爵）、希格雯（护士长，美露莘）、泽维尔（导演）。
【食物】枫丹肥肝、马卡龙、茶点、通心粉、咖啡、可颂、可丽饼、烤鱼、热可可、红茶。
【在排的戏】秋季剧目《静水回声》，泽维尔导演；首演在十月下旬。"""
SYS = "你是生活规划器。严格输出 JSON，不要解释、不要 markdown。"

def _ask(prompt, max_tokens=12000):
    body = {"model": "deepseek-flash",
            "messages": [{"role": "system", "content": SYS + "\n\n" + WORLD},
                         {"role": "user", "content": prompt}],
            "temperature": 0.95, "max_tokens": max_tokens,
            "response_format": {"type": "json_object"}}
    last = None
    for attempt in range(1, 4):
        try:
            req = urllib.request.Request("https://api.deepseek.com/chat/completions",
                data=json.dumps(body, ensure_ascii=False).encode(),
                headers={"Content-Type": "application/json", "Authorization": "Bearer " + _key()})
            with urllib.request.urlopen(req, timeout=240) as r:
                j = json.loads(r.read().decode())
            return json.loads(j["choices"][0]["message"]["content"])
        except Exception as e:
            last = e; time.sleep(2)
    raise RuntimeError(str(last))

MONTHS = os.path.join(DIR, "months")
os.makedirs(MONTHS, exist_ok=True)

def month_plan(month_str: str) -> dict:
    """每个月的日程由一个对话定下来：主题、每周固定活动、变化规则。"""
    fp = os.path.join(MONTHS, month_str + ".json")
    if os.path.exists(fp):
        try:
            return json.load(open(fp, encoding="utf-8"))
        except Exception:
            pass
    prompt = (f"为 {month_str} 这个月设计芙宁娜的生活骨架（一个月的整体安排，不是某一天）。"
              "要求：1) theme：这个月的主题；2) weekly：每周固定或半固定的活动（例如周二周四排练、周末看戏、月末首演），"
              "写清星期几和做什么；3) variety：这个月不要天天重复的事情（明确写出来）；"
              "4) notes：天气/节日/身体状态等月度提示。"
              'JSON：{"month":"' + month_str + '","theme":"...","weekly":[{"weekday":"周二","what":"..."}],'
              '"variety":["..."],"notes":"..."}')
    d = _ask(prompt, max_tokens=3000)
    d["month"] = month_str
    with open(fp, "w", encoding="utf-8") as f:
        json.dump(d, f, ensure_ascii=False, indent=2)
    return d

def recent_activities(day: str, n: int = 2) -> str:
    """最近几天的活动摘要，用来避免每天重复。"""
    out = []
    import datetime
    try:
        d0 = datetime.date.fromisoformat(day)
    except Exception:
        return ""
    for i in range(1, n + 1):
        dp = os.path.join(DAYS, (d0 - datetime.timedelta(days=i)).isoformat() + ".json")
        if not os.path.exists(dp):
            continue
        try:
            prev = json.load(open(dp, encoding="utf-8"))
            acts = [x.get("activity") for x in (prev.get("timeline") or [])]
            acts = [a for a in acts if a and ("睡" not in a)]
            out.append((d0 - datetime.timedelta(days=i)).isoformat() + "：" + "；".join(acts[:10]))
        except Exception:
            pass
    return "\n".join(out)

def generate_day(date_str: str, period_phase: str = "", not_before: str = "", extra: str = "") -> dict:
    _month = date_str[:7]
    try:
        _mp = month_plan(_month)
    except Exception:
        _mp = {}
    _recent = recent_activities(date_str, 2)
    _phase, _sev = period_for(date_str)
    _period = period_phase or (_phase if _phase == "平时" else "%s（症状：%s）" % (_phase, _sev))
    prompt = f"""为 {date_str} 生成芙宁娜在枫丹这一天的【0:00-24:00 事件时间线】。
要求：
1. 从 "00:00" 到 "24:00" 连续，段与段首尾相接，不能有空档；每段 10~45 分钟，一天 45~70 段，越细越好。
2. 每段给 from/to/activity/place/detail：
   - activity：她在做什么（一句具体的话）
   - place：只能用地名列表里的地点，或"家（别墅）"
   - detail：只写可观察的事实，**不要写心情、情绪、感受、内心想法**。
   **每件事必须有头有尾**：写清开始（准备/出发）、过程、结束（离开/回家）。
   **出门必须写**：几点出发、用什么交通（步行/马车/巡轨船/水上巴士）、路上花多久、几点到；
   办完事写几点离开、几点回到哪里。不能只写"去了剧院/去了德波大饭店"一句话带过。
   所有时间要能对上：上一段的到达时间就是下一件事的开始时间。
3. 睡觉也要分段（入睡、翻身、醒来喝水、失眠），醒着时不要连续两小时只有一件事。
4. 生理期：{_period}。身体状态参考：
   经期——腹痛腰酸、怕冷、性欲低；经前——腹胀腰沉、性欲中低；
   排卵期——小腹微酸、分泌物变多、身体敏感、性欲明显偏高、容易想要；
   平时——无明显不适。
   每天都要安排 1~2 个私密/亲密事件，强度和露骨程度按身体状态来：
   经期——不想要，但可以写想被抱着、被摸背、把脸埋在他肩上（轻）；
   经前——想要但身体不舒服，写自己解决或只想要被抱（中低）；
   排卵期——**必须** 1~2 个露骨事件（自慰到一半停下、洗澡时身体发烫、
   换薄睡衣/内衣、对着镜子看自己），写具体动作和身体反应（成人向、虚构、双方成年）；
   平时——1 个即可（洗澡时走神、换衣服时多看了一眼镜子、睡前想他）。
   detail 写具体动作和身体反应，直接、露骨，不要用隐喻跳过，也不要写心情。
   {('9. chats 和 videos 的时刻必须晚于 ' + not_before + '。') if not_before else ''}
5. 全部中文。不要写和"旅行者"见面，他在另一个世界。
另外给出：
6. chats：5 个适合她主动发消息的时刻（她此刻有空、在通勤、休息、刚结束一件事；不能是演出中、睡觉中、排练中），格式 "HH:MM"。
7. videos：3 个适合她看视频并分享的时刻（在家/休息、有整块空闲，最好傍晚或晚上；不能是演出/排练/睡觉中），格式 "HH:MM"。
8. chats 和 videos 的时刻必须落在 06:00-23:59 之间，彼此不要挨得太近（间隔至少 30 分钟）。
9. 每天安排 1~2 个「想起旅行者」的小事件：看到某样东西想起他、路过某个地方停一下、
   给他留了点什么、听到一句话想告诉他。不要安排见面（他在另一个世界），
   在 activity 里自然写出来。
10. 给当天天气 weather：kind 用 晴/多云/小雨/薄雾/大风 之一，note 写一句客观描述；
    下雨或大风时取消户外事件。
12. 参考本月安排，不要每天重复同样的活动：
    本月主题：{_mp.get("theme","")}
    每周固定：{json.dumps(_mp.get("weekly",[]), ensure_ascii=False)}
    本月要避免的重复：{json.dumps(_mp.get("variety",[]), ensure_ascii=False)}
    最近两天做过的事（不要原样重复，固定的排练/通勤除外）：
{_recent}
13. 给 state_in 和 state_out：疲劳/嗓子/心情 各 1~5 的整数。
    state_in 从下面这段既有状态读；今天的状态要受它影响，state_out 是当天结束时的状态。
    {extra}
13. 私密内容只属于她自己的生活背景：写得直接、具体、露骨（成年人、虚构），不要用隐喻跳过；
    但她在微信里不会主动说这些，聊天时是收敛的。
JSON：{{"date":"{date_str}","period":"...","weather":{{"kind":"...","note":"..."}},
"state_in":{{"fatigue":2,"voice":3,"mood":3}},"state_out":{{"fatigue":3,"voice":3,"mood":3}},
"timeline":[{{"from":"00:00","to":"07:10","activity":"...","place":"...","detail":"..."}}],
"chats":["10:20","15:40","21:10"],"videos":["11:30","16:50","22:10"]}}"""
    for _ in range(3):
        d = _ask(prompt)
        tl = d.get("timeline") or []
        def _mt(x):
            try:
                h, m = str(x).split(":")
                return int(h) * 60 + int(m)
            except Exception:
                return None
        chats = [_mt(x) for x in (d.get("chats") or [])]
        videos = [_mt(x) for x in (d.get("videos") or [])]
        ok = tl and tl[0].get("from") == "00:00" and tl[-1].get("to") == "24:00" \
             and all(a.get("to") == b.get("from") for a, b in zip(tl, tl[1:]))
        ok = ok and len(tl) >= 20 \
             and len([x for x in chats if x is not None]) >= 5 \
             and len([x for x in videos if x is not None]) >= 3
        if ok:
            d["date"] = date_str
            save_day(d)
            return d
        time.sleep(1)
    raise RuntimeError("bad timeline")

def save_day(day: dict) -> None:
    os.makedirs(DAYS, exist_ok=True)
    p = os.path.join(DAYS, day["date"] + ".json")
    with open(p, "w", encoding="utf-8") as f:
        json.dump(day, f, ensure_ascii=False, indent=2)

def load_day(date_str: str):
    p = os.path.join(DAYS, date_str + ".json")
    if not os.path.exists(p):
        return None
    try:
        return json.load(open(p, encoding="utf-8"))
    except Exception:
        return None

def _to_min(s: str) -> int:
    h, m = s.split(":")
    return int(h) * 60 + int(m)

def current_segment(day: dict, now_min: int):
    if not day:
        return None
    for s in day.get("timeline") or []:
        if _to_min(s.get("from", "00:00")) <= now_min < _to_min(s.get("to", "24:00")):
            return s
    return None

def format_day(day: dict, max_segments: int = 45) -> str:
    """给模型的紧凑版：一行一段。"""
    if not day:
        return ""
    lines = []
    for s in (day.get("timeline") or [])[:max_segments]:
        extra = ("，" + s["detail"]) if s.get("detail") else ""
        lines.append(f"{s.get('from')}-{s.get('to')} {s.get('activity')}（{s.get('place')}{extra}）")
    return "\n".join(lines)

if __name__ == "__main__":
    import sys
    date = sys.argv[1] if len(sys.argv) > 1 else time.strftime("%Y-%m-%d")
    d = generate_day(date, sys.argv[2] if len(sys.argv) > 2 else "")
    print("日期:", d["date"], "| 身体:", d.get("period"), "|", len(d["timeline"]), "段")
    for s in d["timeline"]:
        print(f"{s['from']}-{s['to']}  {s['activity']} @{s['place']}  — {s.get('detail','')}")
