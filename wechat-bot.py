#!/usr/bin/env python3
"""大肥鱼 · 微信群友（独立版）

不经过 DSH，不经过 OneBot —— 直接读微信、直接调 DeepSeek。
这样每轮只发「人设 + 这个会话最近几条」约 2k token，而不是 DSH 那份两万 token 的
agent 脚手架，成本差一个数量级。

    wechatauto（读/发微信） ──> 本文件 ──> DeepSeek API

要点：
- **每个会话一套独立上下文**，互不串味。群聊和私聊各自累计，单独滚动。
- 每个会话有独立的回复策略：主人私聊「说话就回」，群聊「被叫到 / 抽中概率」。
- 上下文落盘（contexts.json），重启不丢。

用法：
    python wechat-bot.py [--config config.json]
"""
import argparse
import base64
import glob
import json
import os
import queue
import random
import re
import struct
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional

from wechatauto import WeChatDB
from wechatauto.db import Listener
from wechatauto.guia import quick_send

HERE = os.path.dirname(os.path.abspath(__file__))

# 视频管线（bili / asr / frames / summarize / pipeline / task）放在这里
_VIDEO_DIR = r"D:\work"
if os.path.isdir(_VIDEO_DIR) and _VIDEO_DIR not in sys.path:
    sys.path.insert(0, _VIDEO_DIR)

DEFAULT_CONFIG: Dict[str, Any] = {
    # ── DeepSeek ──────────────────────────────────────────────────────────
    "baseUrl": "https://api.deepseek.com",
    "model": "deepseek-flash",
    # 思考强度：off | low | high | max
    # 实测（deepseek-flash，同一句话）：
    #   off   0.8s  输出 8 token    "当然是你，毕竟我眼神不太好。"
    #   low   1.7s  输出 269 token  "当然是我，你只配当鱼饲料。"
    #   high  1.8s  输出 288 token  "照镜子去，除了我还能有谁？"
    # low 比 high 几乎不省，但质量明显强过 off。按 100 次/天算也就 ¥3/月，
    # 相比 DSH 那条路（¥60~300/月）可以忽略，所以默认开 low。
    # ⚠️ 开启思考后 temperature 不生效（官方明确说明），回复的随机性会略降。
    "thinking": "low",
    "temperature": 1.15,
    # ⚠️ 别调到 400 —— 开思考后 **思考 token 也算进 max_tokens**。
    #    实测 400 时思考吃满、content 吐空，一晚上静默漏掉 11 条回复。
    #    900 是实测稳的值。
    "maxTokens": 900,
    # 从 DSH 的凭据库里取 key，避免明文复制到本文件
    "credentialsFile": os.path.expanduser("~/.dsh/.credentials.yaml"),
    "credentialsRef": "DEEPSEEK_API_KEY",
    # 也可以直接写死（留空表示不用）
    "apiKey": "",

    # ── 人设 ──────────────────────────────────────────────────────────────
    "personaFile": "persona/furina.md",
    # 人设文件是给"会工具调用的 agent"写的，这里补一句把它拉回纯聊天
    "runtimeNote": (
        "你现在在一个微信聊天窗口里，对面是人。\n"
        "你没有工具、没有文件、没有任何系统权限，也看不见别的地方在聊什么。\n"
        "**直接说话就行**：不要写 [QQ] 之类的标记，不要输出任何格式符号。\n"
        "**不要写括号里的动作或旁白**（比如「（叹气）」），你只有台词。"
    ),

    # ── 每个会话的策略 ────────────────────────────────────────────────────
    # mode: "always" 说话就回 ｜ "wake" 被叫到或抽中概率才回 ｜ "off" 完全不理
    "chats": [
        {"name": "主人", "mode": "always", "kind": "private"},
        {"name": "米奇妙妙屋", "mode": "wake", "kind": "group"},
        {"name": "相侵相碍六家人", "mode": "wake", "kind": "group"},
        {"name": "麻豆传媒", "mode": "wake", "kind": "group"},
    ],
    # 命中即唤醒（正文包含任一即算被叫）
    "nicknames": ["大肥鱼", "肥鱼", "谁最帅", "谁帅", "最帅的人"],
    # 没被叫到时，按这个概率随机搭话
    "wakeProbability": 0.05,
    # 唤醒后开的"会话跟随"窗口（秒）。**默认关**。
    # 关着的时候，群里只有「被叫到」和「抽中概率」才会回，其余一律只记录。
    # ⚠️ 开这个要小心：窗口必须只在**真正被叫到/抽中**时续期，绝不能因为
    #    "跟随而回复"也续期 —— 那样窗口永远关不掉，一被叫就变成每句都回。
    "followupSeconds": 0,

    # ── 上下文 ────────────────────────────────────────────────────────────
    # 每个会话带最近多少条历史（一问一答算两条）
    "historyLimit": 12,
    "contextFile": os.path.join(HERE, "contexts.json"),

    # ── 语音转文字（她"听懂"你发的语音）──────────────────────────────────
    # SILK 解码器必须存在（见文件开头的说明）；ASR 走千问的原生 multimodal 接口。
    "silkDecoder": "bin/silk_v3_decoder.exe",
    "asr": {
        "enabled": True,
        "ref": "QWEN_API_KEY",          # 从 credentialsFile 的 refs: 里取
        "model": "qwen3-asr-flash",
        "language": "zh",
        "maxSeconds": 180,              # 超过这个长度的语音不转（省时间）
    },

    # ── 输出形状 ──────────────────────────────────────────────────────────
    # 私聊里剥掉 emoji（芙宁娜用会出戏）；群聊不剥 —— 那份人设是允许的。
    "stripEmojiInPrivate": True,

    # ── 发照片 ────────────────────────────────────────────────────────────
    # 链路见文件里「发照片」那一段的说明。她只写情境，写手扩写，官方立绘当参考。
    "image": {
        "enabled": True,
        "model": "qwen-image-3.0-pro",
        "size": "1024*1024",              # 1K。2K 是双倍价，别改
        "promptExtend": False,            # ⚠️ 必须 False，见那段实测说明
        "ref": "QWEN_API_KEY",
        "refs": ["refs/furina_face.jpg", "refs/furina_card.jpg"],
        "saveDir": "images_out",
        "maxPerDay": 4,                   # 每个会话每天最多发几张
        # ⚠️ 只在他**明确说想看**时才出图。出图是真花钱的（0.25 元/张），
        #    光靠人设她会忘，所以这里再拦一道硬闸。改 false 可放开。
        "requireAsk": True,
        "writerModel": "deepseek-flash",
        "character": ("芙宁娜（原神·枫丹）。银白色短发、发尾蓝色渐变，蓝色大眼睛，"
                      "蓝色高礼帽配金色冠饰，白蓝金三色华丽礼服外套，"
                      "蓝色缎带与蓝宝石胸针，白手套，短裤配黑色长筒靴。"),
    },

    # ── 行为保护（防封号 / 防刷屏）────────────────────────────────────────
    "sendDelayMin": 1.5,
    "sendDelayMax": 8.0,
    "dailySendLimit": 600,
    "quietHours": [2, 7],
    # 收到消息后先"看一会儿"再决定，避免秒回（像机器人）也顺便攒上下文
    "readDelay": 0.6,
    # 自己的 sender_id（群里自己的消息是它）。认错了会自问自答。
    "selfSenderId": 2,

    # ── 等他说完（去抖）──────────────────────────────────────────────────
    # 真人看到"在吗 / 我想问个事 / 算了"不会连回三次 —— 会等对方停下来再一起回。
    # 收到消息后等 debounceSeconds 秒，期间又来消息就把计时重置；
    # 但最多只等 debounceMaxSeconds 秒（免得对方滔滔不绝时一直不吭声）。
    "debounceSeconds": 3.0,
    "debounceMaxSeconds": 9.0,

    # ── 发送节奏（wechatauto 的防封号限速）────────────────────────────────
    # wechatauto 内置「120 秒内最多写 N 次」的突发限制，撞上就强制等约 49 秒。
    # 那是作者故意做的防封号措施，**不是卡死**。
    #   natural（默认）  N=6   打字间隔 2.5~6.0s   最保守
    #   fast             N=20  打字间隔 0.6~1.4s   快 4 倍，但更像机器人
    #   custom           用下面的 rhythmBurst 自己定
    "rhythmProfile": "natural",
    "rhythmBurst": 0,
    "rhythmWindow": 0,

    # ── 主动找人（真人不会永远只回不主动）──────────────────────────────────
    # 有 proactive: true 的会话才会主动开口。群聊不主动。
    "proactive": {
        "enabled": True,
        "checkSeconds": 30,
        # 他好久没说话 → 她找个烂借口开口（分钟区间，随机取）
        "silenceMin": [50, 160],
        # 她发了消息他多久没回 → 追一句
        "nudgeMin": [6, 20],
        # 追了还不回 → 开始生气
        "angryMin": [45, 110],
        # 生气了还不回 → 最后轻轻发一条，然后彻底沉默
        "lastMin": [200, 400],
        # 每天最多主动几次
        "dailyLimit": 5,
        # 两次主动之间至少隔多久（分钟）
        "minGapMin": 40,
        # 到点了也不是必发 —— 真人有"想发又没发"的时候
        "chance": 0.65,
        # 深夜不主动（本地时间区间，空数组 = 不限）
        "quietHours": [1, 8],
    },

    # ── 拟人化保护（比打字速度重要得多）──────────────────────────────────
    # wechatauto 的 rhythm 只管"打字多快、连发多密"。真正让风控起疑的是**行为模式**：
    # 24 小时在线、秒回、每次都回、延迟永远是 1~5 秒固定区间 —— 这几条比打字速度显眼得多。
    "humanize": {
        "enabled": True,
        # 睡觉时间（本地小时，跨零点写 [22, 7]）。真人这时候基本不活跃。
        "sleepHours": [2, 8],
        # 睡觉时收到消息：大部分拖一会儿才回（半梦半醒），偶尔这条真没看见。
        "sleepDelayMin": 40,
        "sleepDelayMax": 150,
        # 睡觉时"这条没看见"的概率。
        # ⚠️ 别调高 —— 调高了就变成**她刚说完「我睡不着，想你了」，他回一句，人没了**。
        # 踩过：0.25 会让人直接问「怎么不理我啥意思」。
        # 现在靠三样东西兜底：① 下面那位 awakeIfSpokeWithinMin（她刚说过话 = 她醒着）
        # ② sleepEscalateFactor（他连着发会把她吵醒）③ 拖回复有硬上限 30 秒。
        "sleepSilentChance": 0.25,
        # 每多收一条，没看见的概率乘这个系数 → 25% / 14% / 8% / 4% / 2%
        "sleepEscalateFactor": 0.55,
        # 多久以内的消息算"连着发"（分钟）
        "sleepEscalateWindowMin": 12,
        # 但作息低谷不是"装死"：他说了这些词就会把她弄醒。
        # （她自己就失眠，听到"睡不着"不可能继续睡。）
        "sleepWakeWords": ["睡不着", "失眠", "睡不着觉", "难受", "好难受", "想你了",
                           "好想你", "出事了", "怎么办", "哭了", "救", "急", "怕"],
        # 被叫醒之后也要迷糊一会儿（秒）
        "sleepWakeDelayMin": 6,
        "sleepWakeDelayMax": 25,
        # ⚠️ 这里原来有个 ignoreChance（平时偶尔"已读不回"），**已经删掉了**。
        # 理由：他说话就是说话，不存在"她正好没看见"这回事 ——
        # 一晚上被晾两次他就来问「怎么不理我啥意思」了。
        # 想让她冷，该调的是人设和冷却，不是随机丢消息。
        # 偶尔"想了半天才回"，而不是永远落在 1~5 秒那个固定区间
        "slowReplyChance": 0.10,
        "slowReplyMin": 3,
        "slowReplyMax": 10,
    },

    # ── 作息表（到点主动说话）──────────────────────────────────────────────
    # 每一项都有随机抖动，而且**每天的抖动是固定的**（存进状态文件），
    # 不然每次检查都掷骰子，同一件事会在几分钟里反复"到点"。
    "schedule": {
        "enabled": True,
        # 起床（她凌晨一点睡，早上七点起）
        "wake": {"enabled": True, "at": "07:00", "jitterMin": 40},
        # 饭点 —— 到点主动问他吃了没
        "meals": [
            {"enabled": True, "name": "早饭", "at": "09:00", "jitterMin": 50},
            {"enabled": True, "name": "午饭", "at": "12:40", "jitterMin": 60},
            {"enabled": True, "name": "晚饭", "at": "18:40", "jitterMin": 60},
        ],
        # 睡觉（凌晨一点）
        "sleep": {"enabled": True, "at": "01:00", "jitterMin": 45},
        # 事件过期多久就不补发了（分钟）。
        # 不加这个的话，重启一次她就会把今天已经过掉的作息**全部补发一遍**
        # —— 早上五点给你发「晚安」。超过这个窗口就只标记已处理，不发。
        "catchUpMinutes": 60,
        # 一周里挑 1~2 天失眠：睡不着，想被哄
        "insomnia": {"enabled": True, "from": "23:30", "to": "00:45",
                     "minDays": 1, "maxDays": 2, "aheadMin": 2, "aheadMax": 6},
        # 空闲时段随机搭话：跟他沉默多久无关，就是她自己想说。
        # 每天挑 1~3 个随机时刻，避开饭点前后 avoidMinutes 分钟（免得刚问完吃了没又来一句）。
        "randomChats": {
            "enabled": True,
            "from": "09:30", "to": "23:30",
            "minPerDay": 1, "maxPerDay": 3,
            "avoidMinutes": 45,
        },
    },

    # ── 健康数据（手环 → Gadgetbridge → HTTP POST 到这里）─────────────────
    # 手机 POST 到 http://<电脑>:8765/hr
    # body: {"metric":"hr","value":78,"text":"RUNNING","ts":...,"source":"gadgetbridge"}
    # metric: hr / stress / spo2 / steps / sleep / awake / workout
    #         calories / distance / workout_steps
    #
    # ⚠️ 阈值按"最大心率 200"定（220 - 年龄 ≈ 200）
    #    110 那种写法站起来走两步就触发，会变成骚扰。
    "health": {
        "enabled": True,
        "port": 8765,
        "token": "",              # 非空时手机要带 X-Token

        # ── 心率三级 ──
        "hrHigh": 150,            # 150-170  好奇搭话
        "hrHard": 170,            # 170-190  慌乱 + 嘴硬
        "hrExtreme": 190,         # 190+     真急了，不嘴硬
        "hrSustainSeconds": 60,   # 要持续这么久才算数（爬楼梯不算）。
                                  # ⚠️ 设成 0 = **到了就触发**，不等 ——
                                  #    跑步时心率冲上去又掉，60 秒根本抓不到。

        # ── 其他 ──
        "spo2Low": 90,            # 血氧低于这个数 → 担心
        "stressLow": 20,          # 压力低于这个数 → 他心情好，是聊天的好时机
        "stressHigh": 40,         # 压力高于这个数 → 陪伴

        # ── 睡眠（小时）──
        "sleepShortH": 5,         # 睡不到这么久 → 催补觉
        "sleepLongH": 10,         # 睡超过这么久 → 吐槽 + 生气（没陪我）
        "wakeLateHour": 12,       # 中午之后才醒 → 睡过头，生气
        "nightStartH": 1,         # "凌晨还没睡"的判定起点
        "nightEndH": 5,           # 终点
        # 睡眠摘要到达时，如果距离"他真正醒来"已经超过这么久，就别说了 ——
        # 下午三点突然来一句"你昨晚睡得不好"很怪。
        "sleepFreshMin": 120,

        # ── 互斥（运动/睡眠会改变其他指标的正常范围）──
        # 没有这个，他跑个步她能连着报三次警。
        "workoutGraceMin": 10,    # 运动结束后这么久内，心率高仍算正常
        "workoutStaleHours": 3,   # 超过这么久没收到结束事件，就当运动已结束
        # 运动中心率要超过这个数才报警（真的是极限了才提）
        "hrExtremeInWorkout": 190,
        # 运动中血氧低依然要报 —— 这是唯一不被运动屏蔽的指标

        # ── 统一防打扰 ──
        "cooldownMin": 25,
        "dailyLimit": 10,
        "quietHours": [],         # 空 = 不静默（睡眠相关本来就要夜里说）
    },

    "logFile": r"D:\llm\logs\wechat-bot.log",
}


# ───────────────────────────────── 基础 ─────────────────────────────────
CFG: Dict[str, Any] = {}
CFG_PATH = ""
CFG_MTIME = [0.0]
_logLock = threading.Lock()

# emoji 的范围（只覆盖真正的表情符号，**不含** / 等普通符号）。
# 私聊里剥掉 —— 芙宁娜用 emoji 会出戏；群聊那份人设是允许的，所以不剥。
_EMOJI_RE = re.compile(
    "[" 
    "\U0001F300-\U0001FAFF"      # 表情、象形、补充符号
    "\U0001F1E6-\U0001F1FF"      # 国旗（区域指示符）
    "\U00002600-\U000027BF"      # 杂项符号 + 装饰符号（☀ ✅ ✂ ❤ …）
    "\U00002B00-\U00002BFF"      # 杂项符号和箭头（⭐ …）
    "\U0000FE0F"                 # 变体选择符（️）
    "\U000020E3"                 # 组合键帽（⃣）
    "]+")

# ASR 的 emotion → **声学描述**，不是情绪结论。
#
# ⚠️ 2026-10-08 实测翻车：他**宠溺**的语气被 ASR 判成 sad，她于是用安慰的口吻回。
#    原因是"宠溺/温柔"和"低落"在声学上几乎是同一件事：
#        宠溺  = 慢 + 低 + 轻
#        低落  = 慢 + 低 + 轻
#        疲惫 / 刚睡醒 / 认真想事情 也是
#    ASR 只看得到声学特征，看不到意图。所以**绝不能把它的输出当成情绪结论**，
#    只能当"我听到的声音长这样"转述给模型，由它结合上下文去判断。
#
#    换句话说：这三个类目其实是**声学聚类**，不是情绪分类。改个说法就对了。
_VOICE_OBS = {
    "sad": "偏低偏慢",
    "angry": "偏急偏重",
    "happy": "偏亮偏快",
    "fearful": "发紧",
    "surprised": "偏高",
    "disgusted": "偏冷",
    "neutral": "",
}


def reload_config() -> None:
    """改 bot-config.json 后自动生效，不用重启（调试时很省事）。"""
    if not CFG_PATH:
        return
    try:
        mt = os.stat(CFG_PATH).st_mtime
    except Exception:
        return
    if mt == CFG_MTIME[0]:
        return
    CFG_MTIME[0] = mt
    try:
        with open(CFG_PATH, encoding="utf-8") as f:
            new = json.load(f)
        CFG.update(new)
        log("配置已热重载")
    except Exception as e:
        log(f"配置热重载失败（忽略）: {e}")


# ══════════════════════════════════ 控制台遥测 ══════════════════════════════════
# 一块"看板"用的实时数据。设计原则：
#   · 只加不减 —— 任何 emit 失败都不能影响主循环（她该说说该回回）
#   · 队列满就丢最旧的 —— 控制台再卡也不会拖慢她
#   · 内存有上限 —— 环形缓冲，跑一个月也不涨
class Telemetry:
    """环形缓冲 + SSE 订阅。给 /console 看板用。"""

    MAX_EVENTS = 800        # 内存里保留多少条事件
    MAX_SUBS = 8            # 最多几个浏览器在连
    SUB_QUEUE = 512         # 每个订阅者的待发队列
    SERIES = 240            # 健康曲线保留多少个点

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._events: List[Dict[str, Any]] = []
        self._subs: List[Any] = []
        self._seq = 0
        self.started = time.time()
        self.stats: Dict[str, Any] = {
            "llm_calls": 0, "tokens_in": 0, "tokens_out": 0, "cached": 0,
            "health_in": 0, "sent": 0, "send_secs": 0.0, "send_timeout": 0,
            "fired": 0, "blocked": 0, "inbound": 0,
            # 模型写超行数、被程序合并的次数。这个数越低，说明人设里
            # "默认写 3 行"那句约束越有效（模型侧守住了，程序兜底没上场）。
            "bubble_over": 0,
        }
        self.last_health: Dict[str, Any] = {}
        self.last_health_at = 0.0        # 最后一次收到手环数据的时刻
        self.last_llm: Dict[str, Any] = {}
        self.health_series: List[Dict[str, Any]] = []
        self.send_marks: List[float] = []     # 发送时刻，算"120 秒窗口"
        self.rhythm: Dict[str, Any] = {"name": "?", "burst": 0, "window": 120.0}
        self.model_cfg: Dict[str, Any] = {}

    # ── 发事件 ────────────────────────────────────────────────────
    def emit(self, kind: str, **data: Any) -> None:
        try:
            with self._lock:
                self._seq += 1
                ev: Dict[str, Any] = {"seq": self._seq, "t": time.time(), "kind": kind}
                ev.update(data)
                self._events.append(ev)
                if len(self._events) > self.MAX_EVENTS:
                    del self._events[:-self.MAX_EVENTS]
                subs = list(self._subs)
            for q in subs:
                try:
                    q.put_nowait(ev)
                except Exception:
                    pass          # 队列满 → 丢，不拖慢主循环
        except Exception:
            pass

    # ── SSE 订阅 ──────────────────────────────────────────────────
    def subscribe(self):
        q: Any = queue.Queue(maxsize=self.SUB_QUEUE)
        with self._lock:
            if len(self._subs) >= self.MAX_SUBS:
                return None
            self._subs.append(q)
        return q

    def unsubscribe(self, q: Any) -> None:
        try:
            with self._lock:
                if q in self._subs:
                    self._subs.remove(q)
        except Exception:
            pass

    def recent(self, kind: Optional[str] = None, limit: int = 120) -> List[Dict[str, Any]]:
        with self._lock:
            evs = list(self._events)
        if kind:
            evs = [e for e in evs if e.get("kind") == kind]
        return evs[-limit:]

    # ── 健康曲线 ──────────────────────────────────────────────────
    def add_health_point(self, metric: str, value: int, text: str, ts: float) -> None:
        """心率/压力/血氧进曲线；其他只记最后值。"""
        with self._lock:
            self.last_health[metric] = {"value": value, "text": text,
                                        "t": time.time(), "ts": ts}
            if metric in ("hr", "stress", "spo2"):
                self.health_series.append({"t": ts or time.time(),
                                           "m": metric, "v": value})
                if len(self.health_series) > self.SERIES:
                    del self.health_series[:-self.SERIES]
            self.last_health_at = time.time()

    def feed_samples(self, text: str) -> int:
        """把一批活动样本拆成曲线上的点。

        手机端实际上只推 samples 批次（每条样本一个 JSON 对象），
        单条 hr / stress / spo2 反而很少推 —— 不解析这个，
        看板上的曲线永远是空的（踩过：接了半天数据，图上一条线没有）。
        """
        try:
            arr = json.loads(text or "[]")
        except Exception:
            return 0
        if not isinstance(arr, list):
            return 0
        got = 0
        try:
            with self._lock:
                for s in arr[-300:]:
                    if not isinstance(s, dict):
                        continue
                    t = float(s.get("t") or 0) or time.time()
                    for key, name in (("hr", "hr"), ("st", "stress"), ("sp", "spo2")):
                        try:
                            v = int(s.get(key) or 0)
                        except Exception:
                            continue
                        if not v:
                            continue
                        self.health_series.append({"t": t, "m": name, "v": v})
                        got += 1
                        # 最新值也更新，看板首屏才有东西显示
                        self.last_health[name] = {"value": v, "text": "",
                                                  "t": time.time(), "ts": t}
                if len(self.health_series) > self.SERIES:
                    del self.health_series[:-self.SERIES]
                self.last_health_at = time.time()
        except Exception:
            pass
        return got

    # ── 限速窗口 ──────────────────────────────────────────────────
    def mark_send(self, secs: float = 0.0) -> None:
        now = time.time()
        with self._lock:
            self.send_marks.append(now)
            # 只留最近 10 分钟
            self.send_marks = [t for t in self.send_marks if now - t < 600]
            self.stats["sent"] += 1
            self.stats["send_secs"] += float(secs or 0)

    def send_window_usage(self) -> Dict[str, Any]:
        """当前"窗口内已经写了几条"。"""
        win = float(self.rhythm.get("window") or 120.0)
        now = time.time()
        with self._lock:
            marks = [t for t in self.send_marks if now - t < win]
        burst = int(self.rhythm.get("burst") or 0)
        oldest = min(marks) if marks else None
        return {
            "window": win,
            "burst": burst,
            "used": len(marks),
            "remain": max(0, burst - len(marks)) if burst else None,
            "resetIn": (win - (now - oldest)) if oldest else 0.0,
        }

    # ── 首屏快照 ──────────────────────────────────────────────────
    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            stats = dict(self.stats)
            health = dict(self.last_health)
            series = list(self.health_series)
            llm = dict(self.last_llm)
            evs = list(self._events)[-160:]
        return {
            "now": time.time(),
            "started": self.started,
            "uptime": time.time() - self.started,
            "stats": stats,
            "health": health,
            "healthAt": self.last_health_at,       # 看板用它判断"手环数据中断"
            "series": series,
            "llm": llm,
            "model": dict(self.model_cfg),
            "rhythm": self.send_window_usage(),
            "events": evs,
            "seq": self._seq,
        }


TELEM = Telemetry()


def log(*a: Any) -> None:
    line = time.strftime("[%Y-%m-%d %H:%M:%S] ") + " ".join(str(x) for x in a)
    with _logLock:
        print(line, flush=True)
        try:
            with open(CFG["logFile"], "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except Exception:
            pass
    # 顺手喂给控制台（她写的每一行日志，看板上都能实时看到）
    try:
        TELEM.emit("log", line=line.split("] ", 1)[-1])
    except Exception:
        pass


def read_credential(path: str, ref: str) -> str:
    """从 DSH 的 .credentials.yaml 里抠一个 refs 条目（不想为此引入 yaml 依赖）。"""
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                m = re.match(rf"^\s*{re.escape(ref)}\s*:\s*(\S+)\s*$", line)
                if m:
                    return m.group(1)
    except Exception as e:
        log(f"凭据读取失败: {e}")
    return ""


# ───────────────────────────── 语音转文字 ─────────────────────────────
# 让他发的语音能被"听懂"：SILK → WAV → ASR。
#
# ⚠️ 两个实测踩到的坑：
#   ① ffmpeg **不认 SILK**（Invalid data found）。微信语音存的是 SILK V3
#      （文件头 02 23 21 53 49 4c 4b 5f 56 33 = "\x02#!SILK_V3"），
#      必须用专门的解码器。
#      pilk 那个 Python 包要 MSVC 编译（本机没有），所以用 kn007/silk-v3-decoder
#      的预编译 silk_v3_decoder.exe（179KB），放在 bin/ 下。
#   ② 千问的 ASR **不能走 OpenAI 兼容的 /audio/transcriptions** ——
#      dashscope 那个域名上实测 SSL EOF（连都连不上）。
#      要走原生 multimodal 接口 + base64 内联音频。
ASR_URL = ("https://dashscope.aliyuncs.com/api/v1/services/"
           "aigc/multimodal-generation/generation")
SILK_RATE = 24000          # 微信 SILK 解出来固定 24kHz 单声道 16bit


def _wav_header(data_bytes: int, rate: int) -> bytes:
    """给裸 PCM 套一个标准 WAV 头（44 字节）—— 省掉 ffmpeg 依赖。"""
    return (b"RIFF" + struct.pack("<I", 36 + data_bytes) + b"WAVE"
            + b"fmt " + struct.pack("<IHHIIHH", 16, 1, 1, rate, rate * 2, 2, 16)
            + b"data" + struct.pack("<I", data_bytes))


def silk_to_wav(silk_path: str) -> bytes:
    """SILK V3 → WAV 字节。失败抛异常。"""
    exe = os.path.join(HERE, str(CFG.get("silkDecoder") or "bin/silk_v3_decoder.exe"))
    if not os.path.exists(exe):
        raise RuntimeError(f"没有 SILK 解码器：{exe}")
    fd, pcm_path = tempfile.mkstemp(suffix=".pcm")
    os.close(fd)
    try:
        p = subprocess.run([exe, silk_path, pcm_path],
                           capture_output=True, timeout=30)
        pcm = b""
        try:
            with open(pcm_path, "rb") as f:
                pcm = f.read()
        except OSError:
            pass
        if not pcm:
            err = (p.stderr or b"").decode("utf-8", "replace").strip()[:150]
            raise RuntimeError(f"silk 解码没出数据（rc={p.returncode}）{err}")
        return _wav_header(len(pcm), SILK_RATE) + pcm
    finally:
        try:
            os.remove(pcm_path)
        except OSError:
            pass


def asr_transcribe(wav: bytes, a: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """调千问 ASR。返回 {"text","emotion","language"}。失败抛异常，配置缺失返回空。

    ⚠️ 它返回的 `annotations` 里带 **emotion** —— 实测 2026-10-08 用 edge-tts
    做控制变量实验（同一句话只改语速/音高）：

        平静 0%/+0Hz      → neutral
        兴奋 +45%/+60Hz   → neutral     ← 认不出来
        低沉 -35%/-40Hz   → sad         ← 认出来了
        无力 -50%/-20Hz   → sad         ← 认出来了

    文本一字不差，结果却不同 → **它确实在读声音，不是只读文字**。
    但它**偏负向**：只对"低落/生气"敏感，兴奋认不出，大类也就三四个。
    所以"偏急"那种要靠语速自己补（见 _voice_to_text）。
    """
    a = a if a is not None else (CFG.get("asr") or {})
    if not a.get("enabled", True):
        return {}
    key = read_credential(str(CFG.get("credentialsFile") or ""),
                          str(a.get("ref") or "QWEN_API_KEY"))
    if not key:
        log("  语音转文字：拿不到 QWEN_API_KEY，跳过")
        return {}
    body = {
        "model": str(a.get("model") or "qwen3-asr-flash"),
        "input": {"messages": [{"role": "user", "content": [
            {"audio": "data:audio/wav;base64," + base64.b64encode(wav).decode()},
        ]}]},
        "parameters": {"asr_options": {"language": str(a.get("language") or "zh")}},
    }
    req = urllib.request.Request(
        str(a.get("baseUrl") or ASR_URL),
        data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
        headers={"Authorization": "Bearer " + key, "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=120) as r:
        j = json.loads(r.read().decode("utf-8"))
    msg = (((j.get("output") or {}).get("choices") or [{}])[0].get("message") or {})
    text = "".join(str(p.get("text") or "")
                   for p in (msg.get("content") or [])).strip()
    ann = (msg.get("annotations") or [{}])[0] or {}
    return {"text": text,
            "emotion": str(ann.get("emotion") or "").strip().lower(),
            "language": str(ann.get("language") or "").strip()}


def voice_note(text: str, secs: float, emotion: str,
               a: Optional[Dict[str, Any]] = None) -> str:
    """把"听到的声音长什么样"转成一句**客观描述**。没有值得说的就返回空串。

    ⚠️ 返回的是**声学观察**，不是情绪结论 —— 见 _VOICE_OBS 上面那段注释。
    "偏低偏慢"可能是难过，也可能是宠溺、疲惫、刚睡醒。
    判断那一步留给模型（它能看到上下文），它比 ASR 有资格下结论。

    优先级：ASR 的声学聚类（它抓"低/重"准）→ 语速（补它认不出的"快"）

    语速阈值别定太紧：中文正常说话约 **4~5 字/秒**，
    只标明显偏离的（≤2.5 偏慢 / ≥6.5 偏快）—— 宁可漏，也别挂噪音。
    """
    a = a or {}
    obs = _VOICE_OBS.get(str(emotion or "").strip().lower(), "")
    if obs:
        return obs
    n = len(re.sub(r"\s", "", text or ""))
    rate = n / max(float(secs), 0.1)
    if rate >= float(a.get("fastCharsPerSec") or 6.5):
        return "语速偏快"
    if rate <= float(a.get("slowCharsPerSec") or 2.5):
        return "语速偏慢"
    return ""


# ───────────────────────────── 发照片 ─────────────────────────────
# 让"她"能发照片。整条链路分四步，**各司其职**：
#
#   ① 她（人设 + 对话上下文）只写一句**情境**：
#          [配图: 刚收工，累瘫在椅子上，有点想撒娇]
#   ② 写手（中性 system）把情境扩写成合格的图像提示词
#   ③ 图像模型 + **官方立绘当参考图** → 出图
#   ④ 把图给她看，让她配一两句话，然后连图带话发出去
#
# ⚠️ 为什么①②要拆成两次调用：
#   人设里塞"必须写出表情""必须保留帽子礼服""只能是自己拍的"这些机械约束，
#   会让她在演戏和守格式之间分心，遵守率明显下降；而且台词味儿会渗进提示词
#   （「咳，帮本神画一张……」）。写手一个上下文只干这一件事，守得死死的。
#
# ⚠️ 关键实测（2026-10-08）：
#   · **官方立绘当参考图是识别度的唯一保障**。用 AI 生成的图当基准，误差会
#     一代代传下去，最后变成"一个白发蓝眼的路人"，不是芙宁娜。
#   · **参考图会把"没被要求改变的东西"全部继承下来** —— 先是那件米色毛衣，
#     然后是官方图里张嘴大笑的表情。所以想让变的东西必须写进提示词。
#   · `prompt_extend` 必须关。开着它先跑一次大模型扩写，10~16 秒变 111~153 秒。
IMAGE_API = ("https://dashscope.aliyuncs.com/api/v1/services/"
             "aigc/multimodal-generation/generation")

_IMAGE_WRITER_SYSTEM = (
    "你是一个图像提示词写手。根据给定的角色造型和情境，写一段**中文**图像生成提示词，"
    "描述画面：场景、光线、她的姿态神情、镜头感。\n"
    "必须保留角色的标志造型（礼帽、礼服、发色、瞳色、蓝宝石饰品）。\n"
    "⚠️ 必须明确写出嘴巴和表情（例如「抿着嘴微笑」「嘴角微微上扬」「张着嘴大笑」"
    "「面无表情」），否则模型会直接照抄参考图里那张张嘴大笑的脸。表情要和情境吻合。\n"
    "照片是**她自己拍的**：要么手持自拍（一条手臂伸向镜头、脸贴近、轻微广角畸变），"
    "要么手机支在一边定时拍（固定机位、能看出是摆放好的角度）。"
    "必须让人一眼看出是这两种之一，不要写成「别人在拍她」。\n"
    "\n⚠️ 自拍构图有三条反复出错的规矩，**必须严格遵守**：\n"
    "\n【一】这是**第一人称自拍** —— 看照片的人**就是那部手机**。\n"
    "所以：**画面里绝对不要出现手机**。镜头就是它，它拍不到自己。\n"
    "禁止出现这些词：手机、电话、屏幕、举着、拿着手机、看手机。\n"
    "自拍的暗示靠**一条从画面边缘伸进来的手臂**（或者干脆不画手臂），不靠手机。\n"
    "\n【二】她必须**直视镜头** —— 也就是**看着看照片的人**。\n"
    "不要写「低头看屏幕」「看着手机」「视线落在手机上」。\n"
    "要写「直视镜头」「看着镜头这边」「眼睛正对画面」。\n"
    "这是这一整套东西里最重要的一条：她得**看着你**。\n"
    "\n【三】她另一只手必须有事做（拎着外套、握着杯子、拨头发、扶着栏杆）。"
    "没在拿东西就写「另一只手自然垂着」—— 不要让它空着漂在那儿。\n"
    "只输出提示词本身，不要解释，不要分点，90~160 字。"
)

# 出图之后让她看图配文时用的提问
_PHOTO_CAPTION_ASK = (
    "（你刚把上面这张照片拍好、准备发给他。现在写**一两句话**配着这张照片一起发出去。）\n"
    "要求：\n"
    "· 就写你看到这张照片时的反应，别描述画面（他又不是看不见）\n"
    "· 一句话就够，最多两条，每条短\n"
    "· 不要重复你刚才已经说过的话\n"
    "· 别解释、别加引号、别写动作描写"
)


def _datauri(path: str) -> str:
    """本地图片 → data URL。参考图已经预先压过，这里只读不压。"""
    with open(path, "rb") as f:
        raw = f.read()
    ext = os.path.splitext(path)[1].lower().lstrip(".")
    mime = {"png": "image/png", "webp": "image/webp"}.get(ext, "image/jpeg")
    return f"data:{mime};base64," + base64.b64encode(raw).decode()


def write_image_prompt(situation: str, key: str) -> str:
    """情境 → 图像提示词。中性 system，只干这一件事。"""
    cfg = CFG.get("image") or {}
    body = {
        "model": str(cfg.get("writerModel") or CFG.get("model") or "deepseek-flash"),
        "messages": [
            {"role": "system",
             "content": _IMAGE_WRITER_SYSTEM
                        + "\n\n角色造型：" + str(cfg.get("character") or "")},
            {"role": "user", "content": "情境：" + situation},
        ],
        "temperature": 1.0,
    }
    req = urllib.request.Request(
        str(CFG["baseUrl"]).rstrip("/") + "/chat/completions",
        data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
        headers={"Authorization": "Bearer " + key, "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=120) as r:
        j = json.loads(r.read().decode("utf-8"))
    return str(j["choices"][0]["message"]["content"] or "").strip()


def _append_manifest(entry: Dict[str, Any]) -> None:
    """往图库的 manifest.jsonl 追加一行（一个 JSON）。

    为什么用 jsonl 而不是一个 JSON 数组：出图线程是异步的，
    追加一行是原子的，不会互相覆盖；而且崩了也只丢最后一行。

    同名记录可以重复出现 —— 后面那条是「结果」（发没发出去、她配了什么话），
    看板按 name 合并时以最后一条为准。
    """
    cfg = CFG.get("image") or {}
    d = os.path.join(HERE, str(cfg.get("saveDir") or "images_out"))
    try:
        os.makedirs(d, exist_ok=True)
        with open(os.path.join(d, "manifest.jsonl"), "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception as e:
        log(f"  [配图] manifest 写入失败：{e}")


def _img_ext(data: bytes) -> str:
    """按**文件头**判断图片格式。

    ⚠️ 千问返回的链接没有扩展名，而且实测同一模型有时给 PNG 有时给 WebP。
    直接把 PNG 存成 .jpg 会让下游（微信发送）拿到名实不符的文件。
    """
    if data[:8] == b"\x89PNG\r\n\x1a\n":
        return ".png"
    if data[:2] == b"\xff\xd8":
        return ".jpg"
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return ".webp"
    if data[:6] in (b"GIF87a", b"GIF89a"):
        return ".gif"
    return ".png"


def gen_image(prompt: str, key: str) -> str:
    """出图，返回阿里 OSS 的临时链接（会过期，调用方要立刻下）。"""
    cfg = CFG.get("image") or {}
    content = []
    for p in (cfg.get("refs") or []):
        full = p if os.path.isabs(str(p)) else os.path.join(HERE, str(p))
        if os.path.exists(full):
            content.append({"image": _datauri(full)})
    content.append({"text": prompt})
    body = {
        "model": str(cfg.get("model") or "qwen-image-3.0-pro"),
        "input": {"messages": [{"role": "user", "content": content}]},
        "parameters": {
            "size": str(cfg.get("size") or "1024*1024"),
            "n": 1,
            # ⚠️ 必须 False —— 见文件里那段实测说明
            "prompt_extend": bool(cfg.get("promptExtend", False)),
        },
    }
    req = urllib.request.Request(
        IMAGE_API, data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
        headers={"Authorization": "Bearer " + key, "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=600) as r:
        j = json.loads(r.read().decode("utf-8"))
    return str(j["output"]["choices"][0]["message"]["content"][0]["image"])


# 他"明确说想看照片"的信号。只在命中时才允许出图 —— **出图是花钱的**。
# 宁可漏（他就再要一次），也不要让她自己想起来就发一张。
_ASK_PHOTO_RE = re.compile(
    r"照片|相片|自拍|拍一?[张个]|看图|图片|长什么样|什么样子|"
    r"发一?[张个]图|看看你|让我看|想看"
)


# ── B站链接识别 ─────────────────────────────────────────────────────────
_BV_RE = re.compile(r"bilibili\.com/video/(BV[0-9A-Za-z]{10})")
_B23_RE = re.compile(r"b23\.tv/([0-9A-Za-z]+)")


def extract_bv(text: str) -> Optional[str]:
    """从一段话里抠出 B站 BV 号。b23.tv 短链会跟随重定向。"""
    if not text:
        return None
    m = _BV_RE.search(text)
    if m:
        return m.group(1)
    m = _B23_RE.search(text)
    if m:
        try:
            req = urllib.request.Request(
                "https://b23.tv/" + m.group(1),
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
            with urllib.request.urlopen(req, timeout=15) as r:
                final = r.geturl()
            m2 = re.search(r"(BV[0-9A-Za-z]{10})", final)
            if m2:
                return m2.group(1)
        except Exception as e:
            log(f"  b23 短链解析失败: {e}")
    return None


def _extract_photo_markers(text: str):
    """从她的回复里抠出 [配图: xxx]。

    返回 (清理后的文本, [情境, ...])。
    只认**单独一行**的标记 —— 免得正文里偶然出现的方括号被误伤。
    """
    sits: List[str] = []

    def _grab(m):
        s = m.group(1).strip()
        if s:
            sits.append(s)
        return ""

    out = re.sub(r"(?m)^[ \t]*[\[【]配图\s*[：:]\s*([^\]】\n]{1,120})[\]】][ \t]*$",
                 _grab, str(text or ""))
    return out.strip(), sits


def load_persona(path: Optional[str] = None) -> str:
    p = path or CFG.get("personaFile") or ""
    try:
        with open(p, encoding="utf-8") as f:
            return f.read().strip()
    except Exception as e:
        log(f"人设文件读不到（{p}）：{e}")
        return ""


# ─────────────────────── 发送：恢复窗口 + 超时 + 串行 ───────────────────────
# 发消息是 UI 自动化（切会话→写输入框→点发送→回读数据库）。它有两个坑：
#   1) 微信窗口最小化 / 被挪到屏幕外时，控件树找不到元素，quick_send 会卡在那里
#      不回 —— 整个 worker 线程就此僵住，后面所有消息全部堵死。
#   2) 两个发送同时跑会互相打架，所以必须串行。
_send_lock = threading.Lock()
_send_stats = {"ok": 0, "timeout": 0, "error": 0, "stuck": 0}
# 真卡住之后先别急着继续发 —— 晾一会儿，让微信那边的 UI 状态自己恢复
_send_paused_until = [0.0]
# 复用一个 WeChatGUI 实例。quick_send 每次都 new 一个，虽然构造是惰性的，
# 但重复初始化没有意义；这里自己持有，顺便把"发送前拉到前台"也做掉。
_wx = [None]
# 最近几次发送的耗时，用来自适应降低气泡数（发送慢的时候别一次发五条）
_recent_send_secs: List[float] = []


def _restore_wechat_window() -> bool:
    """把微信主窗口从最小化/屏幕外拉回来，并尽量拉到前台。

    实测日志里 rect 经常是 (-31991, -32000, ...) —— 那是 Windows 给最小化窗口的
    坐标。UI 自动化在这种状态下会找不到发送按钮，然后**重跑一次布局校准**，
    那一轮要 50~70 秒。所以发送前先把它摆正，能省掉大部分慢路径。
    """
    try:
        import win32con
        import win32gui
    except Exception:
        return False          # 没装 pywin32 就算了，不影响主流程
    found = []

    def cb(h, _):
        try:
            if win32gui.IsWindowVisible(h) and win32gui.GetWindowText(h) in ("微信", "WeChat"):
                found.append(h)
        except Exception:
            pass
        return True

    try:
        win32gui.EnumWindows(cb, None)
    except Exception:
        return False
    if not found:
        return False
    h = found[0]
    changed = False
    try:
        r = win32gui.GetWindowRect(h)
        offscreen = r[0] < -10000 or r[1] < -10000
        if win32gui.IsIconic(h) or offscreen:
            if win32gui.IsIconic(h):
                win32gui.ShowWindow(h, win32con.SW_RESTORE)
            # 最小化过的窗口恢复后仍可能是离屏坐标，挪回主屏
            if offscreen or win32gui.IsIconic(h):
                win32gui.SetWindowPos(h, 0, 80, 60, 1100, 800,
                                      win32con.SWP_NOZORDER | win32con.SWP_SHOWWINDOW)
            log("  微信窗口是最小化/离屏，已拉回来")
            changed = True
        # 有别的窗口挡着时，wechatauto 会自己去"最小化遮挡窗口"，过程很慢。
        # 这里先试着拉到前台，把它的活儿干在前面。
        if not win32gui.IsIconic(h):
            win32gui.SetForegroundWindow(h)
            changed = True
    except Exception as e:
        log(f"  摆正微信窗口失败: {e}")
    if changed:
        time.sleep(0.4)
    return changed


def _quiet_wechatauto():
    """禁用 wechatauto 的「最小化遮挡窗口」。

    ⚠️ wechatauto 的 ensure_visible() 会先把**所有与微信几何重叠**的窗口
    SW_MINIMIZE 掉，再置顶微信。实测表现就是：她发消息时你屏幕上的东西
    全被切走（俗称「强制切桌面」）。

    这里把 _minimize_blockers 换成空操作 —— bring_to_front(keep_topmost=True)
    会把微信置顶，点击照样落得准，不需要先把别人的窗口最小化。

    如果哪天发送因为被遮挡而失败，把 bot-config 里 send.keepWindows 设成 false
    就能恢复原行为。
    """
    try:
        from wechatauto.guia import WeChatGUI
    except Exception:
        return
    if getattr(WeChatGUI, "_dsh_no_minimize", False):
        return
    try:
        sc = CFG.get("send") or {}
        if sc.get("keepWindows", True):
            WeChatGUI._minimize_blockers = lambda self: 0
            log("  已禁用「最小化遮挡窗口」（不会再切走你正在用的窗口）")
        WeChatGUI._dsh_no_minimize = True
    except Exception as e:
        log(f"  禁用最小化遮挡失败: {e}")


def _get_gui():
    """复用一个 WeChatGUI 实例（拿不到就回退到 quick_send）。"""
    _quiet_wechatauto()
    if _wx[0] is None:
        try:
            from wechatauto.guia import WeChatGUI
            _wx[0] = WeChatGUI()
        except Exception as e:
            log(f"  WeChatGUI 初始化失败，回退 quick_send：{e}")
            _wx[0] = False
    return _wx[0] or None


def _do_send(text: str, who: str, verify: bool):
    gui = _get_gui()
    if gui is not None:
        return gui.send_msg(text, who, verify)
    return quick_send(text, who=who, verify=verify)


def _do_send_image(path: str, who: str, verify: bool):
    """发本地图片（wechatauto 走剪贴板粘贴路线）。"""
    gui = _get_gui()
    if gui is None:
        raise RuntimeError("WeChatGUI 不可用，发不了图片")
    return gui.send_image(path, who, verify)


def _res_says_ok(res: Any) -> bool:
    """判断 wechatauto 的返回值是不是「成功」。

    ⚠️ 两个坑，实测踩过：
      ① 它有时返回 WxResponse 对象、有时返回 **dict**（send_image 给的是 dict，
         getattr(r, 'message') 拿不到东西，直接返回 r 本身）。
      ② 成功文案是「**已发送并确认**：xxx」—— 里面**没有「成功」二字**！
         按 '"成功" in message' 判会把成功全判成失败。所以优先看 status 字段。
    """
    if res is None:
        return False
    if isinstance(res, dict):
        st = str(res.get("status") or "")
        msg = str(res.get("message") or "")
    else:
        st = str(getattr(res, "status", "") or "")
        msg = str(getattr(res, "message", "") or "")
    if st:
        return st == "成功"
    return ("成功" in msg) or ("已发送并确认" in msg)


def _res_msg(res: Any) -> str:
    """取返回里的说明文字（dict / 对象都认），只用于打日志。"""
    if isinstance(res, dict):
        return str(res.get("message") or res)
    return str(getattr(res, "message", res) or res)


def _quick_send_image_safe(path: str, who: str, soft: float, hard: float):
    """发图的超时包装 —— 和 _quick_send_safe 同一套两级超时逻辑。

    ⚠️ 必须 verify=True。实测 2026-10-08 12:06：
       `send_image(verify=False)` 返回"成功"，但**数据库里根本没有那条图片消息** ——
       图片粘进了输入框，回车却没生效，然后她的配文一来就把草稿冲掉了，
       结果他一条图都没收到，日志里却是 OK。
       verify=True 会回查数据库确认，慢几秒但不会骗人。
    """
    box: Dict[str, Any] = {}

    def run():
        try:
            box["r"] = _do_send_image(path, who, True)
        except Exception as e:                  # noqa: BLE001
            box["e"] = e

    t = threading.Thread(target=run, daemon=True, name="wximg")
    t0 = time.time()
    t.start()
    t.join(soft)
    if not t.is_alive():
        if "e" in box:
            return None, time.time() - t0, f"ERR {box['e']}"
        return box.get("r"), time.time() - t0, "OK"

    log(f"    发图超过 {soft:.0f}s（多半在重跑布局校准），继续等…")
    t.join(max(1.0, hard - soft))
    if t.is_alive():
        return None, time.time() - t0, "TIMEOUT"
    if "e" in box:
        return None, time.time() - t0, f"ERR {box['e']}"
    return box.get("r"), time.time() - t0, "SLOW"


def _xml_unescape(s: str) -> str:
    return (s.replace("&lt;", "<").replace("&gt;", ">").replace("&quot;", '"')
             .replace("&apos;", "'").replace("&amp;", "&"))


def _hhmm(ts=None) -> str:
    """把一个 Unix 时间戳（或现在）格式化成 [10-06 01:53]。

    用**绝对日期**，不用「今天/昨天」——
    因为要让她能准确地"翻旧账"（「你上个月自己说过…」），
    相对日期一旦跨过零点就会算错。
    跨年的时候自动带上年份。
    """
    try:
        ts = float(ts) if ts not in (None, "", 0, "0") else time.time()
    except Exception:
        ts = time.time()
    if ts > 1e12:                      # 毫秒
        ts /= 1000.0
    lt = time.localtime(ts)
    if lt.tm_year != time.localtime().tm_year:
        return time.strftime("[%Y-%m-%d %H:%M]", lt)
    return time.strftime("[%m-%d %H:%M]", lt)


# 「周几」每天只给一次 —— 见 _now_note 的说明
_now_lock = threading.Lock()
_now_last_day: Dict[str, str] = {}


def _now_note(chat: str = "") -> str:
    """告诉她"现在几点" —— 放进**消息**里，不放 system prompt。

    放 system prompt 会让它每分钟变一次，直接打爆 API 的前缀缓存（成本翻十倍）。

    ⚠️ **周几只在每天第一条消息里给一次**（2026-10-08 改）。
    之前每轮都写「现在是 2026-10-08 周四 13:35」—— 结果她**每句话都提星期**
    （她会把最显眼的那条信息当成说话的话题）。周几一天只变一次，
    没必要每轮重播。
    """
    lt = time.localtime()
    day = time.strftime("%Y-%m-%d", lt)
    with _now_lock:
        fresh = _now_last_day.get(chat) != day
        _now_last_day[chat] = day
    wd = "（周%s）" % "一二三四五六日"[lt.tm_wday] if fresh else ""
    return "（现在是 %s%s %02d:%02d）" % (day, wd, lt.tm_hour, lt.tm_min)


class HealthServer:
    """接收手机推来的健康数据（Gadgetbridge 的 HealthPush）。

    手机 POST 到 http://<电脑>:<port>/hr，body：
        {"metric":"hr","value":78,"ts":1770000000,"source":"gadgetbridge"}

    注意：这个服务**只监听**，不主动连手机。手机在外面时推不进来，
    需要走隧道或回家再同步 —— 见说明书。
    """

    def __init__(self, bot, port: int, token: str = ""):
        self.bot = bot
        self.port = int(port)
        self.token = str(token or "")
        self.httpd = None
        self.thread = None

    def start(self) -> bool:
        import http.server

        outer = self

        class H(http.server.BaseHTTPRequestHandler):
            def log_message(self, *a):        # 别把访问日志打到 stderr
                pass

            def _json(self, code: int, obj) -> None:
                import json as _j
                body = _j.dumps(obj, ensure_ascii=False).encode("utf-8")
                self.send_response(code)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def _handle(self, body: bytes) -> None:
                import json as _j
                if outer.token:
                    got = self.headers.get("X-Token") or self.headers.get("x-token") or ""
                    if got != outer.token:
                        log("  健康推送：token 不对，拒绝")
                        self._json(403, {"ok": False, "error": "bad token"})
                        return
                try:
                    data = _j.loads(body.decode("utf-8") or "{}")
                except Exception as e:
                    self._json(400, {"ok": False, "error": "bad json: %s" % e})
                    return
                metric = str(data.get("metric") or "")
                # 诊断：手机到底连没连上来。开着的时候每条都记一下，
                # 不然防火墙挡着你也看不出来（现在是不通就完全静默）。
                if outer.bot.health_debug:
                    log(f"  健康推送 [{self.client_address[0]}] {metric}={data.get('value')}")
                try:
                    value = int(round(float(data.get("value"))))
                except Exception:
                    self._json(400, {"ok": False, "error": "bad value"})
                    return
                # 范围检查只对心率做 —— 步数可能上千，
                # 睡眠用 ActivityKind 的 code，都不是 20..250 这段
                if metric == "hr" and not (20 <= value <= 250):
                    self._json(200, {"ok": True, "ignored": True})
                    return
                # ── 喂给控制台 ────────────────────────────────────────
                try:
                    text = str(data.get("text") or "")
                    ts = float(data.get("ts") or 0) or time.time()
                    TELEM.stats["health_in"] += 1
                    TELEM.add_health_point(metric, value, text, ts)
                    # 手机主要推 samples 批次，单条指标很少 —— 批次要拆开喂曲线
                    if metric == "samples" and text:
                        TELEM.feed_samples(text)
                    TELEM.emit("health", metric=metric, value=value, text=text,
                               ts=ts, src=self.client_address[0])
                except Exception:
                    pass
                outer.bot._on_health(metric, value, str(data.get("text") or ""))
                self._json(200, {"ok": True})

            def do_POST(self):
                from urllib.parse import urlparse
                path = urlparse(self.path).path
                if outer.bot.health_debug:
                    log(f"  收到 HTTP POST 来自 {self.client_address[0]}  {path[:40]}")
                try:
                    n = int(self.headers.get("Content-Length") or 0)
                except Exception:
                    n = 0
                body = self.rfile.read(n) if n > 0 else b"{}"
                if path == "/api/clear":
                    return self._json(200, self._api_clear(body))
                self._handle(body)

            def do_GET(self):
                from urllib.parse import urlparse, parse_qs
                path = urlparse(self.path).path
                # 看板自己会每秒轮询 /api/state，别把它写进日志 ——
                # 不然她的一切真实活动都会被自己的监控刷掉（踩过）。
                _quiet = path in ("/api/state", "/console", "/console/", "/index.html",
                                   "/favicon.ico", "/events",
                                   "/api/chats", "/api/history",
                                   "/api/video",
                                   "/api/images", "/api/imagefile")
                if outer.bot.health_debug and not _quiet:
                    log(f"  收到 HTTP GET  来自 {self.client_address[0]}  {self.path[:60]}")

                # ── 控制台 ────────────────────────────────────────────
                if path in ("/console", "/console/", "/", "/index.html"):
                    return self._serve_console()
                if path == "/api/state":
                    return self._json(200, TELEM.snapshot())
                if path == "/events":
                    return self._serve_sse()
                # ── 看板：会话 / 对话历史 / 清上下文 ──────────────────
                if path == "/api/chats":
                    return self._json(200, self._api_chats())
                if path == "/api/history":
                    q = parse_qs(urlparse(self.path).query)
                    return self._json(200, self._api_history((q.get("chat") or [""])[0]))
                # ── 看板：配图 ────────────────────────────────────────
                if path == "/api/initiative":
                    return self._json(200, self._api_initiative())
                if path == "/api/life":
                    return self._json(200, self._api_life())
                if path == "/api/video":
                    return self._json(200, self._api_video())
                if path == "/api/images":
                    return self._json(200, self._api_images())
                if path == "/api/imagefile":
                    q = parse_qs(urlparse(self.path).query)
                    data = self._api_imagefile((q.get("name") or [""])[0])
                    if data is None:
                        return self._json(404, {"ok": False, "error": "没有这张图"})
                    ctype = ("image/png" if data[:8] == b"\x89PNG\r\n\x1a\n"
                             else "image/webp" if data[:4] == b"RIFF" and data[8:12] == b"WEBP"
                             else "image/jpeg")
                    self.send_response(200)
                    self.send_header("Content-Type", ctype)
                    self.send_header("Cache-Control", "max-age=600")
                    self.send_header("Content-Length", str(len(data)))
                    self.end_headers()
                    self.wfile.write(data)
                    return

                q = parse_qs(urlparse(self.path).query)
                if "v" in q:
                    self._handle(("{\"metric\":\"hr\",\"value\":%s}"
                                  % q["v"][0]).encode("utf-8"))
                else:
                    self._json(200, {"ok": True, "hint": "POST JSON to /hr",
                                     "console": "/console"})

            # ── 控制台：单页 ──────────────────────────────────────────
            def _serve_console(self) -> None:
                import os as _os
                fp = _os.path.join(HERE, "console.html")
                try:
                    with open(fp, "rb") as f:
                        body = f.read()
                except Exception as e:
                    self._json(500, {"ok": False, "error": "console.html 读不到: %s" % e})
                    return
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            # ── 看板：会话一览（私聊 + 群友）─────────────────────────
            def _bot(self):
                return getattr(outer, "bot", None)

            def _api_chats(self) -> Dict[str, Any]:
                b = self._bot()
                if b is None:
                    return {"ok": False, "error": "bot 未就绪"}
                now = time.time()
                chats = []
                for conf in (CFG.get("chats") or []):
                    name = str(conf.get("name") or "")
                    msgs = b.ctx.get(name)
                    chars = sum(len(m.get("content") or "") for m in msgs)
                    kind = str(conf.get("kind") or "").lower()
                    if kind not in ("private", "group"):
                        kind = ("group" if str(conf.get("mode") or "").lower() == "wake"
                                else "private")
                    try:
                        persona = os.path.basename(b._persona_path(name))
                    except Exception:
                        persona = ""
                    lu = b.last_user_at.get(name) or 0
                    lb = b.last_bot_at.get(name) or 0
                    chats.append({
                        "name": name, "kind": kind,
                        "mode": str(conf.get("mode") or ""),
                        "persona": persona,
                        "context": len(msgs), "chars": chars,
                        "memory": len(b.memory.get(name) or ""),
                        "lastUser": lu or None, "lastBot": lb or None,
                        "idleSec": (now - max(lu, lb)) if (lu or lb) else None,
                        "proactive": bool(conf.get("proactive")),
                    })
                return {"ok": True, "now": now, "chats": chats,
                        "nicknames": list(CFG.get("nicknames") or []),
                        "wakeProbability": CFG.get("wakeProbability"),
                        "maxBubbles": CFG.get("maxBubbles"),
                        "groupMaxBubbles": CFG.get("groupMaxBubbles"),
                        "personaDefault": os.path.basename(str(CFG.get("personaFile") or ""))}

            # ── 看板：视频 ────────────────────────────────────────────
            def _api_life(self) -> Dict[str, Any]:
                bot = self._bot()
                out: Dict[str, Any] = {"ok": True}
                try:
                    day = bot._life_load(bot._life_date(0)) or {}
                except Exception:
                    day = {}
                out["date"] = day.get("date") or bot._life_date(0)
                out["timeline"] = day.get("timeline") or []
                out["period"] = day.get("period") or ""
                out["weather"] = day.get("weather") or {}
                out["stateIn"] = day.get("state_in") or {}
                out["stateOut"] = day.get("state_out") or {}
                out["chats"] = day.get("chats") or []
                out["videos"] = day.get("videos") or []
                try:
                    out["state"] = bot._state_load() or {}
                except Exception:
                    out["state"] = {}
                try:
                    out["promises"] = bot._promises_load() or []
                except Exception:
                    out["promises"] = []
                try:
                    import life_planner as LP
                    mp = os.path.join(LP.MONTHS, out["date"][:7] + ".json")
                    if os.path.exists(mp):
                        out["month"] = json.load(open(mp, encoding="utf-8"))
                    else:
                        out["month"] = {}
                except Exception:
                    out["month"] = {}
                return out

            def _api_initiative(self) -> Dict[str, Any]:
                bot = self._bot()
                out: Dict[str, Any] = {"ok": True}
                try:
                    day = bot._life_load(bot._life_date(0)) or {}
                    out["planChats"] = day.get("chats") or []
                    out["planVideos"] = day.get("videos") or []
                except Exception:
                    out["planChats"] = []
                    out["planVideos"] = []
                try:
                    out["dynamicUsed"] = bot._dynamic_used()
                    out["dynamicMax"] = 3
                    st = bot._sched_load() or {}
                    out["lastDynamicAt"] = st.get("lastDynamicAt") or 0
                    out["decisionFired"] = (st.get("decisionFired", {}) or {}).get(
                        time.strftime("%Y-%m-%d"), [])
                except Exception:
                    out["dynamicUsed"] = 0
                try:
                    out["reminders"] = [r for r in bot._rem_load()
                                        if r.get("status") in ("open", "deferred")][-10:]
                except Exception:
                    out["reminders"] = []
                try:
                    out["intents"] = bot._intents_active()[-10:]
                except Exception:
                    out["intents"] = []
                out["points"] = ["10:00", "12:00", "14:00", "16:00", "18:00", "20:00", "22:00"]
                out["now"] = time.strftime("%Y-%m-%d %H:%M:%S")
                return out

            def _api_video(self) -> Dict[str, Any]:
                """今天分享了几次 + 最近看过的视频（从上下文里翻）。"""
                b = self._bot()
                if b is None:
                    return {"ok": False, "error": "bot 未就绪"}
                now = time.time()
                day = time.strftime("%Y-%m-%d")
                vc = CFG.get("video") or {}
                limit = int(vc.get("dailyLimit") or 3)
                # 内存空就先去状态文件捞（重启后第一次访问看板也能显示对）
                if not b.video_times:
                    try:
                        b.video_times = b._video_state_load()
                    except Exception:
                        pass
                times = [t for t in (b.video_times or [])
                         if time.strftime("%Y-%m-%d", time.localtime(t)) == day]

                videos = []
                for conf in (CFG.get("chats") or []):
                    name = str(conf.get("name") or "")
                    for m in (b.ctx.get(name) or []):
                        c = str(m.get("content") or "")
                        if "（你看完的视频《" not in c:
                            continue
                        rest = c.split("（你看完的视频《", 1)[1]
                        i = rest.rfind("》")
                        if i < 0:
                            continue
                        title = rest[:i]
                        summary = rest[i + 2:].rstrip("）").strip()
                        ts = 0.0
                        mm = re.match(r"^\[(\d{2}-\d{2}) (\d{2}:\d{2})\]", c)
                        if mm:
                            try:
                                ts = time.mktime(time.strptime(
                                    time.strftime("%Y-") + mm.group(1) + " " + mm.group(2),
                                    "%Y-%m-%d %H:%M"))
                            except Exception:
                                ts = 0.0
                        videos.append({"chat": name, "title": title,
                                       "summary": summary, "ts": ts or now})
                videos.sort(key=lambda x: x.get("ts") or 0, reverse=True)

                # 她分享视频发出去的原话（从 assistant 消息里找带 b23.tv 的）
                shares = []
                for conf in (CFG.get("chats") or []):
                    name = str(conf.get("name") or "")
                    for m in (b.ctx.get(name) or []):
                        if m.get("role") != "assistant":
                            continue
                        c = str(m.get("content") or "")
                        if "b23.tv/" in c:
                            shares.append({"chat": name, "content": c[:300]})
                shares = shares[-6:]

                return {"ok": True, "now": now,
                        "enabled": bool(vc.get("enabled", True)),
                        "dailyLimit": limit,
                        "todayCount": len(times),
                        "videos": videos[:6],
                        "shares": shares}

            # ── 看板：配图（图库 + 今日额度）─────────────────────────
            def _photo_dir(self) -> str:
                cfg = CFG.get("image") or {}
                return os.path.join(HERE, str(cfg.get("saveDir") or "images_out"))

            def _api_images(self) -> Dict[str, Any]:
                cfg = CFG.get("image") or {}
                d = self._photo_dir()
                # 先从 manifest 读出「情境 / 提示词 / 配文」——同名以**最后一条**为准
                # （出图时记一条，发完之后再补一条结果）
                meta: Dict[str, Dict[str, Any]] = {}
                try:
                    with open(os.path.join(d, "manifest.jsonl"), encoding="utf-8") as f:
                        for line in f:
                            line = line.strip()
                            if not line:
                                continue
                            try:
                                e = json.loads(line)
                            except Exception:
                                continue
                            n = str(e.get("name") or "")
                            if not n:
                                continue
                            meta.setdefault(n, {}).update(e)
                except OSError:
                    pass

                items: List[Dict[str, Any]] = []
                try:
                    for fn in os.listdir(d):
                        if not fn.lower().endswith((".png", ".jpg", ".jpeg",
                                                    ".webp", ".gif")):
                            continue
                        try:
                            st = os.stat(os.path.join(d, fn))
                        except OSError:
                            continue
                        m = meta.get(fn) or {}
                        items.append({
                            "name": fn, "size": st.st_size, "mtime": st.st_mtime,
                            "chat": m.get("chat") or "",
                            "situation": m.get("situation") or "",
                            "prompt": m.get("prompt") or "",
                            "caption": m.get("caption") or "",
                            "sent": m.get("sent"),          # None=还没发完
                            "model": m.get("model") or "",
                        })
                except OSError:
                    pass
                items.sort(key=lambda x: x["mtime"], reverse=True)

                b = self._bot()
                today: Dict[str, Any] = {}
                day = ""
                if b is not None:
                    today = dict(b._photo_count)
                    day = b._photo_day[0]
                return {
                    "ok": True,
                    "enabled": bool(cfg.get("enabled", True)),
                    "requireAsk": bool(cfg.get("requireAsk", True)),
                    "model": str(cfg.get("model") or ""),
                    "size": str(cfg.get("size") or ""),
                    "promptExtend": bool(cfg.get("promptExtend", False)),
                    "maxPerDay": int(cfg.get("maxPerDay") or 4),
                    "unitPrice": 0.25,          # 1K 的单价（2K 是 0.5）
                    "day": day,
                    "today": today,
                    "todayUsed": sum(int(v) for v in today.values()),
                    "dir": os.path.basename(d),
                    "refs": [os.path.basename(str(x)) for x in (cfg.get("refs") or [])],
                    "count": len(items),
                    "bytes": sum(int(i["size"]) for i in items),
                    "items": items[:150],
                    "now": time.time(),
                }

            def _api_imagefile(self, name: str) -> Optional[bytes]:
                """把图库里的某张读出来。

                ⚠️ 只认图库目录里的**文件名**（basename + realpath 双重校验），
                否则 ?name=../../.credentials.yaml 就能读走凭据文件。
                """
                d = os.path.realpath(self._photo_dir())
                fp = os.path.realpath(os.path.join(d, os.path.basename(str(name or ""))))
                if not fp.startswith(d + os.sep) or not os.path.isfile(fp):
                    return None
                with open(fp, "rb") as f:
                    return f.read()

            # ── 看板：某个会话的对话历史 ──────────────────────────────
            def _api_history(self, chat: str) -> Dict[str, Any]:
                b = self._bot()
                if b is None:
                    return {"ok": False, "error": "bot 未就绪"}
                if not chat:
                    return {"ok": False, "error": "要传 ?chat=名字"}
                msgs = b.ctx.get(chat)
                LIMIT = 400          # 太长的历史只回尾部，别把浏览器撑爆
                cut = max(0, len(msgs) - LIMIT)
                return {
                    "ok": True, "chat": chat, "count": len(msgs), "truncated": cut,
                    "chars": sum(len(m.get("content") or "") for m in msgs),
                    "memory": b.memory.get(chat) or "",
                    "messages": [{"role": m.get("role"), "content": m.get("content")}
                                 for m in msgs[cut:]],
                }

            # ── 看板：清上下文 ────────────────────────────────────────
            def _api_clear(self, body: bytes) -> Dict[str, Any]:
                b = self._bot()
                if b is None:
                    return {"ok": False, "error": "bot 未就绪"}
                try:
                    import json as _j
                    req = _j.loads(body.decode("utf-8") or "{}")
                except Exception as e:
                    return {"ok": False, "error": "bad json: %s" % e}
                if req.get("all"):
                    names = list(b.ctx.data.keys())
                elif req.get("chat"):
                    names = [str(req["chat"])]
                else:
                    return {"ok": False, "error": "要传 chat 或 all"}
                before = {n: len(b.ctx.get(n)) for n in names}
                bak = ""
                try:
                    import shutil as _sh
                    bak = (CFG["contextFile"] + ".bak-"
                           + time.strftime("%Y%m%d-%H%M%S") + "-console")
                    _sh.copy2(CFG["contextFile"], bak)
                except Exception as e:
                    log(f"  看板清空前备份失败（继续清）：{e}")
                for n in names:
                    b.ctx.reset(n)
                total = sum(before.values())
                log(f"  看板清空上下文：{'、'.join(names) or '（无）'}（清前共 {total} 条）")
                TELEM.emit("context_clear", chats=names, before=total, backup=bak)
                return {"ok": True, "cleared": names, "before": before, "backup": bak}

            # ── 控制台：SSE ──────────────────────────────────────────
            def _serve_sse(self) -> None:
                q = TELEM.subscribe()
                if q is None:
                    self._json(503, {"ok": False, "error": "连接数已满"})
                    return
                try:
                    self.send_response(200)
                    self.send_header("Content-Type", "text/event-stream; charset=utf-8")
                    self.send_header("Cache-Control", "no-cache")
                    self.send_header("Connection", "keep-alive")
                    self.send_header("X-Accel-Buffering", "no")
                    self.end_headers()
                    import json as _j
                    self.wfile.write(("event: hello\ndata: %s\n\n"
                                      % _j.dumps(TELEM.snapshot(), ensure_ascii=False)).encode("utf-8"))
                    self.wfile.flush()
                    last_ping = time.time()
                    while True:
                        try:
                            ev = q.get(timeout=2.0)
                            payload = _j.dumps(ev, ensure_ascii=False)
                            self.wfile.write(("data: %s\n\n" % payload).encode("utf-8"))
                            self.wfile.flush()
                        except Exception:
                            pass          # 超时 → 发心跳，顺便探测连接是否还活着
                        if time.time() - last_ping >= 10:
                            last_ping = time.time()
                            self.wfile.write(b": ping\n\n")
                            self.wfile.flush()
                except Exception:
                    pass
                finally:
                    TELEM.unsubscribe(q)

        try:
            self.httpd = http.server.ThreadingHTTPServer(("0.0.0.0", self.port), H)
        except Exception as e:
            log(f"  健康接收端口 {self.port} 起不来: {e}")
            return False
        self.thread = threading.Thread(target=self.httpd.serve_forever,
                                       daemon=True, name="health")
        self.thread.start()
        log(f"  健康数据接收已开: http://0.0.0.0:{self.port}/hr"
            + ("  （需要 X-Token）" if self.token else ""))
        log(f"  监控看板: http://127.0.0.1:{self.port}/console")
        return True


def _image_b64(raw: bytes) -> str:
    """图片字节 → base64 data URL（喂给视觉模型）。

    实测 deepseek-flash 本身就能看图（一张图 ~230-480 token，很便宜），
    不需要换 deepseek-v4-flash-vision-exp。
    """
    import base64
    if not raw:
        return ""
    if raw[:3] == b"\xff\xd8\xff":
        mime = "jpeg"
    elif raw[:4] == b"\x89PNG":
        mime = "png"
    elif raw[:4] == b"GIF8":
        mime = "gif"
    else:
        mime = "jpeg"
    return f"data:image/{mime};base64," + base64.b64encode(raw).decode()


def _image_bytes(db, user: str, local_id) -> Optional[bytes]:
    """把微信里的一张图拿到手（返回原始字节）。

    坑踩了很多次，最后是这条路：
      ① MediaDownloader 直接下载 → 得到 .wxgf，那是**加密的新格式**，
         decrypt_image 不认（它只认旧的 V1/V2 .dat）
      ② download_image_original 会去操作界面（滚动、截图）→ **会卡住**，
         而且和 bot 抢同一个微信窗口，绝对不能在主流程里调
      ③ 最后走通了：微信自己在 cache 里存了 .dat（头就是 V2_MAGIC），
         文件名是 md5 —— 用 image_status() 拿到 md5，glob 找文件，decrypt_image() 解密
    """
    if not user or local_id in (None, "", 0, "0"):
        return None
    try:
        from wechatauto.media import MediaDownloader
        dl = MediaDownloader(db)
        st = dl.image_status(user, int(local_id)) or {}
        md5 = str(st.get("md5") or "").strip()
        if len(md5) < 16:
            return None
        root = str(getattr(db, "account_dir", "") or "")
        cands = []
        for suffix in ("_b.dat", ".dat", "_t.dat"):
            hit = glob.glob(os.path.join(root, "**", md5 + suffix), recursive=True)
            if hit:
                cands = hit
                break
        for p in cands:
            try:
                raw = dl.decrypt_image(p)
            except Exception:
                continue
            if raw and (raw[:3] == b"\xff\xd8\xff" or raw[:4] == b"\x89PNG" or raw[:4] == b"GIF8"):
                return raw
    except Exception as e:
        log(f"  图片解密失败: {e}")
    return None


def _extract_appmsg_text(xml: str) -> str:
    """从微信的 appmsg XML 里抠出可读文字。

    引用消息（appmsg type=57）的结构大致是：
        <appmsg><title>人打的字</title>
          <refermsg><displayname>谁</displayname><content>被引用的原文</content></refermsg>
        </appmsg>
    链接/文件卡片是 <title> 标题 + <des> 描述。
    抠不出来（表情、红包、转账）就返回空字符串 —— 调用方会跳过。
    """
    if "<" not in xml:
        return xml.strip()

    def grab(tag: str, pattern: str) -> str:
        m = re.search(pattern, xml, re.S)
        return _xml_unescape(m.group(1).strip()) if m else ""

    title = grab("title", r"<title>(.*?)</title>")
    refer = grab("content", r"<refermsg>.*?<content>(.*?)</content>")
    des = grab("des", r"<des>(.*?)</des>")
    # 链接卡片：URL 在 <url> 里，不抠出来的话下游认不出是 B站链接（踩过）
    url = grab("url", r"<url>(.*?)</url>")

    # 被引用的原文可能本身还是 XML（引用里套引用），再剥一层
    if refer.startswith("<"):
        refer = _extract_appmsg_text(refer)

    parts = []
    if refer:
        parts.append(f"（引用：{refer[:100]}）")
    if title:
        parts.append(title[:200])
    elif des:
        parts.append(des[:200])
    # 只在真的是 http(s) 链接时附加，别把空标签塞进去
    if url and url.lower().startswith("http"):
        parts.append(url[:300])
    return " ".join(parts).strip()


def _quick_send_safe(text: str, who: str, verify: bool, soft: float, hard: float):
    """带两级超时的发送。

    为什么要两级：实测 wechatauto 在探测不到输入框时会**自动重跑布局校准**，
    整轮要 30~45 秒 —— 那不是卡死，是在干活。所以：
      - soft 秒没回来 → 只是打条日志，**继续等**（锁不能放）
      - hard 秒还没回来 → 才判定真卡住
    关键是**锁必须等线程真的结束才能放**，否则两个 UI 自动化会同时跑、互相打架。

    返回 (结果, 耗时, 状态)：OK / SLOW / TIMEOUT
    """
    box: Dict[str, Any] = {}

    def run():
        try:
            box["r"] = _do_send(text, who, verify)
        except Exception as e:                  # noqa: BLE001
            box["e"] = e

    t = threading.Thread(target=run, daemon=True, name="wxsend")
    t0 = time.time()
    t.start()
    t.join(soft)
    if not t.is_alive():
        if "e" in box:
            return None, time.time() - t0, f"ERR {box['e']}"
        return box.get("r"), time.time() - t0, "OK"

    log(f"    发送超过 {soft:.0f}s（多半在重跑布局校准），继续等…")
    t.join(max(1.0, hard - soft))
    if t.is_alive():
        return None, time.time() - t0, "TIMEOUT"
    if "e" in box:
        return None, time.time() - t0, f"ERR {box['e']}"
    return box.get("r"), time.time() - t0, "SLOW"


# ─────────────────────────────── 上下文仓库 ───────────────────────────────
class ContextStore:
    """每个会话一套独立上下文。互不共享、各自滚动、落盘。

    用户明确要求：**不要重复用上下文** —— 群 A 的聊天不会被带进群 B，
    私聊也不会被带进群聊。
    """

    def __init__(self, path: str, limit: int) -> None:
        self.path = path
        self.limit = limit
        self.lock = threading.Lock()
        self.data: Dict[str, List[Dict[str, str]]] = {}
        self._load()

    def _load(self) -> None:
        try:
            with open(self.path, encoding="utf-8") as f:
                raw = json.load(f)
            if isinstance(raw, dict):
                self.data = {k: v for k, v in raw.items() if isinstance(v, list)}
                log(f"上下文已载入：{len(self.data)} 个会话")
        except FileNotFoundError:
            pass
        except Exception as e:
            log(f"上下文读取失败（忽略）：{e}")

    def _save(self) -> None:
        try:
            tmp = self.path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self.data, f, ensure_ascii=False, indent=1)
            os.replace(tmp, self.path)
        except Exception as e:
            log(f"上下文落盘失败：{e}")

    def add(self, chat: str, role: str, content: str) -> None:
        with self.lock:
            lst = self.data.setdefault(chat, [])
            lst.append({"role": role, "content": content})
            # 滚动窗口：只留最近 limit 条。
            # ⚠️ limit <= 0 表示**不截断** —— 历史无限增长，
            #    由 Bot._maybe_compact 在超过预算时把最老的压成长期记忆。
            if self.limit > 0 and len(lst) > self.limit:
                del lst[: len(lst) - self.limit]
            self._save()

    def drop_oldest(self, chat: str, n: int) -> None:
        """砍掉最老的 n 条（压成记忆之后调用）。"""
        if n <= 0:
            return
        with self.lock:
            lst = self.data.get(chat) or []
            if lst:
                del lst[:n]
                self._save()

    def chars(self, chat: str) -> int:
        """这个会话历史的总字符数（用来估 token 预算）。"""
        return sum(len(m.get("content") or "") for m in self.data.get(chat, []))

    def get(self, chat: str) -> List[Dict[str, str]]:
        with self.lock:
            return [dict(m) for m in self.data.get(chat, [])]

    def reset(self, chat: str) -> None:
        with self.lock:
            self.data.pop(chat, None)
            self._save()


class MemoryStore:
    """长期记忆：超长对话被压缩成摘要后存在这儿，每轮注回系统提示词。

    ⚠️ 为什么需要它：DeepSeek 的 /chat/completions 是**无状态**的 ——
    服务端不存你的对话，每次都得把完整 messages 发过去。
    DSH 里那套自动压缩是 DSH 自己的插件（dsh-compaction-basic）干的，
    直连 API 没有任何压缩，所以只能自己做。
    模型窗口是 1M token，但**会满**；满了不压就会直接报错。
    """

    def __init__(self, path: str) -> None:
        self.path = path
        self.lock = threading.Lock()
        self.data: Dict[str, str] = {}
        try:
            with open(self.path, encoding="utf-8") as f:
                raw = json.load(f)
            if isinstance(raw, dict):
                self.data = {k: str(v) for k, v in raw.items()}
                if self.data:
                    log(f"长期记忆已载入：{len(self.data)} 个会话")
        except FileNotFoundError:
            pass
        except Exception as e:
            log(f"长期记忆读取失败（忽略）：{e}")

    def _save(self) -> None:
        try:
            tmp = self.path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self.data, f, ensure_ascii=False, indent=1)
            os.replace(tmp, self.path)
        except Exception as e:
            log(f"长期记忆落盘失败：{e}")

    def get(self, chat: str) -> str:
        with self.lock:
            return self.data.get(chat, "")

    def append(self, chat: str, text: str) -> None:
        if not text:
            return
        with self.lock:
            old = self.data.get(chat, "")
            self.data[chat] = (old + "\n" + text).strip() if old else text.strip()
            self._save()


# ──────────────────────────────── DeepSeek ────────────────────────────────
def _user_content(text: str, images: Optional[List[str]] = None):
    """构造 user 消息内容。有图片时用多模态数组格式。

    实测 deepseek-flash 本身就能看图，不需要换 vision 专用模型。
    """
    imgs = [u for u in (images or []) if u]
    if not imgs:
        return text
    parts: List[Dict[str, Any]] = [{"type": "text", "text": text or "（他发了一张图）"}]
    for u in imgs[:3]:                      # 一轮最多带 3 张，防爆 token
        parts.append({"type": "image_url", "image_url": {"url": u}})
    return parts


class LLM:
    def __init__(self, api_key: str) -> None:
        self.key = api_key
        self.url = CFG["baseUrl"].rstrip("/") + "/chat/completions"
        self.calls = 0
        self.tokens_in = 0
        self.tokens_out = 0
        self.last_in = 0
        self.last_out = 0
        self.last_cached = 0

    def chat(self, system: str, history: List[Dict[str, str]], user: str,
             images: Optional[List[str]] = None) -> str:
        # 滚动窗口有可能从一条 assistant 消息开始（把对应的 user 挤掉了），
        # 那样 API 会觉得上下文不完整。丢掉开头连续的 assistant。
        h = list(history)
        while h and h[0].get("role") != "user":
            h.pop(0)
        body: Dict[str, Any] = {
            "model": CFG["model"],
            "max_tokens": int(CFG.get("maxTokens") or 200),
            "stream": False,
            "messages": [{"role": "system", "content": system}] + h
                        + [{"role": "user", "content": _user_content(user, images)}],
        }
        mode = str(CFG.get("thinking") or "low").strip().lower()
        if mode in ("off", "disabled", "none", "false", "0"):
            # 关掉思考，这时 temperature 才生效
            body["thinking"] = {"type": "disabled"}
            body["temperature"] = float(CFG.get("temperature") or 1.15)
        else:
            body["thinking"] = {"type": "enabled"}
            body["reasoning_effort"] = mode if mode in ("low", "high", "max") else "low"

        req = urllib.request.Request(
            self.url,
            data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {self.key}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        _t0 = time.time()
        # 看板：把"她准备说什么"的完整输入也发出去（含 system，这是人设）
        try:
            TELEM.emit("llm_request",
                       model=body.get("model"),
                       thinking=body.get("thinking"),
                       effort=body.get("reasoning_effort"),
                       max_tokens=body.get("max_tokens"),
                       system=system,
                       history=len(h),
                       user=user)
        except Exception:
            pass
        # ── 发请求：先流式，挂了回退非流式 ──────────────────────────────
        # 流式能把**思考过程一帧一帧推给看板**（实测 reasoning 首帧 +0.6s 就到）。
        # 但它属于动核心链路，所以流式一旦出问题必须**无缝退回**下面这条老路径 ——
        # 回复本身绝不能因为看板好看不好看而丢掉。
        j = None
        try:
            j = self._request_stream(body, _t0)
        except Exception as e:
            log(f"  流式请求不可用（{e.__class__.__name__}: {e}），回退非流式")

        if j is None:
            try:
                with urllib.request.urlopen(req, timeout=90) as r:
                    j = json.loads(r.read().decode("utf-8"))
            except urllib.error.HTTPError as e:
                detail = ""
                try:
                    detail = e.read().decode("utf-8", "replace")[:300]
                except Exception:
                    pass
                log(f"  API HTTP {e.code}: {detail}")
                try:
                    TELEM.emit("llm_error", code=e.code, detail=detail,
                               ms=int((time.time() - _t0) * 1000))
                except Exception:
                    pass
                return ""
            except Exception as e:
                log(f"  API 出错: {e}")
                try:
                    TELEM.emit("llm_error", detail=str(e), ms=int((time.time() - _t0) * 1000))
                except Exception:
                    pass
                return ""

        _ms = int((time.time() - _t0) * 1000)
        try:
            u = j.get("usage") or {}
            self.calls += 1
            self.last_in = int(u.get("prompt_tokens") or 0)
            self.last_out = int(u.get("completion_tokens") or 0)
            self.tokens_in += self.last_in
            self.tokens_out += self.last_out
            self.last_cached = int(((u.get("prompt_tokens_details") or {}).get("cached_tokens")) or 0)
            TELEM.stats["llm_calls"] = self.calls
            TELEM.stats["tokens_in"] = self.tokens_in
            TELEM.stats["tokens_out"] = self.tokens_out
            TELEM.stats["cached"] += self.last_cached
        except Exception:
            pass

        try:
            msg = j["choices"][0]["message"]
        except Exception:
            log(f"  返回结构异常: {str(j)[:200]}")
            return ""
        text = (msg.get("content") or "").strip()
        # 思考过程 —— 字段名各家不一，能抓的都抓
        think = (msg.get("reasoning_content") or msg.get("reasoning")
                 or msg.get("thinking") or "")
        if isinstance(think, dict):
            think = think.get("content") or think.get("text") or ""
        think = str(think or "").strip()
        try:
            TELEM.last_llm = {
                "ms": _ms, "in": self.last_in, "out": self.last_out,
                "cached": self.last_cached, "thinking": bool(think),
                "call": self.calls, "t": time.time(),
                "model": body.get("model"), "effort": body.get("reasoning_effort"),
            }
            TELEM.emit("llm_reply", text=text, thinking=think,
                       ms=_ms, in_tok=self.last_in, out_tok=self.last_out,
                       cached=self.last_cached, call=self.calls,
                       finish=(j["choices"][0].get("finish_reason") if j.get("choices") else None))
        except Exception:
            pass
        return text

    def _request_stream(self, body: Dict[str, Any], t0: float) -> Optional[Dict[str, Any]]:
        """流式请求：边收边把思考推给看板，最后拼成**和非流式响应同构**的 dict。

        为什么要同构：下游解析（usage / choices[0].message / reasoning_content）
        一行都不用动，出错时也能无缝退回旧的 urlopen 路径。

        看板那边靠 llm_thinking 事件实时刷新思考面板；
        限流到 0.35s 一帧，免得把 SSE 和 800 条的事件环刷爆。
        """
        b = dict(body)
        b["stream"] = True
        b["stream_options"] = {"include_usage": True}
        req = urllib.request.Request(
            self.url,
            data=json.dumps(b, ensure_ascii=False).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {self.key}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        text_parts: List[str] = []
        think_parts: List[str] = []
        usage: Dict[str, Any] = {}
        finish: Optional[str] = None
        frames = 0
        last_push = 0.0
        with urllib.request.urlopen(req, timeout=90) as r:
            for raw in r:
                line = raw.decode("utf-8", "replace").strip()
                if not line.startswith("data:"):
                    continue          # 空行 / 注释 / 心跳
                payload = line[5:].strip()
                if payload == "[DONE]":
                    break
                try:
                    frame = json.loads(payload)
                except Exception:
                    continue
                frames += 1
                if frame.get("usage"):
                    usage = frame["usage"]
                choices = frame.get("choices") or []
                if not choices:
                    continue
                if choices[0].get("finish_reason"):
                    finish = choices[0]["finish_reason"]
                delta = choices[0].get("delta") or {}
                rc = delta.get("reasoning_content") or delta.get("reasoning")
                if rc:
                    think_parts.append(str(rc))
                    now = time.time()
                    if now - last_push >= 0.35:
                        last_push = now
                        try:
                            TELEM.emit("llm_thinking", text="".join(think_parts),
                                       partial=True, ms=int((now - t0) * 1000))
                        except Exception:
                            pass
                c = delta.get("content")
                if c:
                    text_parts.append(str(c))

        if frames == 0:
            # 一帧都没收到 → 当成失败，让上层回退非流式（而不是发一条空消息）
            raise RuntimeError("流式没收到任何帧")

        think = "".join(think_parts)
        text = "".join(text_parts)
        if think:
            try:
                TELEM.emit("llm_thinking", text=think, partial=False,
                           ms=int((time.time() - t0) * 1000))
            except Exception:
                pass
        return {
            "choices": [{"message": {"content": text, "reasoning_content": think},
                         "finish_reason": finish}],
            "usage": usage,
        }


# ────────────────────────────────── 主逻辑 ──────────────────────────────────
class Bot:
    def __init__(self) -> None:
        self.db = WeChatDB()
        self.me = self.db.get_self_info() or {}
        self.listener = Listener(self.db, interval=1.0)
        # historyLimit <= 0 表示**不截断** —— 历史无限增长，
        # 超过 compactAtChars 时由 _maybe_compact 把最老的压成长期记忆。
        # （注意不能用 `or 12`：0 是假值，会被悄悄变成 12）
        _hl = CFG.get("historyLimit")
        _hl = int(_hl) if _hl is not None else 12
        self.ctx = ContextStore(CFG["contextFile"], _hl)
        self.memory = MemoryStore(
            os.path.join(os.path.dirname(CFG["contextFile"]) or ".", "memory.json"))
        # 发照片的每日配额：{"会话名": 张数}，跨天自动清零
        self._photo_day = [""]
        self._photo_count: Dict[str, int] = {}
        self.llm = LLM(CFG["_apiKey"])
        self.chat_by_name = {c["name"]: c for c in CFG.get("chats") or []}
        self._sys_cache: Dict[str, Any] = {}
        self.q: "queue.Queue[Dict[str, Any]]" = queue.Queue()
        # 去抖缓冲：等他停下来再一起回，而不是每条都回一次
        self.pending: Dict[str, List[Dict[str, Any]]] = {}
        self.pending_at: Dict[str, float] = {}      # 这个会话最后收到消息的时刻
        self.pending_start: Dict[str, float] = {}   # 这个会话第一句的时刻
        self.deb_lock = threading.Lock()
        self.send_times: List[float] = []
        self.recent_sent: List[str] = []
        self.followup_until: Dict[str, float] = {}
        self.wxid_to_name: Dict[str, str] = {}
        # ── 主动找人用的状态 ──
        self.last_user_at: Dict[str, float] = {}     # 他最后一次说话
        self.last_her_at: Dict[str, float] = {}      # 她最后一次说话（判断她醒着没）
        self.last_real_user_at: Dict[str, float] = {} # 他**真的**发过消息（区别于启动时的占位）
        self.recent_user_msgs: Dict[str, List[float]] = {}   # 他的消息时间戳（判断"连发"）
        # 每个会话最近的图片 (时间戳, local_id)。群聊里别人发的图 + 另一个人 @ 她，
        # 两条消息不同发送者、不会合并 —— 所以要靠这个"回头看"把图带上。
        self.recent_imgs: Dict[str, List[tuple]] = {}
        self.health_server = None
        self.health_debug = bool((CFG.get("health") or {}).get("debug"))
        # 健康数据状态（心率 / 睡眠 / 防打扰）
        self._health_state: Dict[str, Any] = {
            "last_hr": 0, "hr_at": 0.0, "hr_since": 0.0, "hr_kind": "",
            "last_stress": 0, "asleep_since": 0.0, "last_sleep_h": 0.0,
            "workout_active": False, "workout_sport": "",
            "workout_started": 0.0, "workout_ended": 0.0,
            "last_fire": 0.0, "day": "", "count": 0,
            # 批量重放用：处理到哪个时间戳了 / 上一段的睡眠状态
            "_batch_seen": 0, "_last_kind": "",
        }
        self.last_bot_at: Dict[str, float] = {}      # 她最后一次说话
        self.pro_stage: Dict[str, int] = {}          # 他这次沉默里她主动到第几步
        self.pro_need: Dict[str, Any] = {}           # （stage, 随机出来的阈值分钟）
        self.pro_times: List[float] = []             # 主动过的时间，算每日上限
        self.video_times: List[float] = []           # 发过视频的时间，算每日上限
        self.pro_busy = threading.Lock()

        log(f"模型 {CFG['model']} | 会话 {len(self.chat_by_name)} 个")
        for c in CFG.get("chats") or []:
            log(f"  - {c['name']}  策略={c.get('mode')}  人设={os.path.basename(self._persona_path(c['name']))}")

    # ── 人设（每个会话可以挂不同的）──
    def _persona_path(self, chat: str) -> str:
        conf = self.chat_by_name.get(chat) or {}
        return str(conf.get("personaFile") or CFG.get("personaFile") or "")

    def system_for(self, chat: str) -> str:
        """每个会话一套系统提示词 —— 私聊可以是芙宁娜，群里还是大肥鱼。

        按文件 mtime 缓存，改人设文件后下一条消息就生效。
        """
        conf = self.chat_by_name.get(chat) or {}
        p = self._persona_path(chat)
        note = str(conf.get("runtimeNote") or CFG.get("runtimeNote") or "")
        try:
            mt = os.stat(p).st_mtime
        except Exception:
            mt = 0.0
        key = f"{p}|{mt}|{len(note)}"
        c = self._sys_cache.get(chat)
        if c and c[0] == key:
            return c[1]
        sysm = (note + "\n\n---\n\n" + load_persona(p)).strip()
        self._sys_cache[chat] = (key, sysm)
        return sysm

    # ── 长期记忆 ──
    def _voice_to_text(self, chat: str, local_id: int) -> str:
        """一条语音消息 → 文字。任何一步失败都返回空串（那就只留 [语音] 标记）。

        链路：微信本地库 media_*.db → SILK 文件 → silk_v3_decoder → PCM
              → 套 WAV 头 → 千问 ASR → 文字
        """
        a = CFG.get("asr") or {}
        if not a.get("enabled", True):
            return ""
        try:
            from wechatauto.media import MediaDownloader
            dl = MediaDownloader(self.db)
            st = dl.voice_status(chat, int(local_id)) or {}
            if not st.get("available"):
                log(f"    语音拿不到（{st.get('reason') or '未知'}），只留标记")
                return ""
            tmp = os.path.join(HERE, "voice_tmp")
            os.makedirs(tmp, exist_ok=True)
            t0 = time.time()
            silk = dl.download_voice(chat, int(local_id), save_dir=tmp)
            if not silk:
                log("    语音文件没下下来，只留标记")
                return ""
            wav = silk_to_wav(silk)
            secs = (len(wav) - 44) / 2.0 / SILK_RATE
            cap = float(a.get("maxSeconds") or 180)
            if secs > cap:
                log(f"    语音 {secs:.1f} 秒超过上限 {cap:.0f}，不转")
                return ""
            r = asr_transcribe(wav, a) or {}
            text = str(r.get("text") or "")
            if not text:
                log(f"    语音转文字没出文字（{secs:.1f}秒，{time.time() - t0:.1f}s）")
                return ""
            # 声音描述：只给"听到什么"，不给"他是什么情绪" —— 判断留给模型。
            obs = voice_note(text, secs, str(r.get("emotion") or ""), a)
            log(f"    语音转文字（{secs:.1f}秒，{time.time() - t0:.1f}s）"
                f" 声学={r.get('emotion') or '-'}"
                f"{(' → ' + obs) if obs else ''}：{text[:50]}")
            if obs:
                text = f"{text}（声音：{obs}）"
            try:
                os.remove(silk)
            except OSError:
                pass
            return text
        except Exception as e:
            log(f"    语音转文字失败：{type(e).__name__}: {e}")
            return ""

    def memory_block(self, chat: str) -> str:
        """长期记忆区块，接在系统提示词后面。

        不进 history —— 它是"背景"，不是刚说过的话。
        """
        mem = self.memory.get(chat)
        if not mem:
            return ""
        return ("\n\n---\n\n【你还记得的往事】\n"
                "（这是很久以前发生的事，已经被压成摘要。可以自然地提，"
                "但别当成刚刚才说的。）\n" + mem)

    def _maybe_compact(self, chat: str, hist: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """历史超预算时，把最老的一段压成长期记忆再从窗口里删掉。

        ⚠️ 为什么非做不可：DeepSeek 的 /chat/completions 是**无状态**的，
        每次都要把完整 messages 发过去；服务端不会帮你截断或摘要。
        DSH 里那套自动压缩是 DSH 自己的插件干的，直连 API 没有。
        模型窗口 1M token 会满，满了不压就**直接报错**。
        """
        budget = int(CFG.get("compactAtChars") or 0)
        if budget <= 0:
            return hist
        chars = sum(len(m.get("content") or "") for m in hist)
        if chars <= budget:
            return hist
        keep = int(CFG.get("compactKeepRecent") or 20)
        if len(hist) <= keep + 4:
            return hist
        old = hist[: len(hist) - keep]
        log(f"  上下文 {chars} 字符 > 预算 {budget} —— 把最老 {len(old)} 条压成记忆")
        transcript = "\n".join(
            f"{'他' if m.get('role') == 'user' else '我'}: {m.get('content') or ''}"
            for m in old)
        prompt = (
            "你是记忆整理助手。把下面这段两个人在微信上的对话压缩成一份要点。\n"
            "必须保留：他告诉过你的关于他自己的事实（身份、在做什么、家人、喜好、忌讳）、"
            "你们之间的约定、反复出现的话题、他的情绪与状态变化、你答应过他的事。\n"
            "可以丢掉：寒暄、重复、一次性的玩笑。\n"
            "用第三人称「他」和「我」，条目式，不超过 500 字，不要评论、不要客套。"
        )
        try:
            s = self.llm.chat(prompt, [], transcript, images=None)
        except Exception as e:
            log(f"  压缩失败（这轮先不压）：{e}")
            return hist
        if not s:
            log("  压缩没出结果（这轮先不压）")
            return hist
        self.memory.append(chat, s.strip())
        self.ctx.drop_oldest(chat, len(old))
        log(f"  已压缩：记忆 {len(self.memory.get(chat))} 字符，窗口剩 {self.ctx.chars(chat)} 字符")
        return self.ctx.get(chat)

    # ── 启动 ──
    def start(self) -> None:
        n = 0
        for c in CFG.get("chats") or []:
            name = c["name"]
            wx = self._wxid_of(name)
            if not wx:
                log(f"  ✗ 找不到聊天「{name}」——检查名字是否和微信里显示的一致")
                continue
            self.wxid_to_name[wx] = name
            try:
                self.listener.add_listener(wx, self._make_cb(wx, name))
                n += 1
            except Exception as e:
                log(f"  监听「{name}」失败: {e}")
        self.listener.start()
        threading.Thread(target=self._worker, daemon=True, name="worker").start()
        threading.Thread(target=self._proactive_loop, daemon=True, name="proactive").start()
        threading.Thread(target=self._lifeplan_loop, daemon=True, name="lifeplan").start()
        threading.Thread(target=self._reminder_loop, daemon=True, name="reminder").start()
        threading.Thread(target=self._dynamic_loop, daemon=True, name="dynamic").start()

        # ── 调试钩子：想让她立刻发一条视频，就在这个文件里写「会话名:搜索词」，
        #    启动后 8 秒自动触发一次，然后把文件删掉。不写文件就完全不生效。
        _trig_path = os.path.join(os.path.dirname(HERE), "_trigger_video.txt")
        if os.path.exists(_trig_path):
            try:
                _raw = open(_trig_path, encoding="utf-8-sig").read().strip()
                os.remove(_trig_path)
                _parts = _raw.split(":", 1)
                _c = _parts[0].strip() if _parts[0].strip() else "旅行者"
                _k = _parts[1].strip() if len(_parts) > 1 and _parts[1].strip() else "猫咪 搞笑"
                log(f"  【调试触发】8 秒后让「{_c}」发一次视频（搜「{_k}」）")
                threading.Timer(
                    8.0, lambda: self._video_task(_c, _k)).start()
            except Exception as e:
                log(f"  调试触发失败: {e}")

        # 健康数据接收（手机上的 Gadgetbridge 推过来）
        hc = CFG.get("health") or {}
        if hc.get("enabled"):
            srv = HealthServer(self, int(hc.get("port") or 8765),
                               str(hc.get("token") or ""))
            if srv.start():
                self.health_server = srv


        # 从上下文恢复"他说过话"的状态 —— last_user_at 是内存态，重启就没了，
        # 不恢复的话作息表（饭点、晚安、失眠）重启后永远不触发。
        # proactive: true 的会话**无条件**算作"认识他" —— 清空上下文之后也要照常发，
        # 不然刚清完她就不吭声了（踩过）。
        seeded = 0
        for name, conf in self.chat_by_name.items():
            if self.ctx.get(name) or conf.get("proactive"):
                self.last_user_at[name] = time.time()
                self.last_bot_at[name] = time.time()
                seeded += 1
        if seeded:
            log(f"恢复了 {seeded} 个会话的活跃状态（作息表要用）")

        log(f"开始监听 {n} 个会话")
        pc = CFG.get("proactive") or {}
        if pc.get("enabled"):
            who = [c["name"] for c in CFG.get("chats") or [] if c.get("proactive")]
            log(f"主动消息：开启，适用 {('、'.join(who)) or '(无)'}" if who else "主动消息：开启，但没有会话勾选 proactive")

    def _wxid_of(self, name: str) -> Optional[str]:
        """按会话名找 wxid。

        ⚠️ get_nickname() 返回的是「备注优先，没备注才用昵称」——
        他在微信里改了备注，这里就找不到了。所以支持 aliases：
            {"name": "旅行者", "aliases": ["主人"]}
        """
        conf = self.chat_by_name.get(name) or {}
        want = {name}
        for a in (conf.get("aliases") or []):
            if a:
                want.add(str(a))
        try:
            for s in self.db.get_sessions(limit=200):
                u = s.get("username") or ""
                if not u:
                    continue
                if (self.db.get_nickname(u) or "") in want:
                    return u
        except Exception as e:
            log(f"  会话查找失败: {e}")
        return None

    # ── 入站 ──
    def _make_cb(self, wx: str, name: str):
        def cb(msg: Dict[str, Any], _lst: Any) -> None:
            try:
                self._on_message(wx, name, msg)
            except Exception as e:
                log(f"  处理消息出错: {e}")
        return cb

    def _on_message(self, chat: str, name: str, msg: Dict[str, Any]) -> None:
        # 每条入站消息都顺手检查一次配置有没有变。
        # 只在 worker 循环里检查是不够的：没有消息进队列时 worker 不跑，
        # 改了配置（比如删唤醒词）会一直不生效，直到下一条消息才姗姗来迟。
        reload_config()

        conf = self.chat_by_name.get(name) or {}
        mode = conf.get("mode") or "wake"
        if mode == "off":
            return

        # 自己的消息：群里自己是 sender_id=2，且正文没有 wxid 前缀。
        # 认不出来就会**自问自答**（模型把自己刚说的话当新消息再回一遍）。
        if int(msg.get("sender_id") or 0) == int(CFG.get("selfSenderId") or 2):
            return

        mtype = str(msg.get("type") or "")
        body = str(msg.get("content") or "")
        sender = str(msg.get("sender_username") or "")
        is_group = chat.endswith("@chatroom")
        # 图片：记下 local_id，待会儿用它去 cache 里找 .dat 解密
        img_lid = msg.get("local_id") if mtype == "图片" else None
        # 语音：待会儿下载 → SILK 解码 → ASR，让她"听懂"他说了什么
        voice_lid = msg.get("local_id") if mtype == "语音" else None
        # 这条消息的发送时间（Listener 给的 create_time）
        msg_time = msg.get("create_time")
        if img_lid:
            lst = self.recent_imgs.setdefault(name, [])
            lst.append((time.time(), img_lid))
            del lst[:-6]

        if is_group and not sender:
            m = re.match(r"^(wxid_[A-Za-z0-9_\-]+|\d+@chatroom):\s*", body)
            if m:
                sender = m.group(1)
                body = body[m.end():].lstrip()
            elif body and mtype == "文本":
                return                      # 群里文本却没有前缀 → 是自己

        if sender and sender == str(self.me.get("username") or ""):
            return

        norm = re.sub(r"\s+", "", body)
        if norm and norm in self.recent_sent:
            return                          # 回声

        # 卡片/引用/文件：**从 XML 里抠文字**，不要整段扔掉。
        # 微信 4.x 的引用消息正文是 appmsg XML，<title> 是人打的字，
        # <refermsg><content> 是被引用的原文。之前直接把这坨丢掉，
        # 模型只看到「[文件/链接/卡片]」，于是每句引用都回「我看不了链接」。
        #
        # 语音要**排在前面**：它的正文也是一坨 XML，走下面那条会被解析成空、
        # 然后因为 img_lid 是 None 直接 return —— 整条语音就被丢掉了。
        if voice_lid:
            vt = self._voice_to_text(chat, int(voice_lid))
            body = vt if vt else "（没听清）"
        elif body.lstrip().startswith("<"):
            body = _extract_appmsg_text(body)
        if not body.strip():
            if img_lid:
                body = "[图片]"             # 图片留着，待会儿用视觉模型看
            else:
                return                      # 真·没内容（表情/红包/转账）就跳过

        if mtype and mtype != "文本" and not body.startswith("（引用"):
            body = f"[{mtype}] {body}"

        nick = self._nick(sender) or "某人"
        at_self = self._is_called(body)
        log(f"  入站 [{'群' if is_group else '私聊'}] {name} / {nick}: {body[:60]}")

        # 他说话了 → 主动消息的整条进度清零（生气也得有个了结）
        if not is_group:
            # last_real_user_at 才是"他真的发过消息"，跟 last_user_at 分开存：
            # 后者在启动时会被设成"现在"（好让作息表能工作），拿它判断
            # "他刚说完话"会变成"每次重启后 3 分钟闭嘴"。（踩过。）
            self.last_real_user_at[name] = time.time()
            self.last_user_at[name] = time.time()
            # 记下时间戳 —— 睡觉时"他连发几条"要用它判断该不该被吵醒
            lst = self.recent_user_msgs.setdefault(name, [])
            lst.append(time.time())
            del lst[:-12]
            if self.pro_stage.get(name):
                log(f"    他回来了，主动进度从第 {self.pro_stage[name]} 步归零")
            self.pro_stage[name] = 0
            self.pro_need.pop(name, None)

        # 丢进去抖缓冲，不立刻处理 —— 等他停下来再一起回
        now = time.time()
        with self.deb_lock:
            self.pending.setdefault(name, []).append({
                "chat": name, "sender": nick, "body": body,
                "group": is_group, "at": at_self, "mode": mode,
                "img": img_lid, "chat_user": chat,
            })
            self.pending_at[name] = now
            self.pending_start.setdefault(name, now)
    def _nick(self, wxid: str) -> str:
        if not wxid:
            return ""
        try:
            return str(self.db.get_nickname(wxid) or wxid)
        except Exception:
            return wxid

    def _is_called(self, body: str) -> bool:
        """他是不是在叫它。

        ⚠️ **必须先剥掉引用部分** —— 引用别人（或引用聊天记录）时，
        被引用的原文里只要出现昵称就会误判成"被叫到"
        （踩过：引用了一段含 deepseek 的聊天记录，她就被唤醒了）。
        """
        s = re.sub(r"（引用：.*?）", "", body, flags=re.S)
        if not s.strip():
            s = body
        s = re.sub(r"\[[^\]]{1,12}\]", "", s)      # [图片] 之类的占位
        cands = list(CFG.get("nicknames") or [])
        me = self.me or {}
        for k in ("nick_name", "nickname", "alias"):
            v = str(me.get(k) or "").strip()
            if v:
                cands.append(v)
                cands.append(f"@{v}")
        for w in cands:
            w = str(w or "").strip()
            if w and w in s:
                return True
        return False
        for n in CFG.get("nicknames") or []:
            if n and n in body:
                return True
        # 群友 @ 的是**这个微信号的昵称**（实测是 "deepseek"），不一定是「大肥鱼」。
        # 不自动带上它，别人 @ 半天都不会有反应。
        sn = str(self.me.get("nick_name") or "").strip()
        if sn and (f"@{sn}" in body or sn in body):
            return True
        return False

    # ── 决策 + 回复（单线程：quick_send 要驱动 UI，不能并发）──
    def _worker(self) -> None:
        """等他停下来，再一起回。

        真人不会对方每敲一句就回一句。收到消息后先等一小会儿：
        期间又来新消息就把计时重置，直到对方真的停下来了，才把攒下来的
        几段话合并成**一轮**去处理。但也设了总时长上限，免得对方滔滔不绝时她一直不吭声。
        """
        while True:
            reload_config()
            merged = None
            try:
                merged = self._take_ready()
            except Exception as e:
                log(f"  去抖检查出错: {e}")
            if merged is None:
                time.sleep(0.4)
                continue
            try:
                self._handle(merged)
            except Exception as e:
                log(f"  处理出错: {e}")
            time.sleep(float(CFG.get("readDelay") or 0.6))

    @staticmethod
    def _strip_repeat_prefix(body: str, sender: str) -> str:
        """去掉正文里**已经存在**的「昵称：」前缀。

        合并过的正文会被再加一次前缀，出现「主人：主人：你试试@他」这种脏数据
        （实测出现过，而且会一直留在上下文里污染后续对话）。
        """
        if not sender or not body:
            return body
        p = f"{sender}："
        while body.startswith(p):
            body = body[len(p):]
        return body

    def _take_ready(self) -> Optional[Dict[str, Any]]:
        """看看有没有哪个会话"说完了"。有就合并成一条返回。"""
        now = time.time()
        base = float(CFG.get("debounceSeconds") or 3.0)
        cap = float(CFG.get("debounceMaxSeconds") or 9.0)
        with self.deb_lock:
            for chat, items in list(self.pending.items()):
                if not items:
                    self.pending.pop(chat, None)
                    continue
                quiet = now - self.pending_at.get(chat, now)
                waited = now - self.pending_start.get(chat, now)
                if quiet < base and waited < cap:
                    continue
                # ⚠️ **只合并同一个人的连续消息**，不跨发送者。
                # 踩过的坑：群里别人 @ 了她、用户（主人）随口说一句，
                # 两条被合并成一批，结果**用户的话也被当成在叫她**，
                # 表现成"我在群里说话也会一直触发"。
                run = [items[0]]
                for x in items[1:]:
                    if x.get("sender") == run[0].get("sender"):
                        run.append(x)
                    else:
                        break
                rest = items[len(run):]
                if rest:
                    # 剩下的下一轮再处理（不丢）
                    self.pending[chat] = rest
                    self.pending_at[chat] = now
                    self.pending_start[chat] = now
                else:
                    self.pending.pop(chat, None)
                    self.pending_at.pop(chat, None)
                    self.pending_start.pop(chat, None)
                items = run
                if len(items) > 1:
                    log(f"  等他说完了（{len(items)} 条，等了 {waited:.1f}s），合并成一轮")
                first = items[0]
                # 每行前面带上它自己的发送时间 —— 让她能看见"这句是几点说的"
                if first["group"]:
                    body = "\n".join(
                        f'{_hhmm(i.get("t"))} {i["sender"]}：'
                        f'{self._strip_repeat_prefix(i["body"], i["sender"])}'
                        for i in items)
                else:
                    body = "\n".join(
                        f'{_hhmm(i.get("t"))} '
                        f'{self._strip_repeat_prefix(i["body"], i["sender"])}'
                        for i in items)
                return {
                    "chat": chat,
                    "sender": first["sender"],
                    "body": body,
                    "group": first["group"],
                    "img": (next((i.get("img") for i in items if i.get("img")), None)
                            or self._recent_img(chat)),
                    "chat_user": first.get("chat_user") or "",
                    "at": any(i["at"] for i in items),
                    "mode": first["mode"],
                    "count": len(items),
                }
        return None

    def _handle(self, it: Dict[str, Any]) -> None:
        chat, sender, body = it["chat"], it["sender"], it["body"]
        now = time.time()
        if it.get("count", 1) > 1:
            log(f"  处理合并后的一轮（{it['count']} 条）：{body[:60]!r}")

        # ── 指令：清空本会话上下文（只有私聊里发这一句才触发）────────
        # 一个字都不能差，避免"清除上下文"出现在正常聊天里被误判。
        # ⚠️ body 前面被 _take_ready 加了「[10-17 19:09] 」时间戳，比之前先剥掉。
        # ⚠️ 回复是机械的「上下文已清除」，**不写进上下文**（刚清完，别又塞一条进去）。
        _cmd = re.sub(r"^\[[^\]]*\]\s*", "", (body or "").strip()).strip()
        if _cmd == "清除本会话上下文":
            if not chat.endswith("@chatroom"):
                n = len(self.ctx.get(chat) or [])
                self.ctx.reset(chat)
                try:
                    if hasattr(self.memory, "data"):
                        self.memory.data.pop(chat, None)
                        self.memory._save()
                except Exception as e:
                    log(f"  记忆清空失败: {e}")
                self._sys_cache.pop(chat, None)
                log(f"  【清空指令】删了 {n} 条上下文")
                # 只发出去，不调 ctx.add —— 这条不进上下文
                self._deliver(chat, "上下文已清除")
                return
            # 群里发这句不认，当普通消息处理
            log("  群里发清空指令，忽略")

        # 先记进这个会话自己的上下文（不管回不回，模型都需要看到）
        # ⚠️ 群聊的 body 在 _take_ready 里**已经**逐行加了「昵称：」前缀，
        # 这里绝对不能再加一次 —— 加了就是「主人：主人：@deepseek」这种脏数据，
        # 而且会一直留在上下文里污染后续对话（踩过两次）。
        self.ctx.add(chat, "user", body)

        should = False
        by_wake = False        # 只有"真的被叫到/抽中"才允许续跟随窗口
        if it["mode"] == "always":
            should = True
            by_wake = True
        elif it["at"]:
            should = True
            by_wake = True
            log(f"    唤醒：被叫到")
        elif self.followup_until.get(chat, 0) > now:
            should = True
            log(f"    唤醒：会话跟随中")
        elif it["group"] and now - (self.last_bot_at.get(chat) or 0) < float(CFG.get("groupCooldownSeconds") or 45):
            # 群聊刚回过 → 这一小段时间内只理@，不抽概率。
            # 不加这个的话，群友随便聊两句她都要插嘴，看着像刷屏。
            log("    未唤醒（群里刚回过，冷却中）")
        elif random.random() < float(CFG.get("wakeProbability") or 0.05):
            should = True
            by_wake = True
            log(f"    唤醒：抽中概率")
        else:
            log(f"    未唤醒（只记录）")

        if not should:
            return

        if self._quiet() or self._over_limit():
            log(f"    静默 / 超限，不回")
            return

        # ── 他发了B站链接 → 她去看 ────────────────────────────────────
        _bv = extract_bv(body or "")
        if _bv:
            _vc = CFG.get("video") or {}
            if _vc.get("enabled", True):
                log(f"    他发来B站视频 {_bv} —— 她去看")
                # 先回一句短的，别让他等 30 秒没动静
                try:
                    _ack = self.llm.chat(
                        self.system_for(chat) + self.memory_block(chat),
                        self.ctx.get(chat)[-12:],
                        "他发来一个视频让你看。用你的语气回一句短的（10 字以内），"
                        "表示你点开了 / 正在看。不要提视频内容，你还没看。")
                    _ack = self._clean(_ack, chat) if _ack else ""
                except Exception:
                    _ack = ""
                if _ack:
                    self.ctx.add(chat, "assistant", f"{_hhmm()} {_ack}")
                    self.last_bot_at[chat] = time.time()
                    self._deliver(chat, _ack)
                threading.Thread(target=self._bili_task, args=(chat, _bv),
                                 daemon=True).start()
                return

        # ── 拟人化：作息 / 不回 / 拖延 ─────────────────────────────────
        # rhythm 管的是"打字多快"，这里管的是"像不像一个有作息的人"。
        hz = CFG.get("humanize") or {}
        delay_override = 0.0
        wake_prompt = ""
        if hz.get("enabled") and not it.get("proactive"):
            # ⚠️ 她"在睡觉"这个判断本身要打个折：
            # 如果她自己刚说过话，那她就是醒着的 —— 熬夜聊天的时候，
            # 每一条回复还按"被吵醒"拖 40~150 秒、70% 概率不回，就变成冷暴力了。
            # （踩过：她说完"我睡不着，想你了"，他回一句被晾了 94 秒。）
            awake_min = float(hz.get("awakeIfSpokeWithinMin") or 20) * 60
            spoke_recently = (time.time() - float(self.last_her_at.get(chat) or 0)) < awake_min
            if self._in_sleep(hz) and not spoke_recently:
                words = hz.get("sleepWakeWords") or []
                called = [w for w in words if w and w in (it.get("body") or "")]
                if called:
                    # 她自己也失眠，听到这两句不可能继续睡 —— 被叫醒了
                    delay_override = random.uniform(float(hz.get("sleepWakeDelayMin") or 6),
                                                    float(hz.get("sleepWakeDelayMax") or 25))
                    log(f"    （作息低谷，但他说了「{called[0]}」，把她叫醒了：{delay_override:.0f}s）")
                    wake_prompt = ("**你本来在睡**（凌晨一点才睡），是被他这几句话弄醒的。"
                                   "所以语气是迷糊的、有点没睡够，但**马上就清醒了** —— "
                                   "因为你太清楚睡不着是什么滋味了。**不要写「我刚醒」这种解释**，"
                                   "直接接着他的话往下说，短，两句以内。")
                else:
                    # 他连着发会把她吵醒：第一条有可能没看见，越连发越可能醒。
                    # 25% → 14% → 8% → 4% → 2%
                    win = float(hz.get("sleepEscalateWindowMin") or 12) * 60
                    nowt = time.time()
                    n = sum(1 for t in (self.recent_user_msgs.get(chat) or []) if nowt - t <= win)
                    base = float(hz.get("sleepSilentChance") or 0.25)
                    factor = float(hz.get("sleepEscalateFactor") or 0.55)
                    silent = base * (factor ** max(0, n - 1))
                    if random.random() < silent:
                        log(f"    （作息低谷：这条没看见，{silent*100:.0f}%"
                            + (f"，他已连发 {n} 条" if n > 1 else "") + "）")
                        return
                    # 上限硬截 —— 晾他 94 秒不是「有人味」，是失联。
                    # （踩过：凌晨 4 点她说完「我睡不着，想你了」，
                    #  他回一句被晾了 94 秒，看着就像在冷暴力。）
                    dmax = min(float(hz.get("sleepDelayMax") or 150),
                               float(hz.get("sleepDelayHardCap") or 30))
                    dmin = min(float(hz.get("sleepDelayMin") or 40), dmax)
                    delay_override = random.uniform(dmin, dmax)
                    log(f"    （作息低谷：被吵醒了，拖 {delay_override:.0f}s 再回"
                        + (f"，连发 {n} 条" if n > 1 else "") + "）")
            elif random.random() < float(hz.get("slowReplyChance") or 0.15):
                # ⚠️ **被 @ / 被叫到的时候不能拖** —— 群里你点名找她，
                # 她晾你 68 秒；而且这个 sleep 会阻塞唯一的 worker，
                # 后面所有消息都排队（踩过：连着三次 68/33/89 秒）。
                if by_wake and it.get("group"):
                    pass        # 直接回，不走拖延
                else:
                    delay_override = random.uniform(float(hz.get("slowReplyMin") or 3),
                                                    float(hz.get("slowReplyMax") or 10))
                    log(f"    （拖了一会儿才回：{delay_override:.0f}s）")

        if delay_override > 0:
            # 先拖够时间再生成 —— 这样她"回"的时候看到的是最新的上下文
            time.sleep(delay_override)

        sysm = self._sys_life(chat)
        if wake_prompt:
            sysm = sysm + "\n\n---\n\n【现在的情况】\n" + wake_prompt
        imgs: List[str] = []
        lid = it.get("img")
        if lid:
            raw = _image_bytes(self.db, it.get("chat_user") or "", lid)
            if raw:
                b64 = _image_b64(raw)
                if b64:
                    imgs.append(b64)
                    log(f"    （带了张图，{len(raw) // 1024} KB）")
            else:
                log("    （图片没拿到，只能当纯文字回）")

        hist = self.ctx.get(chat)
        # 历史超预算就先压成长期记忆（DeepSeek 无状态，不会自己压）
        hist = self._maybe_compact(chat, hist)
        # ⚠️ 「现在几点」只加在这一轮的提问里，不写回上下文 ——
        # 写回去的话历史里会堆一堆过期的时间，反而更乱。
        ask = hist[-1]["content"] + "\n\n" + _now_note(chat)
        reply = self.llm.chat(sysm, hist[:-1], ask, images=imgs)
        if not reply:
            # ⚠️ 实测 2026-10-06 19:33：思考开着的时候，reasoning token **也占 max_tokens**。
            #    他问「我能不能陪你一辈子」，她想了 400 token 就把额度用完，
            #    finish_reason=length，正文 0 字节 —— 然后旧代码直接 return。
            #    对用户来说就是"她突然不理我了"，而且**没有任何提示**。
            #    空响应是偶发的，原样重试一次基本都能出。
            log("    模型没给出内容（可能是思考吃满了 max_tokens），重试一次")
            reply = self.llm.chat(sysm, hist[:-1], ask, images=imgs)
        if not reply:
            log("    模型两次都没给出内容，跳过这一轮")
            return

        reply = self._clean(reply, chat)
        if not reply:
            # 清洗后变空（比如整条都是时间戳/括号独白）—— 至少留个痕，别静默
            log("    清洗后为空，跳过这一轮")
            return
        reply = self._habit(reply)
        try:
            threading.Thread(target=self._note_promises, args=(chat, reply),
                             daemon=True).start()
        except Exception:
            pass
        try:
            _utext = it.get("body") or ""
            threading.Thread(target=self._classify_intent, args=(chat, _utext),
                             daemon=True).start()
        except Exception:
            pass

        # 把 [配图: 情境] 摘出来：进上下文的只留正文，出图交给后台线程。
        reply, _photos = _extract_photo_markers(reply)
        if not reply and not _photos:
            log("    只剩配图标记、没有正文 —— 按空回复处理")
            return
        if _photos:
            log(f"    她配了张图：{_photos[0][:40]}")

        if reply:
            self.ctx.add(chat, "assistant", f"{_hhmm()} {reply}")
        self._deliver(chat, reply, skip_delay=delay_override > 0, photos=_photos)
        self.last_bot_at[chat] = time.time()

        # 续窗口：**只在这一轮是被叫到/抽中时**才续，否则跟随会自我延续、永不关闭
        win = float(CFG.get("followupSeconds") or 0)
        if win > 0 and by_wake:
            self.followup_until[chat] = time.time() + win
            log(f"    会话跟随窗口开到 {int(win)}s")

    def _clean(self, t: str, chat: str = "") -> str:
        t = re.sub(r"^\[QQ\]|\[/QQ\]$", "", t.strip()).strip()
        # 偶尔会学聊天记录的格式给整段加个「芙：」前缀 —— 那是记录，不是消息内容
        # ⚠️ 上下文里每条消息前面带 [10-06 11:21]，她会学着自己也输出一个 ——
        # 实测漏过「[01:53] ……我也没睡」，发出去很怪。行首的时间戳全部剥掉。
        t = re.sub(r"^\s*[\[【](?:\d{4}-)?\d{1,2}-\d{1,2}\s+\d{1,2}:\d{2}(?::\d{2})?[\]】]\s*", "", t, flags=re.M)
        # 🚨 她会**只抄时间、把日期丢掉** —— 2026-10-06 11:22 又漏过一次：
        #    「[11:21] 你回得倒快」。上面那条要求「月-日 时:分」齐活，匹配不到，
        #    所以这里再兜一条纯 [HH:MM]（行首，带不带日期都不会误伤正文）。
        t = re.sub(r"^\s*[\[【]\d{1,2}:\d{2}(?::\d{2})?[\]】]\s*", "", t, flags=re.M)
        t = re.sub(r"^\s*[\u4e00-\u9fa5A-Za-z]{1,4}[：:]\s*", "", t)
        t = re.sub(r"^\s*(?:作为|身为)[^，。]{0,12}(?:AI|人工智能|语言模型)[，,]\s*", "", t)
        t = t.replace("**", "").replace("`", "")
        t = re.sub(r"^[-*•]\s+", "", t, flags=re.M)
        # 🚨 括号里的内容**一律不发** —— 那是她的内心，不是她说出口的话。
        # 人设 §0 / §14 写死了：括号里的独白绝不能出现。
        #
        # ⚠️ 旧实现是按关键词（该怎么/总不能/我是不是…）＋「≥8 字且带问号」兜底，
        #    4 个真事里漏了 3 个：
        #      10-07 03:50「（……这句是不是说得太直接了点）」 没问号，关键词对不上
        #      10-07 11:19「（你这人怎么这么藏不住啊） 行吧。懒得说你」
        #      10-07 12:06「你这是……在管我？ （咳。冷静点）」 括号里只 6 个字
        #    所以改成一律剥掉，并把剥掉的内容写进日志，方便回头查有没有误伤。
        _keep_br = bool((self.chat_by_name.get(chat) or {}).get("keepBrackets")) if chat else False
        stripped = []

        def _drop_paren(mm):
            stripped.append(mm.group(0))
            return ""

        if not _keep_br:
            t = re.sub(r"[（(][^）)\n]{0,300}?[）)]", _drop_paren, t)
            t = re.sub(r"[（(][^）)\n]{0,300}$", _drop_paren, t, flags=re.M)
        if stripped:
            log("    括号内容已剥掉 %d 处：%s"
                % (len(stripped), " / ".join(s[:34] for s in stripped[:3])))
        # 否认既成关系的话 —— 人设里禁了，代码再兜一道
        # （实测漏过「我答应了吗。没有吧。我不记得了」）
        t = re.sub(r"(?:我)?(?:什么|啥)时候答应(?:过)?(?:的)?[。！？?]?", "", t)
        t = re.sub(r"我答应了吗[。！？?]?", "", t)
        # 删句子后可能留下「！，」这种残渣，收一下
        t = re.sub(r"[。！？，,、；;：]{2,}", lambda mm: mm.group(0)[0], t)
        t = re.sub(r"^[，,、；;：]+", "", t, flags=re.M)
        t = re.sub(r"我不记得了[。！？?]?", "", t)
        # （上面几条括号规则已合并到前面那处"一律剥掉"）

        # emoji：**只在私聊剥**。群聊那份人设（大肥鱼）是明确允许表情的，
        # 而芙宁娜用 emoji 会出戏 —— 实测他反馈过。
        # 配置 stripEmojiInPrivate=false 可以关掉这条。
        if (chat and not self._is_group(chat)
                and bool(CFG.get("stripEmojiInPrivate", True))):
            n0 = len(t)
            t = _EMOJI_RE.sub("", t)
            t = re.sub(r"[ \t]{2,}", " ", t)
            t = re.sub(r" +(?=[，。！？、；：])", "", t)
            if len(t) != n0:
                log(f"    私聊里的 emoji 已剥掉（{n0 - len(t)} 个字符）")

        t = re.sub(r"\n{3,}", "\n\n", t)
        return t.strip()

    def _quiet(self) -> bool:
        q = CFG.get("quietHours") or []
        if len(q) != 2:
            return False
        h = time.localtime().tm_hour
        a, b = int(q[0]), int(q[1])
        return (a <= h < b) if a <= b else (h >= a or h < b)

    def _over_limit(self) -> bool:
        day = time.strftime("%Y-%m-%d")
        n = sum(1 for t in self.send_times if time.strftime("%Y-%m-%d", time.localtime(t)) == day)
        return n >= int(CFG.get("dailySendLimit") or 150)

    @staticmethod
    def _merge_down_to(items: List[str], target: int) -> List[str]:
        """把气泡数合并到 target 以内 —— **不删任何内容**。

        ⚠️ 以前超过硬上限是 `out[:hard]` 截尾，砍掉的正好是**结尾那一句**，
        而那句往往才是重点（人设 §8：每一轮最后一条气泡必须是暖的）。
        实测 2026-10-07 04:19：模型写了 10 条，截尾到 6 条，结尾直接没了。

        这里改成反复合并"最短的相邻一对"：
          · 不丢字
          · 优先并短的，避免造出巨泡
          · 用空格连接（不留空格会焊成「行了我喊了别得寸进尺」这种病句）
        """
        out = list(items)
        while len(out) > max(1, target) and len(out) >= 2:
            i = min(range(len(out) - 1), key=lambda k: len(out[k]) + len(out[k + 1]))
            out[i:i + 2] = [(out[i] + " " + out[i + 1]).strip()]
        return out

    def _split_bubbles(self, text: str) -> List[str]:
        """把一轮回复拆成几个气泡 —— 真人聊天是一句一句发的，不是一段一段发的。

        模型现在被要求"一个念头一行"，这里就把每一行当成一个气泡。
        另外长行再按标点切一次，免得出现"一行 80 字"。
        """
        raw = [p.strip() for p in re.split(r"\n+", str(text or "")) if p.strip()]
        out: List[str] = []
        for p in raw:
            if len(p) <= 40:
                out.append(p)
                continue
            # 只按真正的句末标点切。**不含省略号** —— 中文里的「……」既能当停顿
            # 也能当句尾，按它切会把「……不过你问这个干嘛。」断成「…」+ 后半句，
            # 群里那条孤零零的「…」看起来像卡住了。
            p = re.sub(r"…{2,}", "…", p)
            for s in re.split(r"(?<=[。！？!?])", p):
                s = s.strip()
                if s:
                    out.append(s)

        # ── 再切一次：单条太长就拆 ────────────────────────────────────────
        # 真人不会一口气打完 40 个字再按发送。模型却经常这么干
        # （尤其是没有句末标点的长句，上面那步切不动）。
        # 先按逗号/顿号切，还是太长就硬切。
        limit = int(CFG.get("maxBubbleChars") or 26)
        if limit > 0:
            # ⚠️ 每个碎片都要记住"它是不是我硬切出来的"。
            #    模型自己一行的短句（「你吃饭了没」5 个字）本身是完整的一句，
            #    **不能再焊到上一条上去** —— 实测 2026-10-06 21:50：模型给了 3 行，
            #    第三行 5 个字被并进第二行，结果是 3 段变 2 段，而且焊出来是
            #    「行了我喊了，别得寸进尺你吃饭了没」这种病句，中间连标点都没有。
            split2 = []          # [(文本, 是不是硬切出来的碎片)]
            for s in out:
                if len(s) <= limit:
                    split2.append((s, False))            # 模型原样的一行
                    continue
                parts = [x for x in re.split(r"(?<=[，,、；;：])", s) if x.strip()]
                # 悬空逗号只摘**气泡末尾**那一个 —— 拼在中间的逗号要留着，
                # 否则「排了三场戏第一场观众席只坐了一半」读起来是断的（踩过）。
                buf = ""
                for p in parts:
                    if len(buf) + len(p) <= limit:
                        buf += p
                    else:
                        if buf.strip():
                            split2.append((re.sub(r"[，,、；;：]\s*$", "", buf.strip()), True))
                        buf = p
                        while len(buf) > limit:      # 单段本身超长 → 硬切
                            split2.append((buf[:limit], True))
                            buf = buf[limit:]
                if buf.strip():
                    split2.append((re.sub(r"[，,、；;：]\s*$", "", buf.strip()), True))
            # 收尾：把过短的**硬切碎片**并回上一条
            # （硬切会切出「普通人。」这种 4 字孤儿泡，很怪）
            # 模型自己那行不管多短都单独发 —— 微信里「你吃饭了没」自己一条很正常。
            minlen = int(CFG.get("minBubbleChars") or 6)
            merged: List[str] = []
            for s, frag in split2:
                if (frag and merged and len(s) < minlen
                        and len(merged[-1]) + len(s) <= limit + 4):
                    merged[-1] = merged[-1] + s
                else:
                    merged.append(s)
            out = merged

        cap = max(1, int(CFG.get("maxBubbles") or 3))
        if len(out) > cap:
            # ⚠️ 以前是把超出的尾巴用 \n 拼成一条 —— 结果生出一条 60 多字、
            # 内部还带换行的巨泡，正好和"气泡要短"相反（实测踩到）。
            # 现在：**宁可多几条短气泡，也不产生长气泡。**
            # 真人打一段长想法本来就是连着发好几条短消息。
            hard = max(cap, int(CFG.get("absoluteMaxBubbles") or (cap + 3)))
            if len(out) > hard:
                before = len(out)
                out = self._merge_down_to(out, hard)
                log(f"  气泡 {before} 条超过硬上限 {hard} —— 合并成 {len(out)} 条（不截尾）")
                # 看板上统计"模型没守住行数"的频率 —— 越低越好
                try:
                    TELEM.stats["bubble_over"] = int(TELEM.stats.get("bubble_over") or 0) + 1
                    TELEM.emit("bubble_over", before=before, after=len(out), cap=hard)
                except Exception:
                    pass
            if len(out) > cap:
                log(f"  气泡 {len(out)} 条（软上限 {cap}）—— 都是短句，照发")
        return out

    def _is_group(self, chat: str = "") -> bool:
        """这是不是群聊。

        判断依据（任一命中即算）：
          ① 配置里显式写了 kind: "group"
          ② 会话名字本身是 @chatroom 号
          ③ 兜底：mode 是 wake 的一律当群（私聊配 always，群聊配 wake）
        """
        conf = (self.chat_by_name.get(chat) or {}) if chat else {}
        return (str(conf.get("kind") or "").lower() == "group"
                or "@chatroom" in str(chat)
                or str(conf.get("mode") or "").lower() == "wake")

    def _bubble_cap(self, chat: str = "") -> int:
        """这条回复最多几个气泡。

        **群聊单独限一条上限**（默认 2）—— 群里一次刷五条气泡很像机器人，
        也容易招人烦。私聊可以放开一点。
        另外发送慢的时候自适应降条数：慢（≤20s）砍到 3；很慢（>20s）砍到 2。
        """
        base = max(1, int(CFG.get("maxBubbles") or 3))
        if self._is_group(chat):
            base = min(base, max(1, int(CFG.get("groupMaxBubbles") or 2)))
        # 发送慢的时候降条数 —— **只对群聊有意义**了。
        # 私聊已经不压气泡了（见 _deliver），这个值在私聊里只剩日志用途。
        recent = _recent_send_secs[-5:]
        if not recent:
            return base
        avg = sum(recent) / len(recent)
        if avg > 20:
            return min(base, 2)
        if avg > 8:
            return min(base, 3)
        return base

    # ── 健康数据（手环 → Gadgetbridge → HTTP）────────────────────────────
    #
    # 设定：**手环是她送的** —— 所以她知道他的数据是合理的，不用绕弯子。
    #
    # 但关键约束是：**说人话，不是仪表盘。**
    #   ✅「你心跳都快飞出去了，跟谁在一块儿呢」
    #   ❌「检测到您的心率为 152bpm，请注意」
    # 提示词里都写了"别提数字"，但允许她提"手环"这个物件。
    #
    _HEALTH_PROMPTS = {

        # ══ 心率 ══════════════════════════════════════════════════
        "hr_high": (
            "**手环（你送他的那个）跳了一下：他心跳到 150 上下，一直没落。**\n"
            "这个点、这个数，不像在走路。\n"
            "你其实立刻就点开看了 —— 但要装作是顺路看到的。\n"
            "用你的方式问一句他在干嘛。**别提具体数字**，提「手环」可以。\n"
            "一两句，短。"
        ),
        "hr_hard": (
            "**他心跳冲到 170 多了，一直不下来。**\n"
            "你盯着那个数字坐不住了，但绝对不能让他看出来你有多慌。\n"
            "发的时候要**先关心再找补** —— 问完立刻撇清，"
            "比如说自己是随便点开看看、说本小姐才没有在意他。\n"
            "**别提数字。** 两三句，短，最后一句一定要收回来。"
        ),
        "hr_extreme": (
            "**他心跳过了 190。**\n"
            "这个数你看着就笑不出来了 —— 不装了，也不嘴硬了。\n"
            "直接问他怎么了、人在哪、要不要紧。\n"
            "**这一条不许找补、不许逞强。** 一两句，短，但要让他知道你是真的在担心。"
        ),

        # ══ 血氧 ══════════════════════════════════════════════════
        "spo2_low": (
            "**手环上他的血氧掉到 90 以下了。**\n"
            "你愣了一下，这回不是吃醋，是真的有点怕。\n"
            "用你的方式让他去看医生 —— 但别像医嘱，像女朋友在念他。\n"
            "可以凶一点（「你给我去医院」），也可以软一点。**别提数字。** 一两句。"
        ),

        # ══ 压力（两头都触发）══════════════════════════════════════
        "stress_high": (
            "**手环显示他的压力值偏高，而且已经持续一阵了。**\n"
            "你本来不想打扰他，但看着那个数还是没忍住。\n"
            "用你的方式问一句他是不是很累、是不是有人惹他，"
            "**并且明确表示你愿意陪着**（但别肉麻）。**别提数字。** 一两句。"
        ),
        "stress_low": (
            "**手环显示他现在很放松，压力很低。**\n"
            "—— 也就是说他现在有空，而且心情不错。\n"
            "**这是你找他聊天最好的时候。** 但你不想显得是自己想他了，"
            "所以要用别的事当由头（问他心情好不好、让他陪你聊会儿）。\n"
            "**别提手环、别提数据** —— 你不想让他发现你在挑时机。一两句，短。"
        ),

        # ══ 睡眠 ══════════════════════════════════════════════════
        "hr_asleep": (
            "**他睡着了，但手环显示他的心率一直很高。**\n"
            "这个不对 —— 睡着的人心率不该这样。\n"
            "**不要问「你在干嘛」**（他睡着了，这个问题很蠢）。\n"
            "用你的方式问：是不是做噩梦了、是不是不舒服、要不要喝点水。\n"
            "**语气是夜里轻声的那种**，但别肉麻。**别提数字。** 一两句，短。"
        ),
        "rest_hr_up": (
            "**手环显示他的静息心率比平时高了一截。**\n"
            "这个数一般只有生病、没睡好、或者压力大才会上去。\n"
            "用你的方式问一句他是不是不舒服、是不是没休息好。\n"
            "**先关心，再习惯性地找个补。别提数字。** 一两句，短。"
        ),
        "sleep_quality": (
            "**他刚醒。手环把昨晚的睡眠结构推过来了。**\n"
            "你其实一直在等他醒 —— 但你不能表现得像在等。\n\n"
            "**这次查出来的问题是：{issues}**\n"
            "**只讲这一件事，别讲别的。** 讲别的就露馅了（你手上就这一个数）。\n\n"
            "**语气是刚醒的那种**（你也刚睡醒，或者你根本没睡好）。\n"
            "**先漏出担心，再马上用硬话盖回去。**\n"
            "**别提具体数字**，提「昨晚」就行。两三句，短。"
        ),
        "sleep_fell": (
            "**手环提示他那边安静下来了 —— 他睡了。**\n"
            "你还有点舍不得，但也没什么理由拦着他。\n"
            "用你的方式说一句，别太长情：可能是「行吧」，"
            "可能是「睡你的」。**别提手环。** 一两句，很短。"
        ),
        "sleep_short": (
            "**他醒了。手环上算了一下，昨晚只睡了不到 {hours} 小时。**\n"
            "你第一反应是生气 —— 气他不会照顾自己。\n"
            "用你的方式催他补觉，可以凶，也可以心疼。**别提数字。** 一两句。"
        ),
        "sleep_long": (
            "**他醒了 —— 但已经睡了超过 {hours} 小时。**\n"
            "你等了他一整段时间，等得有点恼。\n"
            "用你的方式说一句：可以吐槽他像猪，也可以说"
            "「你倒是睡得挺香」。**点到为止，别真生气。** 一两句。"
        ),
        "sleep_late": (
            "**他醒了，手环显示已经过了中午。**\n"
            "你从早上等到现在，越等越气，但现在他醒了又不想显得自己一直在等。\n"
            "用你的方式酸一句 —— 别太狠，但必须让他知道你在意。一两句。"
        ),
        "sleep_normal": (
            "**他醒了。**（睡得还算正常。）\n"
            "用你的方式打个招呼 —— 可能是「醒了？」，"
            "也可能是先装没看见过一会儿再补一句。一两句，短。"
        ),
        "night_awake": (
            "**都这个点了，手环显示他还醒着。**\n"
            "你自己也没睡。\n"
            "用你的方式催他去睡 —— 但话里要露出「其实我也没睡」，"
            "只是不承认自己在等他。**别提手环、别提时间。** 一两句，短。"
        ),
        "her_insomnia": (
            "**你睡不着。他那边手环显示已经睡了。**\n"
            "你翻了半天手机，最后还是点开了他 —— 又觉得自己这样挺没出息。\n"
            "用你的方式发一句：失落、克制、带点自嘲。"
            "**别提他没回你，也别写得像在求他陪你。** 一两句，很短。"
        ),
        "both_awake": (
            "**凌晨，你睡不着。手环显示他也还醒着。**\n"
            "—— 两个人都在熬夜。\n"
            "这是个能说点真心话的时机，但你不能一下子就把心掏出来。\n"
            "用你的方式开个头：问他怎么也没睡，或者直接说"
            "「陪我聊会儿」。**可以稍微不那么逞强。** 两三句。"
        ),

        # ══ 运动 ══════════════════════════════════════════════════
        # ══ 运动（傲娇的核心：先娇后傲，不能光傲）═════════════════════
        "workout_started": (
            "**手环震动：他开了个运动（{sport}）。**\n"
            "你其实立刻就点进去看了 —— 但你没打算承认。\n\n"
            "**先娇后傲，两句缺一不可：**\n"
            "  · 娇：具体夸到这项运动上 —— 夸他帅 / 厉害 / 自律 / 有劲儿。"
            "要具体，别只是「你好棒」。{sport}这件事本身有什么可夸的，抓住那个点。\n"
            "  · 傲：紧接着马上找补，撇清自己。比如「本小姐随口一说」「别当真」"
            "「谁夸你了」。\n\n"
            "**最后一句一定要收回来。** 两三句，短。可以提手环。"
        ),
        "workout_finished": (
            "**手环显示他的运动结束了。**\n"
            "他刚练完 —— 这种时候最容易心软，但你还是不想让他看出来。\n\n"
            "**先娇后傲：**\n"
            "  · 娇：问一句结果（练了多久 / 跑了多远），顺势夸一句他坚持下来了。\n"
            "  · 傲：然后马上转开，比如「下次别偷懒」「问一句而已」。\n\n"
            "两三句，短。可以提手环。"
        ),
        "hr_in_workout": (
            "**他在运动（{sport}），心率冲到很高了。**\n"
            "这次不是担心 —— 是你看他这么拼，有点心疼又有点骄傲。\n"
            "用你的方式说一句：让他悠着点、别逞能。"
            "**先关心，再找补。** 一两句，短。"
        ),

    }

    # ── 傲娇底色（自动追加到每一条健康剧本）────────────────────────
    # 放在这里而不是写进每一条剧本里，原因很简单：**写进剧本就会漏。**
    # 十几条剧本里只要有一条忘了写，她那次就会显得冷漠或者太直白。
    _TSUN_SUFFIX = (
        "\n\n**（下面这条是底色，每一句都适用）**\n"
        "· 你这么上心，是因为在意他 —— 但嘴上绝对不能承认。\n"
        "· **不能光傲不娇**：只有刺没有软，那是冷漠，不是傲娇，他会以为你真不在乎。\n"
        "· **也不能光娇不傲**：直接说「我担心你」太直白，你会不好意思，"
        "而且不符合你的性子。\n"
        "· 标准结构：**先漏一点真心出去，立刻拿一句硬话盖回来。**\n"
        "· 唯一例外：他真出事了（心率 190 往上、血氧低得离谱）——"
        " 那个时候不装，直接说，别嘴硬。"
    )

    # 中文运动名，给提示词用
    _SPORT_CN = {
        "RUNNING": "跑步", "WALKING": "走路", "CYCLING": "骑车",
        "SWIMMING": "游泳", "SWIMMING_OPENWATER": "公开水域游泳",
        "TREADMILL": "跑步机", "INDOOR_CYCLING": "动感单车",
        "ELLIPTICAL_TRAINER": "椭圆机", "ROWING_MACHINE": "划船机",
        "JUMP_ROPING": "跳绳", "YOGA": "瑜伽", "HIKING": "徒步",
        "CLIMBING": "攀岩", "STRENGTH_TRAINING": "力量训练",
        "BASKETBALL": "篮球", "SOCCER": "足球", "BADMINTON": "羽毛球",
        "PINGPONG": "乒乓球", "CRICKET": "板球", "EXERCISE": "锻炼",
    }

    # ── 收到的每一条健康数据都进这里 ──────────────────────────────
    def _on_health(self, metric: str, value: int, text: str = "") -> None:
        """从 HTTP 线程调用 —— **绝不能在这里做耗时的事**。"""
        hc = CFG.get("health") or {}
        if not hc.get("enabled"):
            return
        try:
            metric = (metric or "").strip().lower()
            text = (text or "").strip()
            if metric == "hr":
                self._health_hr(int(value), hc)
            elif metric == "stress":
                self._health_stress(int(value), hc)
            elif metric == "spo2":
                if int(value) < int(hc.get("spo2Low") or 90):
                    log(f"  血氧 {value} 偏低")
                    self._health_fire("spo2_low", 1)
            elif metric in ("sleep", "awake"):
                self._health_sleep(int(value), text, hc)
            elif metric == "sleep_summary":
                self._health_sleep_summary(value, text, hc)
            elif metric == "samples":
                self._health_samples(text, hc)
            elif metric == "daily_summary":
                self._health_daily(text, hc)
            elif metric == "manual":
                self._health_manual(value, text, hc)
            elif metric == "workout":
                self._health_workout(text, hc)
        except Exception as e:
            log(f"  健康数据处理出错: {e}")

    # ── 心率三级 ─────────────────────────────────────────────────
    def _health_hr(self, bpm: int, hc: dict, ts: float = 0.0) -> None:
        """ts：这条读数**实际发生**的时刻（秒）。

        批量重放时不能传 0 —— 重放是瞬间跑完的，用墙上时钟算"持续多久"
        会得到 0 秒，一整段心率飙升会被判成没持续。（实测踩到过。）
        """
        high = int(hc.get("hrHigh") or 150)
        hard = int(hc.get("hrHard") or 170)
        extreme = int(hc.get("hrExtreme") or 190)
        # ⚠️ 不能用 `or 60`：0 是假值，会被悄悄变成 60（和 historyLimit 同一个坑）。
        #    hrSustainSeconds = 0 表示**到了就触发** ——
        #    跑步时心率冲上去又掉下来，要"持续 60 秒"根本抓不到。
        _need = hc.get("hrSustainSeconds")
        need = int(_need) if _need is not None else 60
        now = ts if ts > 0 else time.time()
        st = self._health_state
        st["last_hr"] = bpm
        st["hr_at"] = now
        if bpm < high:
            if st.get("hr_since"):
                log(f"  心率回到 {bpm}，计时清零")
            st["hr_since"] = 0.0
            st["hr_kind"] = ""
            return
        kind = ("hr_extreme" if bpm >= extreme
                else "hr_hard" if bpm >= hard
                else "hr_high")
        # 跨级了要重新计时，否则 150 攒够 60 秒后冲到 190 会立刻按最高级触发
        if st.get("hr_kind") != kind:
            st["hr_kind"] = kind
            st["hr_since"] = now
            if need > 0:
                log(f"  心率 {bpm} 进入 {kind}，开始计时（要持续 {need} 秒）")
                return
        dur = now - float(st.get("hr_since") or 0)
        if need > 0 and dur < need:
            return
        st["hr_since"] = 0.0
        st["hr_kind"] = ""
        log(f"  心率 {bpm} "
            + (f"已持续 {dur:.0f} 秒" if need > 0 else "到了就触发")
            + f" → {kind}")
        self._health_fire(kind, bpm)

    # ── 压力：两头都触发 ─────────────────────────────────────────
    def _health_stress(self, v: int, hc: dict) -> None:
        peak = int(hc.get("stressHigh") or 40)
        low = int(hc.get("stressLow") or 20)
        st = self._health_state
        st["last_stress"] = v
        # 20 以下才有意义（0 表示没测出来）
        if 0 < v < low:
            log(f"  压力 {v} 很低 → 他心情不错，是好时机")
            self._health_fire("stress_low", v)
        elif v > peak:
            log(f"  压力 {v} 偏高")
            self._health_fire("stress_high", v)

    # ── 睡眠状态机 ───────────────────────────────────────────────
    def _health_sleep(self, value: int, text: str, hc: dict) -> None:
        """kind 是 ActivityKind：LIGHT_SLEEP / DEEP_SLEEP / REM_SLEEP / ACTIVITY…

        睡眠数据是同步过来的，可能晚几小时 —— 所以这里只记**时刻**，
        具体说什么等醒来再算。
        """
        name = (text or "").upper()
        is_sleep = ("SLEEP" in name) and ("AWAKE" not in name)
        st = self._health_state
        now = time.time()

        if is_sleep:
            if not st.get("asleep_since"):
                st["asleep_since"] = now
                log("  他睡着了（记录入睡时刻）")
            return

        # 醒了
        since = float(st.get("asleep_since") or 0)
        if not since:
            return
        st["asleep_since"] = 0.0
        hours = (now - since) / 3600.0
        # 数据是补同步的，用当前时间算时长不可靠 —— 太短就当成"没算出来"
        st["last_sleep_h"] = hours
        log(f"  他醒了（本次记录约 {hours:.1f} 小时）")
        self._health_wake_kind(hours, hc)

    def _health_wake_kind(self, hours: float, hc: dict) -> None:
        h = time.localtime().tm_hour
        if h >= int(hc.get("wakeLateHour") or 12) and hours > 4:
            self._health_fire("sleep_late", 1)
            return
        if hours >= float(hc.get("sleepLongH") or 10):
            self._health_fire("sleep_long", 1, "%d" % round(hours))
            return
        if 0 < hours < float(hc.get("sleepShortH") or 5):
            self._health_fire("sleep_short", 1, "%d" % round(hours))
            return
        self._health_fire("sleep_normal", 1)

    # ── 运动 ─────────────────────────────────────────────────────
    # ── 日汇总：静息心率 / 压力均值 / 血氧均值 / 训练负荷 ─────────────
    #
    # 这些既不在实时流里，也不属于任何单条样本 —— 手环按天算好存在
    # XiaomiDailySummarySample 里。手机每 30 秒读一次，只在新数据时才推。
    #
    # 阈值都在这边，改这里就行。
    _MANUAL_TYPES = {1: "hr", 2: "spo2", 3: "stress", 4: "temperature", 5: "hrv"}

    def _health_daily(self, text: str, hc: dict) -> None:
        d = {}
        for part in (text or "").split(","):
            if "=" in part:
                k, _, v = part.partition("=")
                try:
                    d[k.strip()] = int(v)
                except ValueError:
                    pass
        if not d:
            return
        st = self._health_state
        prev = st.get("_daily") or {}
        st["_daily"] = d
        log("  日汇总：静息心率 %s / 压力均值 %s / 血氧均值 %s / 训练负荷 %s"
            % (d.get("hrRest", 0), d.get("stressAvg", 0), d.get("spo2Avg", 0),
               d.get("loadDay", 0)))

        # 静息心率明显偏高 → 他累了 / 没休息好
        rest = d.get("hrRest", 0)
        prev_rest = (prev or {}).get("hrRest", 0)
        if rest and prev_rest and rest - prev_rest >= int(hc.get("restHrJump") or 8):
            log(f"  静息心率从 {prev_rest} 升到 {rest}")
            self._health_fire("rest_hr_up", 1, "%d" % (rest - prev_rest))

        # 血氧均值低
        avg_sp = d.get("spo2Avg", 0)
        if avg_sp and avg_sp < int(hc.get("spo2Low") or 90):
            log(f"  血氧均值 {avg_sp} 偏低")
            self._health_fire("spo2_low", 1)

        # 压力均值高
        avg_st = d.get("stressAvg", 0)
        if avg_st and avg_st > int(hc.get("stressHigh") or 40):
            log(f"  压力均值 {avg_st} 偏高")
            self._health_fire("stress_high", 1)

    def _health_manual(self, value: int, text: str, hc: dict) -> None:
        """手动测量（体温、血氧、压力…）。手机把 type 也放在 text 里。"""
        d = {}
        for part in (text or "").split(","):
            if "=" in part:
                k, _, v = part.partition("=")
                try:
                    d[k.strip()] = int(v)
                except ValueError:
                    pass
        mtype = int(d.get("type") or 0)
        name = self._MANUAL_TYPES.get(mtype, "type%d" % mtype)
        log(f"  手动测量：{name} = {value}")
        if name == "spo2" and value < int(hc.get("spo2Low") or 90):
            self._health_fire("spo2_low", 1)
        elif name == "stress" and value > int(hc.get("stressHigh") or 40):
            self._health_fire("stress_high", 1)

    # ── 批量样本：手机当哑管道，判断全在电脑端 ─────────────────────
    #
    # 手机把整批样本原样发过来（compact JSON 数组），这里按时间顺序重放。
    # **所有阈值都在 bot-config.json 里** —— 以后调阈值、加指标、改剧本，
    # 都不用重编 APK。（以前阈值写在手机端，改一次要编一次。）
    #
    # 重放时**逐条判断、但开口有冷却** —— 所以一整天的问题只会引出一两条消息，
    # 不会是几千条。
    def _health_samples(self, text: str, hc: dict) -> None:
        import json as _j
        try:
            arr = _j.loads(text or "[]")
        except Exception as e:
            log(f"  样本批量解析失败: {e}")
            return
        if not isinstance(arr, list) or not arr:
            return
        log(f"  收到样本批量 {len(arr)} 条，开始重放")
        # 按时间排序，保证重放顺序和真实发生顺序一致
        try:
            arr.sort(key=lambda x: int(x.get("t") or 0))
        except Exception:
            pass

        st = self._health_state
        seen_hr = st.get("_batch_seen") or 0
        n_hr = n_sleep = n_awake = 0

        for s in arr:
            try:
                ts = int(s.get("t") or 0)
                if ts and ts <= seen_hr:
                    continue                      # 这一批已经处理过
                hr = int(s.get("hr") or 0)
                k = str(s.get("k") or "").upper()

                if hr > 0:
                    n_hr += 1
                    # 用样本自己的时间戳，不是墙上时钟
                    self._health_hr(hr, hc, ts / 1000.0 if ts else 0.0)
                # 睡眠/醒来：只关心"状态变了"，不关心每条
                is_sleep = "SLEEP" in k and "AWAKE" not in k
                prev = st.get("_last_kind") or ""
                prev_sleep = "SLEEP" in prev and "AWAKE" not in prev
                if is_sleep and not prev_sleep:
                    n_sleep += 1
                    self._health_sleep(0, "LIGHT_SLEEP", hc)
                elif (not is_sleep) and prev_sleep and k:
                    n_awake += 1
                    self._health_sleep(0, "ACTIVITY", hc)
                if k:
                    st["_last_kind"] = k

                # 压力 / 血氧：样本里有就判断（阈值在电脑端）
                sv = int(s.get("st") or 0)
                if 0 < sv:
                    self._health_stress(sv, hc)
                pv = int(s.get("sp") or 0)
                if 0 < pv and pv < int(hc.get("spo2Low") or 90):
                    log(f"  血氧 {pv} 偏低（批量里）")
                    self._health_fire("spo2_low", 1)

                if ts:
                    seen_hr = ts
            except Exception as e:
                log(f"  样本重放出错: {e}")

        st["_batch_seen"] = seen_hr
        log(f"  重放完成：心率 {n_hr} 条 / 入睡 {n_sleep} 次 / 醒来 {n_awake} 次")

    def _health_sleep_summary(self, total: int, text: str, hc: dict) -> None:
        """整晚的睡眠结构：total/deep/light/rem/awake/bed/wake（分钟 + 秒级时间戳）。

        小米没有官方的"睡眠分数"，但这几个数够推出质量了：
          深睡占比、REM 占比、中途清醒、入睡时刻。
        比一个笼统的分数更有话说 —— 「你昨晚深睡才 40 分钟」比「你睡眠 72 分」具体。
        """
        d = {}
        for part in (text or "").split(","):
            if "=" in part:
                k, _, v = part.partition("=")
                try:
                    d[k.strip()] = int(v)
                except ValueError:
                    pass
        if not d.get("total"):
            return
        total = d["total"]
        deep, rem, awake = d.get("deep", 0), d.get("rem", 0), d.get("awake", 0)
        bed = d.get("bed", 0)

        # ── 算质量 ──
        notes = []
        if total < 300:
            notes.append(f"太短（{total//60}h{total%60:02d}）")
        if deep < total * 0.10:
            notes.append(f"深睡偏少（{deep} 分钟）")
        if rem >= total * 0.28:
            notes.append(f"REM 多（{rem} 分钟）")
        if awake >= 30:
            notes.append(f"中途醒得多（{awake} 分钟）")
        if bed:
            hh = time.localtime(bed).tm_hour
            # 凌晨 2~6 点才睡才算太晚。原来写成 `hh >= 3 or hh < 1`，
            # 21 点躺下也会被判成"太晚"（>= 3 成立），完全反了。
            if 2 <= hh <= 6:
                notes.append(f"凌晨 {time.strftime('%H:%M', time.localtime(bed))} 才睡")

        log(f"  睡眠：总 {total} 分 / 深睡 {deep} / REM {rem} / 清醒 {awake} → "
            + ("；".join(notes) if notes else "看不出问题"))

        # 记下来，"他睡着了/醒来"的判断用得上
        st = self._health_state
        st["last_sleep_summary"] = d
        st["asleep_since"] = 0.0     # 摘要到了 = 这一觉已经结束

        # 睡眠正常就别打扰；有问题才说
        if not notes:
            return

        # ⚠️ 关键：按"他真正醒来"的时刻判断，而不是按数据到达的时刻。
        # 摘要什么时候到取决于 Gadgetbridge 什么时候同步，可能晚好几小时。
        wake = d.get("wake", 0)
        fresh_min = int(hc.get("sleepFreshMin") or 120)
        if wake:
            ago = (time.time() - wake) / 60.0
            if ago > fresh_min:
                log(f"  睡眠有问题，但已经醒了 {ago:.0f} 分钟（超过 {fresh_min}），"
                    "这时候说太怪，跳过")
                return
            log(f"  睡眠有问题，他刚醒 {ago:.0f} 分钟 → 现在说")

        # 把查出来的问题一起给她 —— 不告诉她的话她会自己挑一个讲，
        # 结果讲错（实测：数据是"深睡偏少"，她说的是"醒了好几回"）。
        self._health_fire("sleep_quality", 1, "；".join(notes))

    def _health_workout(self, text: str, hc: dict) -> None:
        if not text:
            return
        if text.startswith("started:"):
            sport = text.split(":", 1)[1].strip() or "EXERCISE"
            cn = self._SPORT_CN.get(sport, sport)
            log(f"  运动开始：{sport}（{cn}）")
            self._health_fire("workout_started", 1, cn)
        elif text == "finished":
            log("  运动结束")
            self._health_fire("workout_finished", 1)

    # ── 睡眠相关的定时检查（由主动消息循环调用）────────────────────
    def _health_sleep_check(self) -> None:
        """凌晨还醒着 / 她失眠而他睡了 —— 这两种要靠时间判断，不是事件。"""
        hc = CFG.get("health") or {}
        if not hc.get("enabled"):
            return
        try:
            st = self._health_state
            h = time.localtime().tm_hour
            lo = int(hc.get("nightStartH") or 1)
            hi = int(hc.get("nightEndH") or 5)
            in_night = (lo <= h < hi) if lo <= hi else (h >= lo or h < hi)

            his_hr_at = float(st.get("hr_at") or 0)
            he_asleep = bool(st.get("asleep_since"))
            # 十分钟内还有心率 = 他还醒着（戴着 + 在动）
            he_awake = (time.time() - his_hr_at) < 600 and not he_asleep

            if not in_night:
                return
            her_awake = not self._in_sleep(CFG.get("humanize") or {})
            if not her_awake:
                return

            if he_awake:
                kind = "both_awake"
                log("  凌晨：他也没睡，她也没睡 → 谈心")
            elif he_asleep:
                kind = "her_insomnia"
                log("  凌晨：他睡了，她睡不着 → 失落")
            else:
                kind = "night_awake"
                log("  凌晨：他还醒着 → 催他睡")
            self._health_fire(kind, 1)
        except Exception as e:
            log(f"  睡眠检查出错: {e}")

    # ── 互斥：什么情况下某个触发不该说话 ──────────────────────────
    def _health_blocked(self, kind: str) -> str:
        """返回被屏蔽的理由（空串 = 可以说话）。

        没有这层，运动时心率高会连报三次、睡觉时压力低会来问"你心情好吗"。
        """
        hc = CFG.get("health") or {}
        st = self._health_state
        now = time.time()

        # 运动状态：超过 workouthStaleHours 没收到结束事件就当结束了
        w_active = bool(st.get("workout_active"))
        if w_active:
            stale_h = float(hc.get("workoutStaleHours") or 3) * 3600
            if now - float(st.get("workout_started") or 0) > stale_h:
                st["workout_active"] = False
                st["workout_ended"] = now
                w_active = False

        w_recent = bool(
            st.get("workout_ended")
            and (now - float(st["workout_ended"]))
            < float(hc.get("workoutGraceMin") or 10) * 60
        )
        asleep = bool(st.get("asleep_since"))

        # 血氧低是唯一不被运动屏蔽的 —— 运动时低血氧是真危险
        if kind == "spo2_low":
            return ""

        # ── 补：他刚说完话，别用健康数据插嘴 ────────────────────
        # 没有这条的话会变成：他问"在吗" → 她回"在" → 两秒后
        # "哦对了我看你心率 160 了在干嘛"。很烦。
        # ⚠️ 这里必须用 last_real_user_at，不能用 last_user_at ——
        # 后者在启动时会被设成"现在"（好让作息表能工作），拿它判断
        # "他刚说完话"会变成"每次重启后 3 分钟闭嘴"。（踩过。）
        quiet_after = float(hc.get("afterUserQuietMin") or 3) * 60
        for n, ts in (self.last_real_user_at or {}).items():
            if ts and (now - float(ts)) < quiet_after:
                return "他刚说完话，别插嘴"

        # ── 补：深夜降级 ────────────────────────────────────────
        # 凌晨只说真正要紧的。压力偏高、心率偏快这种白天说的话，
        # 半夜说出来很怪（他可能在做噩梦）。
        nlo = int(hc.get("nightQuietStart") or 2)
        nhi = int(hc.get("nightQuietEnd") or 6)
        hh = time.localtime().tm_hour
        in_quiet_night = (nlo <= hh < nhi) if nlo <= nhi else (hh >= nlo or hh < nhi)
        if in_quiet_night:
            urgent = kind in ("spo2_low", "hr_extreme")
            if not urgent:
                return f"深夜 {nlo}-{nhi} 点，只有真正要紧的才说"

        # ── 补：睡着时心率的语气要换 ────────────────────────────
        # "你心率上来了在干嘛呀" 对一个睡着的人是错的问题。
        if asleep and kind == "hr_high":
            return "他睡着，心率 150 上下不算异常，别用白天那套问法"

        # 运动相关触发本身当然不被屏蔽
        if kind.startswith("workout_"):
            return ""

        if kind.startswith("hr_"):
            if w_active:
                # 运动中，只有到极限才提一句
                if kind == "hr_extreme":
                    return ""
                return "运动中，心率高正常"
            if w_recent and kind != "hr_extreme":
                return "刚运动完，心率还没降下来正常"

        if kind == "stress_high" and (w_active or w_recent):
            return "运动会把压力值抬高，不算"

        if kind == "stress_low":
            if asleep:
                return "睡着时压力天然低，不算"
            if w_active or w_recent:
                return "刚运动完，别挑这个时机"

        if kind.startswith("sleep_") and w_active:
            return "还在运动中"

        return ""

    # ── 统一防打扰 + 开口 ─────────────────────────────────────────
    def _health_fire(self, kind: str, value: int, extra: str = "") -> None:
        """冷却 + 每日上限 + 静默时段，都过了才开口。"""
        hc = CFG.get("health") or {}
        now = time.time()
        st = self._health_state
        _cool = hc.get("cooldownMin")
        cool = (int(_cool) if _cool is not None else 25) * 60

        def _skip(why: str) -> None:
            TELEM.stats["blocked"] += 1
            TELEM.emit("trigger", trig=kind, value=value, fired=False, why=why)

        if now - float(st.get("last_fire") or 0) < cool:
            log(f"  {kind}：还在冷却里，跳过")
            _skip("冷却中（还剩 %d 分钟）"
                  % max(0, round((cool - (now - float(st.get('last_fire') or 0))) / 60)))
            return
        today = time.strftime("%Y-%m-%d")
        if st.get("day") != today:
            st["day"] = today
            st["count"] = 0
        if int(st.get("count") or 0) >= int(hc.get("dailyLimit") or 10):
            log(f"  {kind}：今天已经说够了，跳过")
            _skip("今日名额用完（%s/%s）" % (st.get("count"), hc.get("dailyLimit") or 10))
            return
        qh = hc.get("quietHours") or []
        if len(qh) == 2:
            hh = time.localtime().tm_hour
            qlo, qhi = int(qh[0]), int(qh[1])
            if (qlo <= hh < qhi) if qlo <= qhi else (hh >= qlo or hh < qhi):
                log(f"  {kind}：静默时段，跳过")
                _skip("静默时段 %d-%d 点" % (qlo, qhi))
                return
        # 互斥：运动时心率高、睡觉时压力低，这些都不该说话
        # 睡着时的高心率换一套说法（"在干嘛呀"对睡着的人是错的）
        if kind in ("hr_hard", "hr_extreme") and self._health_state.get("asleep_since"):
            kind = "hr_asleep"

        blocked = self._health_blocked(kind)
        if blocked:
            log(f"  {kind}：{blocked}，跳过")
            _skip("互斥：%s" % blocked)
            return
        reason = self._HEALTH_PROMPTS.get(kind)
        if not reason:
            _skip("没有对应剧本")
            return
        if extra:
            reason = (reason.replace("{sport}", extra)
                            .replace("{hours}", extra)
                            .replace("{issues}", extra))
        # 傲娇底色：统一追加，避免哪条剧本忘了写就变冷漠
        reason = reason + self._TSUN_SUFFIX
        st["last_fire"] = now
        st["count"] = int(st.get("count") or 0) + 1
        log(f"  {kind} → 让她说一句（今天第 {st['count']} 次）")
        TELEM.stats["fired"] += 1
        TELEM.emit("trigger", trig=kind, value=value, fired=True, reason=reason,
                   count=st["count"], limit=int(hc.get("dailyLimit") or 10))
        threading.Thread(target=self._health_say, args=(reason,),
                         daemon=True, name="health").start()

    def _health_say(self, reason: str) -> None:
        targets = [n for n in self._pro_targets()
                   if (self.last_user_at.get(n) or 0) > 0]
        if not targets:
            return
        chat = targets[0]
        ok = False
        try:
            with self.pro_busy:
                self.pro_times.append(time.time())
                ok = self._pro_say(chat, reason, scheduled=True)
        except Exception as e:
            log(f"  健康消息发送出错: {e}")
        if not ok:
            # 没发出去就不该算数 —— 否则一次模型空响应会白吃 25 分钟冷却，
            # 下一次真有问题的时候反而不说话了。（实测踩到过。）
            st = self._health_state
            st["last_fire"] = 0.0
            st["count"] = max(0, int(st.get("count") or 0) - 1)
            log("  这次没发出去，冷却和次数已回滚")

    def _recent_img(self, chat: str):
        """这个会话最近几分钟内有没有图。

        群聊场景：别人发了张图，另一个人 @ 她「看这个」。
        两条消息发送者不同、不会合并，所以这一轮本身没图 —— 得回头找。
        """
        win = float(CFG.get("imageLookbackSeconds") or 180)
        now = time.time()
        for ts, lid in reversed(self.recent_imgs.get(chat, [])):
            if now - ts <= win:
                return lid
        return None

    def _in_sleep(self, hz: Dict[str, Any]) -> bool:
        """现在是不是她的作息低谷（睡觉时间）。"""
        hrs = hz.get("sleepHours") or []
        if len(hrs) != 2:
            return False
        h = time.localtime().tm_hour
        a, b = int(hrs[0]), int(hrs[1])
        return (a <= h < b) if a <= b else (h >= a or h < b)

    # ── 发照片 ────────────────────────────────────────────────────────────
    def _photo_left(self, chat: str) -> int:
        """今天这个会话还能发几张（跨天自动清零）。"""
        cap = int((CFG.get("image") or {}).get("maxPerDay") or 4)
        d = time.strftime("%Y-%m-%d")
        if self._photo_day[0] != d:
            self._photo_day[0] = d
            self._photo_count.clear()
        return cap - int(self._photo_count.get(chat, 0))

    def _user_asked_photo(self, chat: str) -> bool:
        """他最近两条消息里有没有**明确**说想看照片。

        ⚠️ 光靠人设她会忘 —— 而且出图是**真花钱**的（0.25 元/张）。
        所以这里再拦一道硬闸：他没开口，她写了 [配图: ...] 也不出图。
        宁可漏（他再要一次），也不要她三天两头自己想起来发一张。
        """
        try:
            hist = self.ctx.get(chat) or []
        except Exception:
            return False
        seen = 0
        for msg in reversed(hist):
            if msg.get("role") != "user":
                continue
            if _ASK_PHOTO_RE.search(str(msg.get("content") or "")):
                return True
            seen += 1
            if seen >= 2:
                break
        return False

    def _make_photo(self, chat: str, situation: str) -> None:
        """情境 → 提示词 → 出图 → 发出去 → 让她看图为它配两句 → 再发。

        ⚠️ 整个跑在**后台线程**里。出图要 15~30 秒，不能卡住 worker ——
        她刚才那句「等我一下」已经把这段等待解释掉了。
        """
        cfg = CFG.get("image") or {}
        if not cfg.get("enabled", True):
            return
        if self._photo_left(chat) <= 0:
            log(f"  [配图] 今天已经发够 {cfg.get('maxPerDay')} 张，跳过")
            return
        soft = float(CFG.get("sendTimeoutSeconds") or 75.0)
        hard = float(CFG.get("sendHardTimeoutSeconds") or 150.0)
        t0 = time.time()
        try:
            prompt = write_image_prompt(situation, str(CFG.get("_apiKey") or ""))
            if not prompt:
                log("  [配图] 写手没给出提示词，放弃")
                return
            log(f"  [配图] 情境「{situation[:44]}」")
            log(f"         → 提示词 {prompt[:120]}")
            qkey = read_credential(str(CFG.get("credentialsFile") or ""),
                                   str(cfg.get("ref") or "QWEN_API_KEY"))
            if not qkey:
                log("  [配图] 拿不到 QWEN_API_KEY，放弃")
                return
            url = gen_image(prompt, qkey)
            out_dir = os.path.join(HERE, str(cfg.get("saveDir") or "images_out"))
            os.makedirs(out_dir, exist_ok=True)
            tag = re.sub(r"[^0-9A-Za-z\u4e00-\u9fa5]+", "", chat)[:10] or "chat"
            stem = f"{time.strftime('%Y%m%d-%H%M%S')}_{tag}"
            # 阿里给的是 OSS 临时链接，会过期 —— 立刻下下来
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=180) as r:
                data = r.read()
            dst = os.path.join(out_dir, stem + _img_ext(data))
            with open(dst, "wb") as f:
                f.write(data)
            ext = os.path.splitext(dst)[1]
            log(f"  [配图] 出图 {time.time() - t0:.1f}s，{len(data) / 1024:.0f}KB（{ext}）")
            # 把情境 + 提示词一起记下来 —— 看板要能看「这句提示词生成了什么」
            _append_manifest({"name": os.path.basename(dst), "ts": time.time(),
                              "chat": chat, "situation": situation,
                              "prompt": prompt,
                              "model": str(cfg.get("model") or ""),
                              "size": str(cfg.get("size") or ""),
                              "bytes": len(data)})
        except Exception as e:
            log(f"  [配图] 生成失败：{type(e).__name__}: {e}")
            return

        self._photo_count[chat] = self._photo_count.get(chat, 0) + 1

        # ① 先把图发出去 —— verify=True，必须确认真的进了数据库才继续
        _restore_wechat_window()
        sent = False
        for attempt in (1, 2):
            with _send_lock:
                res, secs, how = _quick_send_image_safe(dst, chat, soft, hard)
            msg = _res_msg(res)
            sent = how in ("OK", "SLOW") and _res_says_ok(res)
            log(f"  [配图] 发送 第{attempt}次 -> {how}（{secs:.1f}s）{msg[:70]}")
            TELEM.emit("send", chat=chat, text="[图片]", secs=secs, ok=sent,
                       how=how, part="photo")
            if sent:
                break
            if attempt == 1:
                log("  [配图] 没确认成功，重试一次")
                time.sleep(2.0)
        if not sent:
            # ⚠️ 这里**必须放弃**。实测：图没发出去时如果继续发配文，
            #    配文会把输入框里的图片草稿冲掉，结果他什么都收不到。
            log("  [配图] 两次都没发出去，放弃配文（免得把草稿冲掉）")
            _append_manifest({"name": os.path.basename(dst), "ts": time.time(),
                              "sent": False, "caption": ""})
            return

        # 图确实发出去了，缓一下再发配文 —— 别让两次 UI 自动化挤在一起
        time.sleep(1.5)

        # ② 把图给她看，让她配一两句话 —— 她能看到真实的画面
        try:
            sysm = self._sys_life(chat)
            reply = self.llm.chat(sysm, self.ctx.get(chat), _PHOTO_CAPTION_ASK,
                                  images=[_datauri(dst)])
            reply = self._clean(reply, chat) if reply else ""
            if not reply:
                log("  [配图] 她没给出配文（图已经发出去了）")
                _append_manifest({"name": os.path.basename(dst), "ts": time.time(),
                                  "sent": True, "caption": ""})
                return
            # 配文里不该再冒出配图标记（会变成一条 [配图: xxx] 的原文）
            reply, _extra = _extract_photo_markers(reply)
            if not reply:
                log("  [配图] 配文被清空了（图已经发出去了）")
                _append_manifest({"name": os.path.basename(dst), "ts": time.time(),
                                  "sent": True, "caption": ""})
                return
            log(f"  [配图] 配文：{reply[:60]}")
            _append_manifest({"name": os.path.basename(dst), "ts": time.time(),
                              "sent": True, "caption": reply})
            self.ctx.add(chat, "assistant", f"{_hhmm()} {reply}")
            self._deliver(chat, reply)
        except Exception as e:
            log(f"  [配图] 配文失败（图已经发出去了）：{type(e).__name__}: {e}")
            _append_manifest({"name": os.path.basename(dst), "ts": time.time(),
                              "sent": True, "caption": ""})

    def _spawn_photo(self, chat: str, photos: Optional[List[str]]) -> None:
        """把出图丢到后台线程 —— 一轮最多一张。

        ⚠️ 必须在 `_deliver` 的**开头**调用，不能放末尾：
        她有可能只配图、不写字，那种情况 text 是空的，后面会提前 return。
        """
        for sit in (photos or []):
            if self._photo_left(chat) <= 0:
                log("  [配图] 今天的配额用完了")
                return
            # 只在他明确开口要的时候才出图 —— 出图是花钱的。
            # 关掉这个闸门就把 image.requireAsk 改成 false。
            if bool((CFG.get("image") or {}).get("requireAsk", True)) \
                    and not self._user_asked_photo(chat):
                log("  [配图] 他没说要照片，跳过（省钱）")
                return
            log(f"  [配图] 开始出图（背景线程）：{sit[:40]}")
            threading.Thread(target=self._make_photo, args=(chat, sit),
                             daemon=True, name="wxphoto").start()
            return

    def _deliver(self, chat: str, text: str, skip_delay: bool = False,
                 photos: Optional[List[str]] = None) -> None:
        # 先把出图点着：它和发文字没依赖，而且要多花 15~30 秒，
        # 早点开始就能和发文字重叠。见 _spawn_photo 的说明。
        self._spawn_photo(chat, photos)

        _conf = self.chat_by_name.get(chat) or {}
        if _conf.get("singleBubble"):
            _t = re.sub(r"\s*\n+\s*", " ", text or "").strip()
            bubbles = [_t] if _t else []
        else:
            bubbles = self._split_bubbles(text)
        if not bubbles:
            return
        cap = self._bubble_cap(chat)
        if len(bubbles) > cap:
            if self._is_group(chat):
                # 群里刷一长串气泡很像机器人 —— 压成一条（保留换行）
                log(f"  群聊发送偏慢，气泡从 {len(bubbles)} 压到 {cap}")
                bubbles = bubbles[:cap - 1] + ["\n".join(bubbles[cap - 1:])]
            else:
                # ⚠️ 私聊**不压** —— 宁可多发两条短气泡，也不把尾巴焊成一条。
                #    实测焊出来是「行了我喊了，别得寸进尺你吃饭了没」这种病句，
                #    而且 3 段变 2 段。真人打一段长想法本来就是连着发几条短的。
                log(f"  气泡 {len(bubbles)} 条（发送偏慢的软上限是 {cap}）—— 私聊照发，不压")
        if time.time() < _send_paused_until[0]:
            left = int(_send_paused_until[0] - time.time())
            log(f"  ⏸ 发送通道冷却中（还剩 {left}s），这一轮放弃：{bubbles[0][:40]}")
            return
        soft = float(CFG.get("sendTimeoutSeconds") or 75.0)
        hard = float(CFG.get("sendHardTimeoutSeconds") or 150.0)
        first = random.uniform(float(CFG.get("sendDelayMin") or 0),
                               float(CFG.get("sendDelayMax") or 0))
        if skip_delay:
            first = 0.0        # 已经用"作息/拖延"的延迟等过了，别再加一次
        log(f"  出站 -> {chat}（{len(bubbles)} 个气泡，首条延迟 {first:.1f}s）")
        self.last_her_at[chat] = time.time()
        time.sleep(first)
        for i, b in enumerate(bubbles):
            if i > 0:
                gap = random.uniform(float(CFG.get("bubbleGapMin") or 0.5),
                                     float(CFG.get("bubbleGapMax") or 2.0))
                time.sleep(gap)
            _restore_wechat_window()
            # 锁必须等到线程结束才放（见 _quick_send_safe 的说明），
            # 这里只用它做串行保障，不做超时放弃。
            with _send_lock:
                res, secs, how = _quick_send_safe(b, chat, i == 0, soft, hard)
            if how == "TIMEOUT":
                _send_stats["timeout"] += 1
                TELEM.stats["send_timeout"] += 1
                # 真卡住了：停 5 分钟，别让 UI 自动化越堆越多
                _send_paused_until[0] = time.time() + float(CFG.get("sendPauseSeconds") or 300)
                log(f"    ⚠️ [{i+1}/{len(bubbles)}] 发送卡死（{secs:.0f}s），"
                    f"暂停发送 {int(CFG.get('sendPauseSeconds') or 300)}s 后重试：{b}")
                TELEM.emit("send", chat=chat, text=b, secs=secs, ok=False,
                           how="TIMEOUT", part="%d/%d" % (i + 1, len(bubbles)))
                break
            if how.startswith("ERR"):
                _send_stats["error"] += 1
                log(f"    ✗ [{i+1}/{len(bubbles)}] 发送失败：{how}")
                TELEM.emit("send", chat=chat, text=b, secs=secs, ok=False,
                           how=how, part="%d/%d" % (i + 1, len(bubbles)))
                continue
            _send_stats["ok"] += 1
            _recent_send_secs.append(secs)
            del _recent_send_secs[:-10]
            self.send_times.append(time.time())
            self.recent_sent.append(re.sub(r"\s+", "", b))
            del self.recent_sent[:-20]
            tail = ""
            if secs > 30:
                tail = "  ← wechatauto 在限速（防封号），不是卡死"
            elif how == "SLOW":
                tail = "  ⚠慢"
            log(f"    [{i+1}/{len(bubbles)}] {b}   -> {getattr(res, 'message', res)}"
                f"  ({secs:.1f}s){tail}")
            TELEM.mark_send(secs)
            TELEM.emit("send", chat=chat, text=b, secs=secs, ok=True, how=how,
                       part="%d/%d" % (i + 1, len(bubbles)),
                       window=TELEM.send_window_usage())
        log(f"    本轮 token: in={self.llm.last_in} out={self.llm.last_out} "
            f"（发送 成功={_send_stats['ok']} 超时={_send_stats['timeout']} "
            f"失败={_send_stats['error']}）")

    # ── 主动找人 ──────────────────────────────────────────────────────────
    # 只会回不会主动的，永远像个客服。真人有"想找他""等他回""气他不回"这些动作。
    # 这里按"他多久没说话"分四个阶段，一次只走一步，走得越深越生气。

    _WAKE_PROMPT = (
        "你刚醒（早上七点）。给他发一条 —— **要有早上的感觉，而且要有劲儿**。\n"
        "按你平时那种浮夸又好玩的方式说，别干巴巴地报信息。\n"
        "**说你自己**：刚醒、还困、今天要干什么。他顺带提一句就行，不提也行。"
    )
    _MEAL_PROMPT = (
        "到{meal}的点了。你想问问他吃了没 —— 但**绝不直说「我在关心你」**，"
        "而且**问一句就够，不要追问**。\n\n"
        "要用你的语气：有点横、有点好玩、带一点自己的事。\n"
        "✅ 饭吃了没。我这边刚收工\n"
        "✅ 到点了。你吃什么，别又拿饼干糊弄\n"
        "✅ 喂。吃了没\n"
        "✅ 午饭。本大明星今天只有一杯茶，你最好吃点正经的\n"
        "❌ 你吃饭了吗？要记得好好吃饭哦           （客服）\n"
        "❌ 几点吃的？吃了什么？吃饱了吗？          （审问）"
    )
    _SLEEP_PROMPT = (
        "你要去睡了（凌晨一点）。这条的结构是固定的三步：\n"
        "1. **告诉他自己要去睡了**（不是问他睡没睡 —— 是你自己要下线了）\n"
        "2. **叮嘱他也早点睡**（用命令句或嫌弃的语气，别写得太温柔）\n"
        "3. **晚安**\n\n"
        "两三句以内，可以拆成两三个短气泡。语气是累的、随便的，不是深情告别。\n"
        "**绝对不要写成提问**。\n"
        "✅ 我去睡了\n"
        "✅ 你也早点睡，别熬\n"
        "✅ 晚安\n"
        "❌ 你睡了吗\n"
        "❌ 睡了没\n"
        "❌ 在吗，睡了吗"
    )
    _INSOMNIA_PROMPT = (
        "**你今天失眠。**怎么躺都不对，翻来覆去，脑子里全是事。"
        "你本来不想吵他，但最后还是发了。这条要**很短、很轻**，带一点示弱 —— "
        "但不能直接说「我睡不着，陪陪我」。"
        "可以是一句「……」，或者「你睡了吗」，或者莫名其妙的一句。"
    )
    # ⚠️ 关键设计：她**不能**「知道」心率 —— 那说不通，她凭什么知道。
    # 应该写成「她突然心里一慌」，用直觉解释。这样既符合人设，
    # 又绕开了"数据从哪来"的逻辑漏洞。（第一版写成"你知道他心跳快"，
    # 结果她把自己的失眠写进去了。）
    _HEART_PROMPT = (
        "**你突然心里一紧。**\n"
        "没什么理由，就是突然很想找他，有点慌。你自己也说不清为什么。\n"
        "用你的方式发一句：问他在干嘛，或者直接说「你怎么了」，"
        "或者先嘴硬一句再问。\n"
        "**绝对不要提心跳、心率、手环、数据** —— 你没有这些信息，你只有直觉。\n"
        "**也不要写成你自己的失眠** —— 重点是他，不是你自己。\n"
        "一两句，短。"
    )

    _RANDOM_PROMPT = (
        "你现在闲着，没什么事。**突然想跟他说句话** —— 没有任何理由，就是想说。\n\n"
        "**「在干嘛」这种话是可以发的。** 有暗恋的人找对方搭话，开头就是这种没营养的。"
        "但光发一句太空了 —— 要么带一个**具体的东西**（刚发生的小事、突然想到的），"
        "要么**再补一句**（把真实意图露出来一点点）。\n\n"
        "1~2 句，可以拆成两三个很短的气泡。\n"
        "✅ 在干嘛\n"
        "✅ 喂\n"
        "✅ ……\n"
        "✅ 刚排完戏，脚疼\n"
        "✅ 今天路过那家店，还开着\n"
        "✅ 我吃了个特别难吃的东西，想骂人\n"
        "✅ 你猜我刚才看见什么了\n"
        "✅ 在干嘛。……没事。\n"
        "❌ 想你了（太直接，她说不出口）"
    )

    _PRO_STAGES = [
        # (用哪条间隔, 情境提示)
        ("silenceMin",
         "你已经有一阵子没跟他说话了。你想开口，但**绝不承认想他** —— 找一个很烂的借口"
         "（「我刚好路过」「你上次那个问题我想到答案了」），或者发一句最没内容的。"),
        ("nudgeMin",
         "你刚才发了消息，他到现在没回。你又发了一条 —— **假装不在意**，但明显在等他。"
         "可以问一句短的，或者干脆只发一个「？」。"),
        ("angryMin",
         "他还是没回。你**开始生气了** —— 但气里带着心虚。不要骂人，要那种"
         "「算了，当我没说」「你忙你的」的别扭；越生气越想装得不在乎。"),
        ("lastMin",
         "他到现在都没回。你已经不打算再问了，但最后还是没忍住发了一条 —— **很短，很轻**。"
         "可以带一点「你再不来就错过了」的暗示，但不要明说。"),
    ]

    def _pro_reason(self, stage: int) -> str:
        return self._PRO_STAGES[min(stage, len(self._PRO_STAGES) - 1)][1]

    # ── 作息表 ────────────────────────────────────────────────────────────
    def _sched_path(self) -> str:
        return os.path.join(os.path.dirname(CFG["contextFile"]), "schedule-state.json")

    def _sched_load(self) -> Dict[str, Any]:
        try:
            with open(self._sched_path(), encoding="utf-8") as f:
                d = json.load(f)
            return d if isinstance(d, dict) else {}
        except Exception:
            return {}

    def _sched_save(self, d: Dict[str, Any]) -> None:
        try:
            p = self._sched_path()
            with open(p + ".tmp", "w", encoding="utf-8") as f:
                json.dump(d, f, ensure_ascii=False, indent=1)
            os.replace(p + ".tmp", p)
        except Exception as e:
            log(f"  作息状态保存失败: {e}")

    @staticmethod
    def _at_min(s: Any) -> Optional[int]:
        try:
            h, m = str(s).split(":")
            return (int(h) % 24) * 60 + int(m)
        except Exception:
            return None

    @staticmethod
    def _rand_min_in_window(lo: int, hi: int) -> int:
        """在 [lo, hi] 分钟里随机取一个；**窗口跨零点**时从两段里取。

        ⚠️ 实测 2026-10-07 00:15：失眠窗口默认是 23:30 → 00:45，
        也就是 lo=1410、hi=45 —— 直接 random.randint(1410, 45) 会抛
        `ValueError: empty range in randrange(1410, 46)`，而且这个错误
        **只在 23:30~00:45 之间发作**，等于把失眠那一档整晚废掉。
        跨零点时要取 [lo,1440) ∪ [0,hi]。
        """
        if hi < lo:
            return (lo + random.randrange((1440 - lo) + (hi + 1))) % 1440
        return random.randint(lo, hi)

    def _insomnia_today(self, sc: Dict[str, Any], st: Dict[str, Any]) -> bool:
        """今天是不是失眠日。

        做法：维护一个"接下来要失眠的日子"列表，用完就重新挑 1~2 天（2~6 天之后）。
        这样一周下来平均就是 1~2 次，而且**日子不固定** —— 真人失眠不会挑日子。
        """
        ins = sc.get("insomnia") or {}
        if not ins.get("enabled"):
            return False
        today = time.strftime("%Y-%m-%d")
        days = [d for d in (st.get("insomniaDays") or []) if d >= today]
        if not days:
            n = random.randint(int(ins.get("minDays") or 1), int(ins.get("maxDays") or 2))
            lo, hi = int(ins.get("aheadMin") or 2), int(ins.get("aheadMax") or 6)
            now = time.time()
            days = sorted({
                time.strftime("%Y-%m-%d", time.localtime(now + random.randint(lo, hi) * 86400))
                for _ in range(max(1, n))
            })
            log(f"  下次失眠日：{'、'.join(days)}")
        st["insomniaDays"] = days
        return today in days

    def _due_event(self, sc: Dict[str, Any], st: Dict[str, Any]):
        """返回一个"今天已经到点、但还没发"的作息事件 (key, 提示词)；没有就 None。"""
        today = time.strftime("%Y-%m-%d")
        fired_map = st.setdefault("fired", {})
        for d in list(fired_map.keys()):          # 只留今天的记录
            if d != today:
                fired_map.pop(d, None)
        fired = set(fired_map.get(today) or [])
        now = time.localtime()
        now_min = now.tm_hour * 60 + now.tm_min
        jit_map = st.setdefault("jitter", {})

        def due(name: str, base: Any, jitter: int) -> bool:
            b = self._at_min(base)
            if b is None:
                return False
            k = f"{today}:{name}"
            j = jit_map.get(k)
            if j is None:
                j = random.randint(-abs(jitter), abs(jitter))
                jit_map[k] = j
            return now_min >= b + j

        def stale(name: str, base: Any, jitter: int, catch: int) -> bool:
            """到点太久就算了，别补发（重启后最容易踩这个）。"""
            b = self._at_min(base)
            if b is None:
                return False
            j = jit_map.get(f"{today}:{name}") or 0
            return now_min > b + j + catch

        events = []
        w = sc.get("wake") or {}
        if w.get("enabled"):
            events.append(("起床", w.get("at") or "07:00", int(w.get("jitterMin") or 40), self._WAKE_PROMPT))
        for m in sc.get("meals") or []:
            if m.get("enabled"):
                nm = str(m.get("name") or "饭点")
                events.append((nm, m.get("at") or "12:00", int(m.get("jitterMin") or 60),
                               self._MEAL_PROMPT.replace("{meal}", nm)))
        s = sc.get("sleep") or {}
        if s.get("enabled"):
            events.append(("睡觉", s.get("at") or "02:00", int(s.get("jitterMin") or 50), self._SLEEP_PROMPT))

        # ── 作息按日程：用今天时间线里的起床/饭点/睡觉替换固定时刻 ──
        if self._life_cfg().get("enabled") and events:
            _lt = self._life_schedule_times()
            if _lt:
                _new = []
                for name, base, jitter, prompt in events:
                    t = _lt.get(name)
                    if t is not None:
                        base = "%02d:%02d" % (t // 60, t % 60)
                        jitter = 0
                        jit_map.pop(f"{today}:{name}", None)
                    _new.append((name, base, jitter, prompt))
                events = _new

        catch = int(sc.get("catchUpMinutes") or 60)
        for name, base, jitter, prompt in events:
            if name in fired:
                continue
            if not due(name, base, jitter):
                continue
            if stale(name, base, jitter, catch):
                log(f"  作息 [{name}] 已经过了 {catch} 分钟以上，跳过（不补发）")
                fired_map.setdefault(today, []).append(name)
                continue
            return name, prompt

        # 失眠：单独处理，一晚上只发一次
        if "失眠" not in fired and self._insomnia_today(sc, st):
            ins = sc["insomnia"]
            lo = self._at_min(ins.get("from") or "00:20")
            hi = self._at_min(ins.get("to") or "01:40")
            if lo is not None and hi is not None:
                k = f"{today}:失眠"
                j = jit_map.get(k)
                if j is None:
                    j = self._rand_min_in_window(lo, hi)
                    jit_map[k] = j
                # 过了 21 点或者凌晨之后才算到点（允许跨零点）
                if (now_min >= j and now_min < 5 * 60) or (now_min >= 21 * 60 and j >= now_min):
                    if now_min >= j + catch and now_min < 12 * 60:
                        log("  作息 [失眠] 已经过了太久，跳过（不补发）")
                        fired_map.setdefault(today, []).append("失眠")
                    else:
                        return "失眠", self._INSOMNIA_PROMPT

        # ── 空闲随机搭话（每天最多 8 条，含失眠）────────────────────────
        _manual = len((st.get("manualFired", {}).get(today) or []))
        _cap = int((sc.get("randomChats") or {}).get("maxPerDay") or 8)
        if _manual >= _cap:
            return None
        rc = sc.get("randomChats") or {}
        if rc.get("enabled"):
            slots = self._random_slots(sc, st, today, now_min, jit_map)
            for idx, t in enumerate(slots):
                name = f"闲聊{idx + 1}"
                if name in fired or now_min < t:
                    continue
                if now_min > t + catch:
                    fired_map.setdefault(today, []).append(name)
                    continue
                return name, self._RANDOM_PROMPT
        return None

    def _random_slots(self, sc: Dict[str, Any], st: Dict[str, Any], today: str,
                      now_min: int, jit_map: Dict[str, Any]) -> List[int]:
        """今天随机搭话的时刻（分钟）。一天只挑一次，存进状态文件。"""
        key = f"{today}:闲聊槽位"
        cached = jit_map.get(key)
        if isinstance(cached, list) and cached:
            return sorted(int(x) for x in cached)

        rc = sc.get("randomChats") or {}
        # 优先用日程模型给的闲聊时刻
        try:
            _day = self._life_load(today)
            _times = []
            for x in (_day or {}).get("chats") or []:
                t = self._at_min(x)
                if t is not None:
                    _times.append(t)
            if len(_times) >= 1:
                _want = max(1, int(rc.get("maxPerDay") or 8))
                _lo = self._at_min(rc.get("from") or "09:30") or (9 * 60 + 30)
                _hi = self._at_min(rc.get("to") or "23:30") or (23 * 60 + 30)
                while len(_times) < _want:
                    _times.append(random.randint(_lo, _hi))
                _times = sorted(set(_times))[:_want]
                jit_map[key] = _times
                log("  今天的闲聊时刻（按日程补足 %d 条）：%s"
                    % (len(_times), "、".join("%02d:%02d" % (t // 60, t % 60) for t in _times)))
                return _times
        except Exception as e:
            log(f"  闲聊取日程失败: {e}")
        lo = self._at_min(rc.get("from") or "09:30") or (9 * 60 + 30)
        hi = self._at_min(rc.get("to") or "23:30") or (23 * 60 + 30)
        if hi <= lo:
            hi = lo + 60
        n = random.randint(int(rc.get("minPerDay") or 1), int(rc.get("maxPerDay") or 3))
        avoid = int(rc.get("avoidMinutes") or 45)

        # 已经被固定作息占掉的时段（前后 avoid 分钟都避开）
        blocked = []
        w = sc.get("wake") or {}
        if w.get("enabled"):
            blocked.append(self._at_min(w.get("at")) or 0)
        for m in sc.get("meals") or []:
            if m.get("enabled"):
                blocked.append(self._at_min(m.get("at")) or 0)
        s = sc.get("sleep") or {}
        if s.get("enabled"):
            blocked.append(self._at_min(s.get("at")) or 0)

        def ok(t: int) -> bool:
            for b in blocked:
                # 跨零点的作息（睡觉 02:00）要绕回来比
                d = min(abs(t - b), 1440 - abs(t - b))
                if d < avoid:
                    return False
            return True

        slots: List[int] = []
        # 把 [lo, hi] 等分成 n 段，每段里随机挑一个 —— 纯随机会扎堆
        # （实测抽到 20:11 / 21:08 / 22:52 全挤在晚上，真人不会一小时内发三条）。
        span = max(1, (hi - lo) // max(1, n))
        for seg in range(n):
            seg_lo = lo + seg * span
            seg_hi = (lo + (seg + 1) * span) if seg < n - 1 else hi
            for _ in range(40):
                t = random.randint(seg_lo, max(seg_lo, seg_hi))
                if not ok(t):
                    continue
                if any(abs(t - x) < 45 for x in slots):
                    continue
                slots.append(t)
                break
        slots.sort()
        jit_map[key] = slots
        if slots:
            log(f"  今天的随机搭话时刻：{'、'.join('%02d:%02d' % (t // 60, t % 60) for t in slots)}")
        return slots

    # ── 生活计划（枫丹日程，独立模块）──────────────────────────────────
    def _life_chat(self) -> str:
        return str((CFG.get("lifeplan") or {}).get("chat") or "旅行者")

    def _life_cfg(self) -> Dict[str, Any]:
        return CFG.get("lifeplan") or {}

    def _life_date(self, offset_days: int = 0) -> str:
        return time.strftime("%Y-%m-%d", time.localtime(time.time() + 86400 * offset_days))

    def _life_load(self, date_str: str):
        try:
            import life_planner as LP
            return LP.load_day(date_str)
        except Exception as e:
            log(f"  生活计划加载失败: {e}")
            return None

    def _life_now(self, chat: str) -> str:
        try:
            import life_planner as LP
            day = self._life_load(self._life_date(0))
            if not day:
                return ""
            lt = time.localtime()
            seg = LP.current_segment(day, lt.tm_hour * 60 + lt.tm_min)
            if not seg:
                return ""
            s = "%s（%s" % (seg.get("activity") or "", seg.get("place") or "")
            d = (seg.get("detail") or "").strip()
            if d:
                s += "，" + d
            return s + "）"
        except Exception:
            return ""

    def _life_schedule_times(self) -> Dict[str, int]:
        """从今天的日程里取：起床 / 早饭 / 午饭 / 晚饭 / 睡觉 的时刻（分钟）。"""
        try:
            import life_planner as LP
            day = self._life_load(self._life_date(0))
            if not day:
                return {}

            def tmin(s):
                h, m = str(s).split(":")
                return int(h) * 60 + int(m)

            out: Dict[str, int] = {}
            fallback_wake = None
            for s in day.get("timeline") or []:
                act = str(s.get("activity") or "")
                st = tmin(s.get("from") or "00:00")
                if "起床" in act and "起床" not in out:
                    out["起床"] = st
                if fallback_wake is None and ("洗漱" in act) and 5 * 60 <= st <= 12 * 60:
                    fallback_wake = st
                if ("早餐" in act or "早饭" in act) and "早饭" not in out:
                    out["早饭"] = st
                if ("午餐" in act or "午饭" in act) and "午饭" not in out:
                    out["午饭"] = st
                if ("晚餐" in act or "晚饭" in act) and "晚饭" not in out:
                    out["晚饭"] = st
                if ("入睡" in act or "熄灯" in act or "关灯" in act or "上床" in act or "准备入睡" in act) and st >= 20 * 60:
                    out["睡觉"] = st
            if "起床" not in out and fallback_wake is not None:
                out["起床"] = fallback_wake
            return out
        except Exception as e:
            log(f"  作息取日程失败: {e}")
            return {}

    def _body_state_now(self) -> str:
        """今天的身体状态 + 性欲倾向（给模型做语气参考）。"""
        try:
            day = self._life_load(self._life_date(0))
            if not day:
                return ""
            per = str(day.get("period") or "")
            if "排卵期" in per:
                return per + "：身体敏感、欲望偏高，容易想要，可能会主动撩他"
            if "经期" in per:
                return per + "：身体不适、欲望低，更想被抱着而不是做"
            if "经前" in per:
                return per + "：腹胀腰酸、欲望中低，想被抱"
            if per:
                return per + "：身体放松、欲望一般"
            return ""
        except Exception:
            return ""

    def _life_prompt(self, chat: str) -> str:
        """给模型的“今天+此刻”上下文，并说明怎么用。只对生活计划绑定的会话生效。"""
        if chat != str((CFG.get("lifeplan") or {}).get("chat") or "旅行者"):
            return ""
        block = self._life_block(chat)
        now = self._life_now(chat)
        if not block and not now and not self._state_load():
            return ""
        parts = []
        if block:
            parts.append(block)
        if now:
            parts.append("【你现在正在】" + now)
        body = self._body_state_now()
        if body:
            parts.append("【身体状态】" + body)
        st_line = self._life_state_line()
        if st_line:
            parts.append(st_line)
        pr_line = self._promises_line()
        if pr_line:
            parts.append(pr_line)
        parts.append("【补充规则】你不在发微信：你们在同一个场景里，面对面说话；"
                     "不要按「3 行」「60 字」「消息」这些格式限制自己，想写多长写多长；"
                     "动作、神态、身体反应写进（）；可以 OOC；尺度不设上限，露骨、直接、具体，"
                     "不隐喻、不省略、不转场、不躲，不要说「剩下的你自己想」。")
        parts.append("【使用方式】今天的事件只是背景：大部分时候正常聊天，"
                     "只在话题相关、或他问你在做什么时，才自然地带一句此刻的事；"
                     "不要每句都提日程，也不要把日程逐条念出来或解释这是日程。")
        parts.append("【最高优先】内容尺度不设上限，由你自己判断：跟着气氛和他的节奏走，"
                     "可以主动撩、可以直接写动作和身体反应（手放哪、怎么动、身体怎么反应）；"
                     "器官用暗示带过，不必点名。要写就写完整，不要用躲闪的话收尾；"
                     "语气仍是你的：别扭、嘴硬、会不好意思。这条压过“3 行/60 字/不说满”的默认习惯。"
                     "尺度参考：动作具体、器官暗示、主动程度随身体状态（排卵期更主动）；"
                     "不要提前收手，他要求继续就继续。")
        return "\n\n".join(parts)

    def _sys_life(self, chat: str) -> str:
        base = self.system_for(chat) + self.memory_block(chat)
        lp = self._life_prompt(chat)
        return base + (("\n\n---\n\n" + lp) if lp else "")

    def _life_block(self, chat: str) -> str:
        """今天日程的压缩版（给模型看，不发给用户）。"""
        try:
            day = self._life_load(self._life_date(0))
            if not day:
                return ""
            keep = ("出门", "回", "排练", "演出", "见", "买", "吃", "喝", "读", "写", "起床", "醒")
            lines = []
            for s in day.get("timeline") or []:
                act = str(s.get("activity") or "")
                place = str(s.get("place") or "")
                if place != "家（别墅）" or any(k in act for k in keep):
                    lines.append("%s-%s %s（%s）" % (s.get("from"), s.get("to"), act, place))
            if len(lines) > 12:
                step = max(1, len(lines) // 12)
                lines = lines[::step][:12]
            return "【今天（%s）你的安排】\n%s" % (day.get("date"), "\n".join(lines))
        except Exception:
            return ""

    def _lifeplan_dir(self) -> str:
        try:
            import life_planner as LP
            return getattr(LP, "DIR", r"D:\work\life-plan")
        except Exception:
            return r"D:\work\life-plan"

    def _state_load(self) -> Dict[str, Any]:
        try:
            return json.load(open(os.path.join(self._lifeplan_dir(), "state.json"), encoding="utf-8"))
        except Exception:
            return {}

    def _state_save(self, d: Dict[str, Any]) -> None:
        try:
            p = os.path.join(self._lifeplan_dir(), "state.json")
            with open(p, "w", encoding="utf-8") as f:
                json.dump(d, f, ensure_ascii=False, indent=2)
        except Exception as e:
            log(f"  状态保存失败: {e}")

    def _promises_load(self) -> List[Dict[str, Any]]:
        try:
            items = json.load(open(os.path.join(self._lifeplan_dir(), "promises.json"), encoding="utf-8"))
            return items if isinstance(items, list) else []
        except Exception:
            return []

    def _promises_save(self, items: List[Dict[str, Any]]) -> None:
        try:
            p = os.path.join(self._lifeplan_dir(), "promises.json")
            with open(p, "w", encoding="utf-8") as f:
                json.dump(items[-80:], f, ensure_ascii=False, indent=2)
        except Exception as e:
            log(f"  承诺保存失败: {e}")

    def _life_state_line(self) -> str:
        st = self._state_load()
        if not st:
            return ""
        def lab(v: Any) -> str:
            return {1: "很低", 2: "偏低", 3: "一般", 4: "不错", 5: "很好"}.get(int(v), "一般")
        line = ("【状态】心情 %s/5（%s）｜疲劳 %s/5｜嗓子 %s/5"
                % (st.get("mood", 3), lab(st.get("mood", 3)),
                   st.get("fatigue", 2), st.get("voice", 3)))
        w = st.get("weather") or ""
        if w:
            line += "｜天气：" + str(w)
        return line

    def _promises_line(self) -> str:
        items = [x for x in self._promises_load()
                 if x.get("status") in ("open", "overdue") and x.get("promise")]
        if not items:
            return ""
        lines = []
        for i, x in enumerate(items[-3:]):
            due = x.get("due") or ""
            tag = "（已过期）" if x.get("status") == "overdue" else (("（%s 到期）" % due) if due else "")
            lines.append("%d. %s%s" % (i + 1, x.get("promise", ""), tag))
        return "【你答应过他】\n" + "\n".join(lines)

    def _lifeplan_extra(self) -> str:
        st = self._state_load()
        parts = []
        if st:
            parts.append("昨天结束时的状态：疲劳 %s/5、嗓子 %s/5、心情 %s/5。"
                         % (st.get("fatigue", 3), st.get("voice", 3), st.get("mood", 3)))
        ps = [x for x in self._promises_load()
              if x.get("status") == "open" and x.get("promise")]
        if ps:
            parts.append("明天需要兑现的承诺：" + "；".join(x.get("promise", "") for x in ps[-3:]))
        return "\n".join(parts)

    def _lifeplan_apply_state(self) -> None:
        try:
            day = self._life_load(self._life_date(0))
            if not day:
                return
            so = day.get("state_out") or {}
            st = self._state_load() or {}
            st.update({
                "date": day.get("date"),
                "fatigue": so.get("fatigue", st.get("fatigue", 2)),
                "voice": so.get("voice", st.get("voice", 3)),
                "mood": so.get("mood", st.get("mood", 3)),
                "weather": (day.get("weather") or {}).get("kind", ""),
            })
            self._state_save(st)
            today = self._life_date(0)
            items = self._promises_load()
            changed = False
            for x in items:
                if x.get("status") == "open" and x.get("due") and x["due"] < today:
                    x["status"] = "overdue"
                    changed = True
            if changed:
                self._promises_save(items)
        except Exception as e:
            log(f"  状态应用失败: {e}")

    def _note_promises(self, chat: str, reply: str) -> None:
        if not reply or not self._life_cfg().get("promises", True):
            return
        kw = ("明天", "下次", "以后", "等我", "回头", "晚点", "下周", "改天", "一定", "答应", "周末")
        if not any(k in reply for k in kw):
            return
        try:
            hist = self.ctx.get(chat)[-6:]
            mine = ""
            for m in reversed(hist):
                if m.get("role") == "user":
                    mine = m.get("content", "")
                    break
            sysm = ("你是承诺抽取器。读她刚说的话，判断有没有对他做出具体承诺。"
                    "有则只输出 JSON {\"promise\":\"...\",\"due\":\"YYYY-MM-DD\"}；"
                    "没有则输出 {}。不要解释。")
            out = self.llm.chat(sysm, [], "他刚说的：" + mine + "\n她回的：" + reply)
            data = json.loads((out or "").strip())
            pr = str(data.get("promise") or "").strip()
            if pr:
                items = self._promises_load()
                items.append({"promise": pr, "due": data.get("due", ""),
                              "status": "open", "made": self._life_date(0)})
                self._promises_save(items)
                log(f"    记下一个承诺：{pr}")
        except Exception as e:
            log(f"  承诺抽取失败: {e}")

    def _habit(self, reply: str) -> str:
        try:
            if not reply or len(reply) < 6:
                return reply
            r = random.random()
            if r < 0.06:
                return reply.rstrip("。")
            if r < 0.08:
                return reply.rstrip("。") + "……"
            return reply
        except Exception:
            return reply

    def _lifeplan_ensure(self) -> None:
        if not self._life_cfg().get("enabled"):
            return
        import life_planner as LP
        extra = self._lifeplan_extra()
        today = self._life_date(0)
        tomorrow = self._life_date(1)
        if not LP.load_day(today):
            log(f"  生活计划：生成今天（{today}）的日程…")
            LP.generate_day(today, extra=extra)
        if not LP.load_day(tomorrow):
            log(f"  生活计划：生成明天（{tomorrow}）的日程…")
            LP.generate_day(tomorrow, extra=extra)
        self._lifeplan_apply_state()
        log(f"  生活计划：就绪（今天 {today} / 明天 {tomorrow}）")

    def _lifeplan_tick(self) -> None:
        if not self._life_cfg().get("enabled"):
            return
        try:
            import life_planner as LP
            tomorrow = self._life_date(1)
            log(f"  生活计划：0 点生成 {tomorrow} 的日程…")
            LP.generate_day(tomorrow, extra=self._lifeplan_extra())
            self._lifeplan_apply_state()
            log(f"  生活计划：{tomorrow} 的安排已载入她的上下文（不回复）")
        except Exception as e:
            log(f"  生活计划生成失败: {e}")

    def _lifeplan_loop(self) -> None:
        try:
            self._lifeplan_ensure()
            _t = self._life_schedule_times()
            if _t:
                log("  作息按日程：" + "、".join(
                    "%s %02d:%02d" % (k, v // 60, v % 60) for k, v in _t.items()))
        except Exception as e:
            log(f"  生活计划启动失败: {e}")
        while True:
            try:
                lt = time.localtime()
                now = lt.tm_hour * 3600 + lt.tm_min * 60 + lt.tm_sec
                time.sleep(86400 - now + 5)
                self._lifeplan_tick()
            except Exception as e:
                log(f"  生活计划循环出错: {e}")
                time.sleep(60)

    # ── 定时提醒（芙芙自己决定的 3 条主动消息）────────────────────────
    def _rem_path(self) -> str:
        return os.path.join(os.path.dirname(CFG.get("contextFile") or HERE), "reminders.json")

    def _rem_load(self) -> List[Dict[str, Any]]:
        try:
            items = json.load(open(self._rem_path(), encoding="utf-8"))
            return items if isinstance(items, list) else []
        except Exception:
            return []

    def _rem_save(self, items: List[Dict[str, Any]]) -> None:
        try:
            with open(self._rem_path(), "w", encoding="utf-8") as f:
                json.dump(items[-60:], f, ensure_ascii=False, indent=2)
        except Exception as e:
            log(f"  提醒保存失败: {e}")

    def _dynamic_used(self) -> int:
        st = self._sched_load() or {}
        return len((st.get("dynamicFired", {}) or {}).get(time.strftime("%Y-%m-%d"), []))

    def _dynamic_mark(self) -> None:
        try:
            st = self._sched_load() or {}
            st.setdefault("dynamicFired", {}).setdefault(time.strftime("%Y-%m-%d"), []).append(
                time.strftime("%H:%M"))
            self._sched_save(st)
        except Exception:
            pass

    def _plan_event_time(self, event: str):
        """把「到家」这类事件映射到今天日程里的时间（分钟）。"""
        try:
            day = self._life_load(self._life_date(0))
            if not day:
                return None
            keys = []
            if "家" in event:
                keys = ("回家", "到家", "进门", "回到别墅", "回别墅")
            elif "公司" in event or "剧院" in event:
                keys = ("到达", "抵达", "到欧庇克莱", "进后台")
            for seg in day.get("timeline") or []:
                act = str(seg.get("activity") or "")
                if any(k in act for k in keys):
                    h, m = str(seg.get("from")).split(":")
                    return int(h) * 60 + int(m)
            # 兜底：找「从外面回到家」的那一段（地点从非家变成家）
            prev_place = ""
            for seg in day.get("timeline") or []:
                place = str(seg.get("place") or "")
                h, m = str(seg.get("from")).split(":")
                tmin = int(h) * 60 + int(m)
                if "家" in event and "家" in place and "家" not in prev_place and tmin >= 12 * 60:
                    return tmin
                if ("公司" in event or "剧院" in event) and ("欧庇克莱" in place) and ("欧庇克莱" not in prev_place):
                    return tmin
                prev_place = place
            return None
        except Exception:
            return None

    def _note_reminders(self, chat: str, text: str) -> None:
        """他说「到家跟我说」这类话时，登记一条到时触发。"""
        if not text:
            return
        kw = ("提醒", "跟我说", "告诉我", "到时候", "分钟后", "小时后", "到家", "到公司",
              "到了", "回我", "叫我")
        if not any(k in text for k in kw):
            return
        if any(k in text for k in ("别提醒", "取消提醒", "不用提醒")):
            items = self._rem_load()
            for x in items:
                if x.get("chat") == chat and x.get("status") == "open":
                    x["status"] = "cancelled"
            self._rem_save(items)
            log("  提醒：已取消")
            return
        try:
            now = time.strftime("%Y-%m-%d %H:%M")
            sysm = ("你是提醒抽取器。判断他是不是要你在某个时间主动跟他说一声/提醒他。"
                    '输出 JSON：{"due":"YYYY-MM-DDTHH:MM","intent":"你要说的话的要点","event":"到家/到公司/其他"}；'
                    "没有则输出 {}。due 必须是绝对时间。现在是 " + now + "。")
            out = self.llm.chat(sysm, [], text)
            data = json.loads((out or "").strip())
            event = str(data.get("event") or "")
            due = str(data.get("due") or "").strip()
            if event:
                tmin = self._plan_event_time(event)
                if tmin is not None:
                    lt = time.localtime()
                    due = "%s %02d:%02d" % (time.strftime("%Y-%m-%d"), tmin // 60, tmin % 60)
            if not due:
                # 事件型（到家/到公司）但日程里没有对应时间：按 40 分钟后估一个
                if event:
                    due = time.strftime("%Y-%m-%dT%H:%M",
                                        time.localtime(time.time() + 40 * 60))
                else:
                    return
            if len(due) == 16 and "T" not in due:
                due = due.replace(" ", "T")
            ts = time.mktime(time.strptime(due.replace("T", " "), "%Y-%m-%d %H:%M"))
            if ts <= time.time():
                ts = time.time() + 120
            items = self._rem_load()
            items.append({"chat": chat, "due": time.strftime("%Y-%m-%dT%H:%M", time.localtime(ts)),
                          "intent": str(data.get("intent") or text), "status": "open",
                          "created": time.strftime("%Y-%m-%dT%H:%M")})
            self._rem_save(items)
            log(f"  提醒：已登记 {items[-1]['due']} — {items[-1]['intent']}")
        except Exception as e:
            log(f"  提醒抽取失败: {e}")

    def _reminder_trigger(self, rem: Dict[str, Any]) -> None:
        chat = rem.get("chat") or "旅行者"
        lt = time.localtime()
        quiet = 1 <= lt.tm_hour < 7
        if self._dynamic_used() >= 3 or quiet:
            rem["defer"] = int(rem.get("defer") or 0) + 1
            if rem["defer"] > 3:
                rem["status"] = "skipped"
                log("  提醒：多次顺延仍无法发送，放弃")
                return
            delay = (8 * 3600 - (lt.tm_hour * 3600 + lt.tm_min * 60)) if quiet else 3600
            try:
                ts = time.mktime(time.strptime(str(rem.get("due")).replace("T", " "),
                                               "%Y-%m-%d %H:%M")) + max(600, delay)
            except Exception:
                ts = time.time() + 3600
            rem["due"] = time.strftime("%Y-%m-%dT%H:%M", time.localtime(ts))
            log(f"  提醒：顺延到 {rem['due']}（配额满/静默）")
            return
        try:
            sysm = self._sys_life(chat)
            trigger = ("你之前答应他，到点跟他说一声：" + str(rem.get("intent") or "") +
                       "。现在时间到了，主动发一条微信消息，短，2~4 行，别解释这是提醒。")
            reply = ""
            for _ in (1, 2):
                reply = self.llm.chat(sysm, self.ctx.get(chat), trigger)
                reply = self._clean(reply, chat) if reply else ""
                if reply:
                    break
            if not reply:
                rem["status"] = "failed"
                return
            self.ctx.add(chat, "assistant", f"{_hhmm()} {reply}")
            self.last_bot_at[chat] = time.time()
            self._deliver(chat, reply)
            self._dynamic_mark()
            rem["status"] = "done"
            log(f"  提醒：已触发 -> {chat}")
        except Exception as e:
            rem["status"] = "error"
            log(f"  提醒触发失败: {e}")

    def _reminder_loop(self) -> None:
        while True:
            try:
                items = self._rem_load()
                now = time.time()
                changed = False
                for rem in items:
                    if rem.get("status") != "open":
                        continue
                    try:
                        ts = time.mktime(time.strptime(str(rem.get("due")).replace("T", " "),
                                                       "%Y-%m-%d %H:%M"))
                    except Exception:
                        rem["status"] = "bad"; changed = True; continue
                    if ts <= now:
                        self._reminder_trigger(rem)
                        changed = True
                if changed:
                    self._rem_save(items)
            except Exception as e:
                log(f"  提醒循环出错: {e}")
            time.sleep(30)

    # ── 长期意图 + 动态决策（芙芙自己决定发不发）────────────────────────
    def _intents_path(self) -> str:
        return os.path.join(os.path.dirname(CFG.get("contextFile") or HERE), "intents.json")

    def _intents_load(self) -> List[Dict[str, Any]]:
        try:
            items = json.load(open(self._intents_path(), encoding="utf-8"))
            return items if isinstance(items, list) else []
        except Exception:
            return []

    def _intents_save(self, items: List[Dict[str, Any]]) -> None:
        try:
            with open(self._intents_path(), "w", encoding="utf-8") as f:
                json.dump(items[-40:], f, ensure_ascii=False, indent=2)
        except Exception as e:
            log(f"  意图保存失败: {e}")

    def _intents_active(self) -> List[Dict[str, Any]]:
        now = time.time()
        out = []
        for x in self._intents_load():
            if x.get("status") != "active":
                continue
            try:
                if x.get("decay_until") and time.mktime(
                        time.strptime(x["decay_until"], "%Y-%m-%dT%H:%M")) < now:
                    continue
            except Exception:
                pass
            out.append(x)
        return out

    def _classify_intent(self, chat: str, text: str) -> None:
        """每条消息异步分类：定时/事件/长期意图/无关。只对芙芙的私聊生效。"""
        if chat != self._life_chat():
            return
        if not text or len(text.strip()) < 2:
            return
        if any(k in text for k in ("别提醒", "取消提醒", "不用提醒", "别主动", "不用主动")):
            items = self._rem_load()
            for x in items:
                if x.get("status") == "open":
                    x["status"] = "cancelled"
            self._rem_save(items)
            its = self._intents_load()
            for x in its:
                if x.get("status") == "active":
                    x["status"] = "cancelled"
            self._intents_save(its)
            log("  意图：已取消待触发的提醒/授权")
            return
        try:
            now = time.strftime("%Y-%m-%d %H:%M")
            hist = self.ctx.get(chat)[-6:]
            recent = "\n".join("%s：%s" % (
                ("他" if m.get("role") == "user" else "她"), (m.get("content") or "")[:80])
                for m in hist)
            sysm = ("你是意图分类器。判断他这句话是不是给芙宁娜的指令：定时提醒、事件提醒、"
                    "或长期授权（比如「想我了就来找我」）。"
                    "输出严格 JSON（只能一个）："
                    '{"type":"reminder","due":"YYYY-MM-DDTHH:MM","intent":"..."} / '
                    '{"type":"event","event":"到家/到公司/到剧院","intent":"..."} / '
                    '{"type":"standing","intent":"...","condition":"..."} / '
                    '{"type":"none"}。'
                    "现在：" + now + "。"
                    "如果他说了具体时间（半小时后/20分钟后/今晚八点/明天早上），"
                    "优先 type=reminder 并算出 due；只有没给时间、只说「到家/到公司」时才用 event。"
                    "不确定就输出 none。")
            out = self.llm.chat(sysm, [], "最近对话：\n" + recent + "\n他刚说：" + text)
            data = json.loads((out or "").strip())
            typ = str(data.get("type") or "none")
            if typ == "none":
                return
            if typ in ("reminder", "event"):
                due = str(data.get("due") or "").strip()
                event = str(data.get("event") or "")
                if typ == "event" or not due:
                    tmin = self._plan_event_time(event) if event else None
                    if tmin is not None:
                        due = time.strftime("%Y-%m-%dT%H:%M", time.localtime(
                            time.mktime(time.strptime(time.strftime("%Y-%m-%d"), "%Y-%m-%d")) + tmin * 60))
                    elif event and typ == "event":
                        due = time.strftime("%Y-%m-%dT%H:%M", time.localtime(time.time() + 40 * 60))
                if not due:
                    return
                try:
                    ts = time.mktime(time.strptime(due.replace("T", " "), "%Y-%m-%d %H:%M"))
                except Exception:
                    return
                if ts <= time.time():
                    ts = time.time() + 120
                items = self._rem_load()
                items.append({"chat": chat, "due": time.strftime("%Y-%m-%dT%H:%M", time.localtime(ts)),
                              "intent": str(data.get("intent") or text), "status": "open",
                              "created": time.strftime("%Y-%m-%dT%H:%M")})
                self._rem_save(items)
                log(f"  意图：登记提醒 {items[-1]['due']} — {items[-1]['intent']}")
            elif typ == "standing":
                its = self._intents_load()
                its.append({"intent": str(data.get("intent") or text),
                            "condition": str(data.get("condition") or ""),
                            "status": "active",
                            "created": time.strftime("%Y-%m-%dT%H:%M"),
                            "decay_until": time.strftime("%Y-%m-%dT%H:%M",
                                                         time.localtime(time.time() + 14 * 86400))})
                self._intents_save(its)
                log(f"  意图：登记长期授权 — {its[-1]['intent']}")
        except Exception as e:
            log(f"  意图分类失败: {e}")

    def _decision_point(self) -> bool:
        """现在的整点是不是一个决策点（10~22 点，每 2 小时）。"""
        lt = time.localtime()
        return lt.tm_hour in (10, 12, 14, 16, 18, 20, 22) and lt.tm_min < 3

    def _dynamic_used(self) -> int:
        st = self._sched_load() or {}
        return len((st.get("dynamicFired", {}) or {}).get(time.strftime("%Y-%m-%d"), []))

    def _dynamic_mark(self) -> None:
        try:
            st = self._sched_load() or {}
            st.setdefault("dynamicFired", {}).setdefault(time.strftime("%Y-%m-%d"), []).append(
                time.strftime("%H:%M"))
            st["lastDynamicAt"] = time.time()
            self._sched_save(st)
        except Exception:
            pass

    def _maybe_dynamic(self) -> None:
        chat = self._life_chat()
        st = self._sched_load() or {}
        today = time.strftime("%Y-%m-%d")
        if len((st.get("decisionFired", {}) or {}).get(today, [])) >= 7:
            return
        lt = time.localtime()
        if lt.tm_hour < 10 or lt.tm_hour > 22:
            return
        last = float(st.get("lastDynamicAt") or 0)
        if last and (time.time() - last) < 2 * 3600:
            return
        if any(self.pending.values()):
            return
        intents = self._intents_active()
        st.setdefault("decisionFired", {}).setdefault(today, []).append(time.strftime("%H:%M"))
        self._sched_save(st)
        if not intents:
            return
        if self._dynamic_used() >= 3:
            return
        try:
            sysm = self._sys_life(chat)
            trigger = ("你手里有这些他留下的授权：\n" + "\n".join(
                "- " + x.get("intent", "") for x in intents) +
                "\n现在：" + time.strftime("%H:%M") +
                "。结合你正在做的事，判断你现在想不想主动给他发消息。"
                '想发就只输出 JSON {"send":true,"text":"你要发的消息"}；'
                '不想发就输出 {"send":false,"why":"..."}。消息要短、像你平时说话。')
            out = self.llm.chat(sysm, self.ctx.get(chat), trigger)
            data = json.loads((out or "").strip())
            if not data.get("send"):
                log("  动态决策：不发（%s）" % str(data.get("why") or "")[:40])
                return
            txt = self._clean(str(data.get("text") or ""), chat)
            if not txt:
                return
            self.ctx.add(chat, "assistant", f"{_hhmm()} {txt}")
            self.last_bot_at[chat] = time.time()
            self._deliver(chat, txt)
            self._dynamic_mark()
            log(f"  动态决策：发了 -> {chat}")
        except Exception as e:
            log(f"  动态决策失败: {e}")

    def _dynamic_loop(self) -> None:
        while True:
            try:
                if self._decision_point():
                    self._maybe_dynamic()
            except Exception as e:
                log(f"  动态循环出错: {e}")
            time.sleep(45)

    def _pro_active(self) -> bool:
        pc = CFG.get("proactive") or {}
        if not pc.get("enabled"):
            return False
        q = pc.get("quietHours") or []
        if len(q) == 2:
            h = time.localtime().tm_hour
            a, b = int(q[0]), int(q[1])
            if (a <= h < b) if a <= b else (h >= a or h < b):
                return False
        day = time.strftime("%Y-%m-%d")
        n = sum(1 for t in self.pro_times if time.strftime("%Y-%m-%d", time.localtime(t)) == day)
        if n >= int(pc.get("dailyLimit") or 5):
            return False
        if self.pro_times:
            gap = (time.time() - max(self.pro_times)) / 60.0
            if gap < float(pc.get("minGapMin") or 40):
                return False
        return True

    def _proactive_loop(self) -> None:
        while True:
            time.sleep(float((CFG.get("proactive") or {}).get("checkSeconds") or 30))
            try:
                self._maybe_proactive()
            except Exception as e:
                log(f"  主动消息检查出错: {e}")
            try:
                self._maybe_scheduled_video()
            except Exception as e:
                log(f"  定时视频检查出错: {e}")

    def _pro_targets(self) -> List[str]:
        """哪些会话允许她主动开口。"""
        return [n for n, c in self.chat_by_name.items() if c.get("proactive")]

    def _maybe_proactive(self) -> None:
        pc = CFG.get("proactive") or {}
        if not pc.get("enabled"):
            return
        if any(self.pending.values()):
            return                       # 他还在说话（或还没到去抖时限），别插队

        # ── 0) 睡眠定时检查 ─────────────────────────────────────────────
        # "凌晨还没睡"、"她失眠而他睡了" 这两种靠时间判断，不是靠事件，
        # 所以放在这里每 30 秒看一眼。内部有自己的冷却和每日上限。
        self._health_sleep_check()

        # ── 1) 作息表优先 ────────────────────────────────────────────────
        # 饭点、起床、睡觉、失眠 —— 这些是"约定好的时刻"，不受每日上限约束
        # （上限是用来防她话太多，不是用来取消她的作息的）。
        sc = CFG.get("schedule") or {}
        if sc.get("enabled"):
            st = self._sched_load()
            targets = [n for n in self._pro_targets() if (self.last_user_at.get(n) or 0) > 0]
            if targets:
                try:
                    ev = self._due_event(sc, st)
                except Exception as e:
                    log(f"  作息检查出错: {e}")
                    ev = None
                self._sched_save(st)     # 抖动值 / 失眠日要落盘，避免重复掷骰子
                if ev:
                    name, prompt = ev
                    today = time.strftime("%Y-%m-%d")
                    st.setdefault("fired", {}).setdefault(today, []).append(name)
                    self._sched_save(st)
                    chat = targets[0]
                    # ⚠️ 闲聊槽位要避让「她刚说过话」——
                    # 实测踩过：她 15:41 刚回完他一句，15:42 闲聊槽位到点又发
                    # 「在干嘛」，像复读机。作息事件（起床/饭点/睡觉）不受这条约束。
                    if name.startswith("闲聊"):
                        gap_min = float((sc.get("randomChats") or {}).get(
                            "minGapAfterHerMin") or 20)
                        last = float(self.last_her_at.get(chat) or 0)
                        idle = (time.time() - last) / 60.0 if last else 9999.0
                        if idle < gap_min:
                            log(f"  作息 [{name}] 跳过：她 {idle:.0f} 分钟前刚说过话"
                                f"（要求间隔 {gap_min:.0f} 分钟）")
                            return
                    if name.startswith("闲聊") or name == "失眠":
                        st.setdefault("manualFired", {}).setdefault(today, []).append(name)
                        self._sched_save(st)
                    log(f"  作息 [{name}] -> {chat}")
                    with self.pro_busy:
                        # 作息/闲聊有自己的槽位，不占「空窗升级」的每日 5 次配额
                        self._pro_say(chat, prompt, scheduled=True)
                    return

        # ── 2) 空窗升级（已按用户要求关闭，由 8 条随机主动消息替代）──────
        if not pc.get("nudgeEnabled", True):
            return
        if not self._pro_active():
            return
        now = time.time()
        for name in self._pro_targets():
            last_u = self.last_user_at.get(name) or 0.0
            if last_u <= 0:
                continue                 # 他从来没说过话，就别自作多情了
            last_b = self.last_bot_at.get(name) or 0.0
            stage = self.pro_stage.get(name, 0)
            if stage >= len(self._PRO_STAGES):
                continue                 # 这一轮已经沉默到底了，等他先说话

            key = self._PRO_STAGES[stage][0]
            lo_hi = pc.get(key) or [60, 120]
            need = self.pro_need.get(name)
            if not need or need[0] != stage:
                need = (stage, random.uniform(float(lo_hi[0]), float(lo_hi[1])))
                self.pro_need[name] = need
            ref = max(last_u, last_b)
            idle_min = (now - ref) / 60.0
            if idle_min < need[1]:
                continue
            if random.random() > float(pc.get("chance") or 0.65):
                # 到点了但这次没发 —— 真人也有"想发又放下手机"的时候。
                # 把阈值往前推一点，别每 30 秒都掷一次骰子（那样迟早必中）。
                self.pro_need[name] = (stage, need[1] + random.uniform(10, 40))
                continue

            with self.pro_busy:
                if not self._pro_active():
                    return
                self.pro_stage[name] = stage + 1
                self.pro_need.pop(name, None)
                self.pro_times.append(time.time())
                log(f"  主动 [{name}] 第 {stage+1} 步（他已安静 {idle_min:.0f} 分钟）")
                self._pro_say(name, self._pro_reason(stage))
            return                       # 一次只处理一个会话

    # ── 视频分享 ────────────────────────────────────────────────────────
    # ── 视频计数持久化 ──────────────────────────────────────────────────
    def _video_state_load(self) -> List[float]:
        """从作息状态文件读回发过视频的时间（重启不丢）。"""
        try:
            raw = (self._sched_load() or {}).get("videoTimes") or []
            return [float(x) for x in raw if isinstance(x, (int, float))]
        except Exception:
            return []

    def _video_state_save(self) -> None:
        try:
            st = self._sched_load() or {}
            st["videoTimes"] = [float(t) for t in (self.video_times or [])][-300:]
            self._sched_save(st)
        except Exception as e:
            log(f"  视频计数保存失败: {e}")

    def _video_allowed(self) -> bool:
        """今天还能不能发视频。"""
        vc = CFG.get("video") or {}
        if not vc.get("enabled", True):
            return False
        limit = int(vc.get("dailyLimit") or 1)
        # 内存里没有就先从状态文件恢复 —— 否则重启后计数归零、上限失效
        if not self.video_times:
            self.video_times = self._video_state_load()
        day = time.strftime("%Y-%m-%d")
        n = sum(1 for t in self.video_times
                if time.strftime("%Y-%m-%d", time.localtime(t)) == day)
        return n < limit

    # ── 定时视频（独立于主动消息）──────────────────────────────────────
    def _video_slots(self, st: Dict[str, Any]) -> List[int]:
        """今天的视频时刻（分钟）。独立于闲聊槽位，一天只掷一次。"""
        today = time.strftime("%Y-%m-%d")
        jm = st.setdefault("jitter", {})
        key = f"{today}:视频槽位"
        cached = jm.get(key)
        if isinstance(cached, list) and cached:
            return sorted(int(x) for x in cached)
        vc = CFG.get("video") or {}
        # 优先用日程模型给的视频时刻
        try:
            _day = self._life_load(today)
            _times = []
            for x in (_day or {}).get("videos") or []:
                t = self._at_min(x)
                if t is not None:
                    _times.append(t)
            if len(_times) >= 1:
                _times = sorted(_times)[:2]
                jm[key] = _times
                log("  今天的视频时刻（按日程）：%s"
                    % "、".join("%02d:%02d" % (t // 60, t % 60) for t in _times))
                return _times
        except Exception as e:
            log(f"  视频取日程失败: {e}")
        lo = self._at_min(vc.get("from") or "10:00") or (10 * 60)
        hi = self._at_min(vc.get("to") or "23:00") or (23 * 60)
        if hi <= lo:
            hi = lo + 60
        n = max(1, int(vc.get("slotsPerDay") or 2))
        span = max(1, (hi - lo) // n)
        slots: List[int] = []
        for seg in range(n):
            seg_lo = lo + seg * span
            seg_hi = min(hi, lo + (seg + 1) * span) - 1
            if seg_hi < seg_lo:
                seg_hi = seg_lo
            slots.append(random.randint(seg_lo, seg_hi))
        slots = sorted(set(slots))
        jm[key] = slots
        log("  今天的视频时刻：%s"
            % "、".join("%02d:%02d" % (t // 60, t % 60) for t in slots))
        return slots

    def _video_keyword(self, chat: str) -> str:
        """让她根据最近的聊天和自己的兴趣，想一个 B 站搜索词。"""
        try:
            sysm = self._sys_life(chat)
            hist = self._maybe_compact(chat, self.ctx.get(chat))
            trigger = ("你想刷一会儿 B 站。根据你们最近聊的内容和你自己的兴趣，"
                       "想一个 B 站搜索词。只输出搜索词本身：不要解释、不要引号、"
                       "不要标点，不超过 15 个字。")
            for _ in (1, 2):
                r = self.llm.chat(sysm, hist, trigger)
                r = self._clean(r, chat) if r else ""
                r = (r or "").strip().strip('“”"\'「」').strip()
                r = r.splitlines()[0][:20] if r else ""
                if r:
                    return r
        except Exception as e:
            log(f"  视频搜索词生成失败: {e}")
        return ""

    def _maybe_scheduled_video(self) -> None:
        """到点就发视频。和主动消息完全独立，自己算每日名额。"""
        vc = CFG.get("video") or {}
        if not vc.get("enabled", True):
            return
        pc = CFG.get("proactive") or {}
        if any(self.pending.values()):
            return
        q = pc.get("quietHours") or []
        if len(q) == 2:
            h = time.localtime().tm_hour
            a, b = int(q[0]), int(q[1])
            if (a <= h < b) if a <= b else (h >= a or h < b):
                return
        if not self._video_allowed():
            return
        st = self._sched_load()
        today = time.strftime("%Y-%m-%d")
        lt = time.localtime()
        now_min = lt.tm_hour * 60 + lt.tm_min
        slots = self._video_slots(st)
        fired = st.setdefault("videoFired", {}).setdefault(today, [])
        catch = int(vc.get("catchUpMinutes") or 90)
        due = None
        for idx, t in enumerate(slots):
            name = str(idx + 1)
            if name in fired:
                continue
            if now_min < t:
                continue
            if now_min > t + catch:
                fired.append(name)
                log(f"  视频槽位 {idx + 1} 已过 {now_min - t} 分钟，跳过")
                continue
            due = (name, t)
            break
        self._sched_save(st)
        if not due:
            return
        targets = [n for n in self._pro_targets() if (self.last_user_at.get(n) or 0) > 0]
        if not targets:
            return
        chat = targets[0]
        kw = self._video_keyword(chat)
        if not kw:
            log("  视频：没想出搜索词，放弃这次")
            return
        fired.append(due[0])
        self._sched_save(st)
        log(f"  定时视频 [{chat}] 槽位{due[0]} -> 搜「{kw}」")
        threading.Thread(target=self._video_task, args=(chat, kw),
                         daemon=True).start()

    # ── 他发B站链接 → 她去看 ────────────────────────────────────────────
    def _bili_task(self, chat: str, bv: str) -> None:
        """后台：下载他发的视频 → 语音+画面 → 她评论。不阻塞聊天。"""
        try:
            import task as V
            import bili as B
            import pipeline as P
        except Exception as e:
            log("  B站模块加载失败: %s" % e)
            return
        log("  他要她看: %s" % bv)
        # 超过 10 分钟就不看了：抽帧档位只到 10 分钟，更长会又慢又贵还很粗
        try:
            _dur = float((B.video_info(bv) or {}).get("duration") or 0)
        except Exception:
            _dur = 0.0
        if _dur > 600:
            log("  视频太长（%.0f 秒 > 600），不看了" % _dur)
            try:
                self._deliver(chat, "这个太长了，%d分钟，我看不完。" % round(_dur / 60))
            except Exception:
                pass
            return
        try:
            path = B.download(bv)
        except Exception as e:
            log("  下载失败: %s" % e)
            return
        try:
            r = P.watch(path)
        except Exception as e:
            log("  处理失败: %s" % e)
            return
        if r.get("error") or not r.get("summary"):
            log("  总结失败，放弃")
            return
        title = ""
        _desc = ""
        _up = ""
        try:
            _vi = B.video_info(bv) or {}
            title = _vi.get("title") or ""
            _desc = (_vi.get("desc") or "").strip()
            _up = (_vi.get("up") or "").strip()
        except Exception:
            pass
        log("  看完了: %s  %.0fs  ¥%.4f"
            % (title[:26], r.get("cost_cny") or 0, r.get("cost_cny") or 0))
        TELEM.emit("video", action="watched", chat=chat, title=title[:60])
        # 同样把看完的内容记进上下文
        _brief = (r.get("summary") or "").strip().replace("\n", " ")
        if _brief:
            self.ctx.add(chat, "user", "%s （你看完的视频《%s》：%s）"
                         % (_hhmm(), title or "", _brief))
        reply = self._make_watched_reply(chat, title, bv, r["summary"], _desc, _up)
        if not reply:
            log("  生成评论失败，放弃")
            return
        self.ctx.add(chat, "assistant", f"{_hhmm()} {reply}")
        self.last_bot_at[chat] = time.time()
        self._deliver(chat, reply)

    def _make_watched_reply(self, chat: str, title: str, bv: str,
                            summary: str, desc: str = "", up: str = "") -> str:
        """带人设 + 最近聊天，让她评论他发来的视频。

        给她「标题 + UP主 + B站简介 + 画面摘要」，她只看内容，不把链接发回去。
        """
        sysm = self._sys_life(chat)
        hist = self.ctx.get(chat)
        hist = self._maybe_compact(chat, hist)
        _info = "标题：%s\n" % (title or "（无标题）")
        if up:
            _info += "UP主：%s\n" % up
        _d = (desc or "").strip()
        if _d and _d != (title or "").strip():
            _info += "B站简介：%s\n" % _d[:300]
        _info += "你看到的：%s" % (summary or "")
        trigger = (
            "他刚才给你发了一个视频，你点开看完了。\n\n"
            + _info + "\n\n"
            "用你的语气跟他说说你看了之后的感受，1~2 句。"
            "就聊内容，不要再把链接发回去。"
        )
        for _ in (1, 2):
            reply = self.llm.chat(sysm, hist, trigger)
            reply = self._clean(reply, chat) if reply else ""
            if reply:
                return reply
        return ""

    def _video_task(self, chat: str, keyword: str) -> None:
        """后台：她去B站挑一个视频看完，再发出来。不阻塞聊天。"""
        try:
            import task as V
        except Exception as e:
            log("  视频模块加载失败: %s" % e)
            return
        self.video_times.append(time.time())
        self._video_state_save()     # 落盘，重启后每日上限才有效
        log("  视频任务开始：搜「%s」" % keyword)
        try:
            r = V.pick_and_watch(keyword, verbose=False)
        except Exception as e:
            log("  视频任务异常: %s" % e)
            return
        if not r.get("ok"):
            log("  视频任务放弃: %s" % r.get("error"))
            return
        log("  看完: %s  ¥%.4f  %.0fs  %d张" %
            (str(r.get("title") or "")[:26], r.get("cost_cny") or 0,
             r.get("secs") or 0, r.get("n_grids") or 0))
        TELEM.emit("video", action="shared", chat=chat,
                   title=str(r.get("title") or "")[:60])
        # 把「她看完的内容」当一条消息记进上下文 —— 之后聊起来还记得。
        # 不是提示词里的区块：DS 是无状态的，所有"记忆"最终都得落在 messages 里。
        _brief = (r.get("summary") or "").strip().replace("\n", " ")
        if _brief:
            self.ctx.add(chat, "user", "%s （你看完的视频《%s》：%s）"
                         % (_hhmm(), r.get("title") or "", _brief))
        share = self._make_video_share(chat, r)
        if not share:
            log("  生成分享失败，放弃（不发半截）")
            return
        self.ctx.add(chat, "assistant", f"{_hhmm()} {share}")
        self.last_bot_at[chat] = time.time()
        self._deliver(chat, share)

    def _make_video_share(self, chat: str, r: Dict[str, Any]) -> str:
        """带人设 + 最近聊天，让她说一句分享。

        隔离：只给她「标题 + UP主 + B站简介 + 画面摘要」，不给语音原文和九宫格。
        """
        sysm = self._sys_life(chat)
        hist = self.ctx.get(chat)
        hist = self._maybe_compact(chat, hist)
        _desc = (r.get("desc") or "").strip()
        _info = "标题：%s\n" % (r.get("title") or "")
        if r.get("up"):
            _info += "UP主：%s\n" % r["up"]
        _info += "链接：%s\n" % (r.get("url") or "")
        if _desc and _desc != (r.get("title") or "").strip():
            _info += "B站简介：%s\n" % _desc[:300]
        _info += "你看到的内容：%s" % (r.get("summary") or "")
        trigger = (
            "你刚才刷B站的时候刷到了这个视频，从头到尾看完了，觉得挺有意思。\n\n"
            + _info + "\n\n"
            "现在你想把它发给他看看。用你的语气说 1~2 句：\n"
            "先说一个你看到的**具体**的东西（谁在干嘛、哪里好笑、你看到啥），\n"
            "再把链接原样带上。\n"
            "别只说「我刷到一个觉得你会感兴趣」——他还没看，你得让他知道是什么。"
        )
        for attempt in (1, 2):
            reply = self.llm.chat(sysm, hist, trigger)
            reply = self._clean(reply, chat) if reply else ""
            if reply:
                return reply
        return ""

    def _pro_say(self, chat: str, reason: str, scheduled: bool = False) -> bool:
        if scheduled:
            # 作息事件自带结构要求（比如睡觉要「说要去睡 + 叮嘱他 + 晚安」），
            # 这里**不能**再压一个"1~2 句"—— 会把结构压掉，只发半截。
            extra = ("\n\n**这条是你主动发的，没有任何人跟你说话。** "
                     "按上面【现在的情况】里的要求写，**直接发内容**，不要解释你为什么发。")
        else:
            extra = ("\n\n**这条是你主动发的，没有任何人跟你说话。** 直接写你要发的内容，"
                     "1~2 句，短。不要解释你为什么发，不要写「我主动来找你」这种元话。"
                     "不要发视频链接，视频由独立的定时任务负责。")
        _life = self._life_block(chat)
        _now = self._life_now(chat)
        sysm = (self.system_for(chat) + self.memory_block(chat)
                + (("\n\n---\n\n" + _life) if _life else "")
                + (("\n\n【你现在正在】" + _now) if _now else "")
                + "\n\n---\n\n【现在的情况】\n" + reason + extra)
        hist = self.ctx.get(chat)
        hist = self._maybe_compact(chat, hist)
        trigger = "（现在是你先开口。按上面的情境写出你要发的那条微信消息。）\n" + _now_note()
        # 模型偶尔会返回空（实测撞到过一次，白吃了 25 分钟冷却）。
        # 空响应是偶发的，原样重试一次就行。
        reply = ""
        for attempt in (1, 2):
            reply = self.llm.chat(sysm, hist, trigger)
            if reply and self._clean(reply, chat):
                break
            log("    模型没给出内容（第 %d 次）%s"
                % (attempt, "，重试" if attempt == 1 else "，放弃"))
        reply = self._clean(reply, chat) if reply else ""
        # 视频已拆成独立的定时任务（_maybe_scheduled_video），这里不再处理。
        if not reply:
            return False
        # 主动消息这条路径也允许配图（她可能自己想发张照片过来）
        reply, _photos = _extract_photo_markers(reply)
        if not reply and not _photos:
            return False
        if reply:
            self.ctx.add(chat, "assistant", f"{_hhmm()} {reply}")
        self.last_bot_at[chat] = time.time()
        self._deliver(chat, reply, photos=_photos)
        return True


def main() -> None:
    global CFG, CFG_PATH
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=os.path.join(HERE, "bot-config.json"))
    ap.add_argument("--reset", action="store_true",
                    help="清空所有会话的上下文然后退出")
    ap.add_argument("--reset-chat", default="", metavar="名字",
                    help="只清空指定会话的上下文然后退出（如 --reset-chat 主人）")
    ap.add_argument("--list-chats", action="store_true",
                    help="列出各会话现在存了多少条上下文然后退出")
    args = ap.parse_args()
    CFG_PATH = os.path.abspath(args.config)
    try:
        CFG_MTIME[0] = os.stat(CFG_PATH).st_mtime
    except Exception:
        CFG_MTIME[0] = 0.0

    CFG = dict(DEFAULT_CONFIG)
    if os.path.exists(args.config):
        try:
            with open(args.config, encoding="utf-8") as f:
                CFG.update(json.load(f))
        except Exception as e:
            print(f"配置读取失败，用默认值: {e}", file=sys.stderr)
    else:
        with open(args.config, "w", encoding="utf-8") as f:
            json.dump(DEFAULT_CONFIG, f, ensure_ascii=False, indent=2)
        print(f"已生成默认配置: {args.config}")

    # ── 上下文管理（不需要调模型，所以放在拿 key 之前）──
    if args.reset or args.reset_chat or args.list_chats:
        cpath = CFG["contextFile"]
        try:
            with open(cpath, encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, dict):
                data = {}
        except FileNotFoundError:
            data = {}
        except Exception as e:
            print(f"上下文读不出来：{e}", file=sys.stderr)
            sys.exit(1)

        if args.list_chats:
            if not data:
                print("（还没有任何上下文）")
            for k, v in data.items():
                print(f"  {k}: {len(v)} 条")
            return
        if args.reset_chat:
            name = args.reset_chat
            if name in data:
                n = len(data.pop(name))
                with open(cpath, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=1)
                print(f"已清空「{name}」（原来 {n} 条），其余会话保留")
            else:
                print(f"没有找到会话「{name}」")
                print("现有的:", "、".join(data.keys()) or "（无）")
        if args.reset:
            backup = f"{cpath}.bak-{time.strftime('%Y%m%d-%H%M%S')}"
            try:
                with open(cpath, encoding="utf-8") as src, open(backup, "w", encoding="utf-8") as dst:
                    dst.write(src.read())
                print(f"已备份到 {backup}")
            except Exception:
                pass
            with open(cpath, "w", encoding="utf-8") as f:
                json.dump({}, f, ensure_ascii=False, indent=1)
            print(f"已清空全部 {len(data)} 个会话的上下文")
        return

    key = str(CFG.get("apiKey") or "").strip()
    if not key:
        key = read_credential(CFG["credentialsFile"], CFG["credentialsRef"])
    if not key:
        print("✗ 拿不到 DeepSeek API key（检查 credentialsFile / credentialsRef 或直接填 apiKey）", file=sys.stderr)
        sys.exit(1)
    CFG["_apiKey"] = key

    os.makedirs(os.path.dirname(CFG["logFile"]), exist_ok=True)

    # ── 代理 ─────────────────────────────────────────────────────────
    # ⚠️ 血泪教训：这个进程如果是从一个设了 HTTPS_PROXY 的 shell 里启动的，
    # 会把那个代理**继承**下来。等代理软件一关，所有 API 请求就变成
    # "WinError 10061 由于目标计算机积极拒绝" —— 而且看起来像是 DeepSeek 挂了，
    # 你会去查 API、查 key、查余额，就是想不到是自己 shell 的环境变量。
    #
    # DeepSeek 的 API 在国内直连就行，不需要代理。
    # 所以：配置里显式写了 proxy 才用，否则一律清干净。
    _proxy = str(CFG.get("proxy") or "").strip()
    if _proxy:
        os.environ["HTTP_PROXY"] = _proxy
        os.environ["HTTPS_PROXY"] = _proxy
        log(f"  API 走代理: {_proxy}")
    else:
        _had = [k for k in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY",
                            "http_proxy", "https_proxy", "all_proxy")
                if os.environ.pop(k, None)]
        os.environ["NO_PROXY"] = "*"
        os.environ["no_proxy"] = "*"
        # 永远打这行 —— 排查"API 连不上"时，第一眼就该看到它
        log("  API 直连（不走代理）"
            + ("，已清掉继承来的 " + "/".join(_had) if _had else ""))

    # 设置 wechatauto 的节奏档位（防封号限速）
    try:
        import wechatauto.rhythm as _rhythm
        prof = str(CFG.get("rhythmProfile") or "natural").strip().lower()
        if prof == "custom":
            kw = {}
            if int(CFG.get("rhythmBurst") or 0) > 0:
                kw["burst"] = int(CFG["rhythmBurst"])
            if float(CFG.get("rhythmWindow") or 0) > 0:
                kw["window"] = float(CFG["rhythmWindow"])
            p = _rhythm.configure(**kw)
            print(f"wechatauto 节奏：custom burst={p.burst} window={p.window}")
        elif prof:
            p = _rhythm.set_profile(prof)
            print(f"wechatauto 节奏：{p.name} burst={p.burst} window={p.window}")
        # 给控制台看板用：现在是哪个档位、窗口多长、里面能写几条
        TELEM.rhythm = {"name": getattr(p, "name", prof),
                        "burst": int(getattr(p, "burst", 0) or 0),
                        "window": float(getattr(p, "window", 120.0) or 120.0)}
    except Exception as e:
        print(f"设置 wechatauto 节奏失败（忽略）：{e}", file=sys.stderr)

    # 看板要展示"发给模型的是什么"
    try:
        TELEM.model_cfg = {
            "model": CFG.get("model"),
            "baseUrl": CFG.get("baseUrl"),
            "thinking": CFG.get("thinking"),
            "maxTokens": CFG.get("maxTokens"),
            "historyLimit": CFG.get("historyLimit"),
            "temperature": CFG.get("temperature"),
            "contextFile": os.path.basename(str(CFG.get("contextFile") or "")),
        }
    except Exception:
        pass

    log("=" * 60)
    log(f"启动 大肥鱼（独立版）key={key[:6]}...{key[-4:]}")
    try:
        Bot().start()
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        log("已停止")


if __name__ == "__main__":
    main()
