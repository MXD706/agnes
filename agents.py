# -*- coding: utf-8 -*-
import json
from openai import AsyncOpenAI
from db import get_db, get_prompt, get_setting

DEEPSEEK_BASE = "https://api.deepseek.com"
INPUT_COST  = 0.00014   # per 1K tokens
OUTPUT_COST = 0.00028

def get_client():
    return AsyncOpenAI(api_key=get_setting("api_key"), base_url=DEEPSEEK_BASE)

async def call(agent_name: str, user_msg: str, project_id: int = None, chapter_id: int = None):
    """Non-streaming agent call. Returns (content, cost)."""
    system = get_prompt(agent_name)
    client = get_client()
    run_id = _log_start(project_id, chapter_id, agent_name, user_msg)
    resp = await client.chat.completions.create(
        model="deepseek-chat",
        messages=[{"role":"system","content":system},{"role":"user","content":user_msg}],
        max_tokens=8192,
    )
    content = resp.choices[0].message.content
    cost = (resp.usage.prompt_tokens/1000*INPUT_COST) + (resp.usage.completion_tokens/1000*OUTPUT_COST)
    _log_done(run_id, resp.usage.prompt_tokens + resp.usage.completion_tokens, cost)
    return content, cost

async def call_stream(agent_name: str, user_msg: str, project_id: int = None, chapter_id: int = None):
    """Streaming call that yields text deltas, then returns (full_content, cost) via StopAsyncIteration value."""
    parts = []
    async for delta in stream(agent_name, user_msg, project_id, chapter_id):
        parts.append(delta)
        yield delta
    # cost not tracked here; caller uses return value pattern via separate call if needed

async def stream(agent_name: str, user_msg: str, project_id: int = None, chapter_id: int = None):
    """Streaming agent call. Async generator yielding text deltas."""
    system = get_prompt(agent_name)
    client = get_client()
    run_id = _log_start(project_id, chapter_id, agent_name, user_msg)
    resp = await client.chat.completions.create(
        model="deepseek-chat",
        messages=[{"role":"system","content":system},{"role":"user","content":user_msg}],
        max_tokens=8192, stream=True,
    )
    parts = []
    async for chunk in resp:
        delta = chunk.choices[0].delta.content or ""
        if delta:
            parts.append(delta)
            yield delta
    _log_done(run_id, 0, 0)

def _log_start(project_id, chapter_id, node, msg):
    if not project_id:
        return None
    with get_db() as conn:
        cur = conn.execute(
            "INSERT INTO pipeline_runs (project_id,chapter_id,node,status,started_at) VALUES (?,?,?,'running',CURRENT_TIMESTAMP)",
            (project_id, chapter_id, node)
        )
        return cur.lastrowid

def _log_done(run_id, tokens, cost):
    if not run_id:
        return
    with get_db() as conn:
        conn.execute(
            "UPDATE pipeline_runs SET status='done',tokens_used=?,cost_usd=?,finished_at=CURRENT_TIMESTAMP WHERE id=?",
            (tokens, cost, run_id)
        )

def build_context(project_id: int, chapter_num: int = None) -> str:
    """Build shared context string for all agents."""
    from db import get_world_state, get_memory
    with get_db() as conn:
        proj = conn.execute("SELECT * FROM projects WHERE id=?", (project_id,)).fetchone()
        chars = conn.execute(
            "SELECT name,role,data,current_state FROM characters WHERE project_id=? LIMIT 6",
            (project_id,)
        ).fetchall()
    world = get_world_state(project_id)
    mem = get_memory(project_id)
    name, idea, genre, style = proj["name"], proj["idea"], proj["genre"], proj["style"]
    parts = [f"【项目】{name}\n【灵感】{idea}\n【类型】{genre}\n【风格】{style}"]
    if world.get("core"):
        lines = "\n".join(f"- {k}: {v[:80]}" for k, v in list(world["core"].items())[:8])
        parts.append("【世界观】\n" + lines)
    if chars:
        cs = []
        for c in chars:
            d = json.loads(c["data"]) if c["data"] else {}
            s = json.loads(c["current_state"]) if c["current_state"] else {}
            cname, crole = c["name"], c["role"]
            personality = d.get("personality", "")[:40]
            emotion = s.get("emotion", "平静")
            cs.append(f"{cname}({crole}): {personality} 当前:{emotion}")
        parts.append("【角色】\n" + "\n".join(cs))
    if mem.get("permanent"):
        lines = "\n".join(f"- {k}: {v[:80]}" for k, v in list(mem["permanent"].items())[:4])
        parts.append("【永久设定】\n" + lines)
    if mem.get("long_term"):
        lines = "\n".join(f"- {k}: {v[:80]}" for k, v in list(mem["long_term"].items())[:6])
        parts.append("【长期记忆】\n" + lines)
    if chapter_num and chapter_num > 1:
        with get_db() as conn:
            prev = conn.execute(
                "SELECT title,content FROM chapters WHERE project_id=? AND chapter_num=? AND status='done'",
                (project_id, chapter_num - 1)
            ).fetchone()
        if prev:
            ptitle, pcontent = prev["title"], prev["content"][-300:]
            parts.append(f"【上章结尾】{ptitle}\n{pcontent}")
    return "\n\n".join(parts)

def sse(data: dict) -> str:
    return "data: " + json.dumps(data, ensure_ascii=False) + "\n\n"

def get_stats(project_id: int) -> dict:
    with get_db() as conn:
        cost  = conn.execute("SELECT COALESCE(SUM(cost_usd),0) c FROM pipeline_runs WHERE project_id=?", (project_id,)).fetchone()["c"]
        tok   = conn.execute("SELECT COALESCE(SUM(tokens_used),0) t FROM pipeline_runs WHERE project_id=?", (project_id,)).fetchone()["t"]
        done  = conn.execute("SELECT COUNT(*) c FROM chapters WHERE project_id=? AND status='done'", (project_id,)).fetchone()["c"]
        words = conn.execute("SELECT COALESCE(SUM(word_count),0) w FROM chapters WHERE project_id=?", (project_id,)).fetchone()["w"]
    return {"cost_cny": round(cost*7.2,2), "tokens": tok, "done_chapters": done, "total_words": words}
