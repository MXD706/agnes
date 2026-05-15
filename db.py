# -*- coding: utf-8 -*-
import sqlite3, json
from pathlib import Path

DB_PATH = Path(__file__).parent / "agnes.db"
_PROMPTS_PATH = Path(__file__).parent / "prompts.json"

SCHEMA = """
CREATE TABLE IF NOT EXISTS projects (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    idea TEXT NOT NULL,
    genre TEXT DEFAULT "",
    style TEXT DEFAULT "qidian",
    synopsis TEXT DEFAULT "",
    chapter_target INTEGER DEFAULT 20,
    status TEXT DEFAULT "init",
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS world_state (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER NOT NULL,
    category TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(project_id, category, key)
);
CREATE TABLE IF NOT EXISTS characters (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    role TEXT DEFAULT "supporting",
    data TEXT NOT NULL DEFAULT "{}",
    current_state TEXT NOT NULL DEFAULT "{}",
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS plot_lines (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER NOT NULL,
    type TEXT DEFAULT "main",
    title TEXT NOT NULL,
    data TEXT NOT NULL DEFAULT "{}"
);
CREATE TABLE IF NOT EXISTS chapters (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER NOT NULL,
    chapter_num INTEGER NOT NULL,
    title TEXT NOT NULL,
    outline TEXT DEFAULT "",
    content TEXT DEFAULT "",
    word_count INTEGER DEFAULT 0,
    status TEXT DEFAULT "pending",
    audit_result TEXT DEFAULT "{}"
);
CREATE TABLE IF NOT EXISTS memory (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER NOT NULL,
    layer TEXT NOT NULL,
    key TEXT NOT NULL,
    content TEXT NOT NULL,
    importance INTEGER DEFAULT 5,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(project_id, layer, key)
);
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER NOT NULL,
    chapter_id INTEGER,
    node TEXT NOT NULL,
    status TEXT DEFAULT "pending",
    tokens_used INTEGER DEFAULT 0,
    cost_usd REAL DEFAULT 0,
    started_at DATETIME,
    finished_at DATETIME
);
CREATE TABLE IF NOT EXISTS prompts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    agent_name TEXT NOT NULL,
    version INTEGER DEFAULT 1,
    system_prompt TEXT NOT NULL,
    is_active INTEGER DEFAULT 1,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS knowledge_files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    content TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
"""

with open(_PROMPTS_PATH, encoding='utf-8') as _f:
    AGENT_PROMPTS = json.load(_f)

def get_db():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def init_db():
    with get_db() as conn:
        conn.executescript(SCHEMA)
        conn.execute("DELETE FROM prompts")
        for name, prompt in AGENT_PROMPTS.items():
            conn.execute("INSERT INTO prompts (agent_name,system_prompt) VALUES (?,?)", (name, prompt))

def get_setting(key, default=""):
    with get_db() as conn:
        row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return row["value"] if row else default

def set_setting(key, value):
    with get_db() as conn:
        conn.execute("INSERT OR REPLACE INTO settings VALUES (?,?)", (key, value))

def get_prompt(agent_name):
    with get_db() as conn:
        row = conn.execute("SELECT system_prompt FROM prompts WHERE agent_name=? AND is_active=1 ORDER BY version DESC LIMIT 1", (agent_name,)).fetchone()
        return row["system_prompt"] if row else "你是AI助手。"

def get_world_state(project_id):
    with get_db() as conn:
        rows = conn.execute("SELECT category,key,value FROM world_state WHERE project_id=?", (project_id,)).fetchall()
    state = {}
    for r in rows:
        state.setdefault(r["category"], {})[r["key"]] = r["value"]
    return state

def set_world_state(project_id, category, key, value):
    with get_db() as conn:
        conn.execute("INSERT OR REPLACE INTO world_state (project_id,category,key,value,updated_at) VALUES (?,?,?,?,CURRENT_TIMESTAMP)", (project_id, category, key, value))

def get_memory(project_id, layer=None):
    with get_db() as conn:
        if layer:
            rows = conn.execute("SELECT key,content FROM memory WHERE project_id=? AND layer=? ORDER BY importance DESC", (project_id, layer)).fetchall()
        else:
            rows = conn.execute("SELECT layer,key,content FROM memory WHERE project_id=? ORDER BY importance DESC", (project_id,)).fetchall()
    if layer:
        return {r["key"]: r["content"] for r in rows}
    result = {}
    for r in rows:
        result.setdefault(r["layer"], {})[r["key"]] = r["content"]
    return result

def set_memory(project_id, layer, key, content, importance=5):
    with get_db() as conn:
        conn.execute("INSERT OR REPLACE INTO memory (project_id,layer,key,content,importance,updated_at) VALUES (?,?,?,?,?,CURRENT_TIMESTAMP)", (project_id, layer, key, content, importance))

def get_project_stats(project_id):
    with get_db() as conn:
        cost = conn.execute("SELECT COALESCE(SUM(cost_usd),0) as c FROM pipeline_runs WHERE project_id=?", (project_id,)).fetchone()["c"]
        tokens = conn.execute("SELECT COALESCE(SUM(tokens_used),0) as t FROM pipeline_runs WHERE project_id=?", (project_id,)).fetchone()["t"]
        done = conn.execute("SELECT COUNT(*) as c FROM chapters WHERE project_id=? AND status='done'", (project_id,)).fetchone()["c"]
        words = conn.execute("SELECT COALESCE(SUM(word_count),0) as w FROM chapters WHERE project_id=?", (project_id,)).fetchone()["w"]
    return {"cost_usd": round(cost,4), "cost_cny": round(cost*7.2,2), "tokens": tokens, "done_chapters": done, "total_words": words}

def parse_json(text):
    if not text: return None
    text = text.strip()
    for fence in ["```json", "```"]:
        if fence in text:
            parts = text.split(fence)
            for i in range(1, len(parts), 2):
                chunk = parts[i].strip().rstrip("`").strip()
                try: return json.loads(chunk)
                except: pass
    try: return json.loads(text)
    except: return None
