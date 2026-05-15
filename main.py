# -*- coding: utf-8 -*-
import json
from fastapi import FastAPI, Request, UploadFile, File
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from contextlib import asynccontextmanager
from db import (init_db, get_db, get_setting, set_setting,
                get_world_state, get_memory, set_memory)
from agents import sse, get_stats
from pipeline import run_full_pipeline, run_chapter_pipeline, run_continue_pipeline, run_fix_meta

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield

app = FastAPI(lifespan=lifespan)

# ── Settings ──────────────────────────────────────────────────────────────────

@app.get("/api/settings")
async def api_get_settings():
    key = get_setting("api_key")
    return {"configured": bool(key), "hint": key[:8]+"..." if len(key) > 8 else ""}

@app.post("/api/settings")
async def api_save_settings(req: Request):
    d = await req.json()
    set_setting("api_key", d.get("api_key",""))
    return {"ok": True}

# ── Projects ──────────────────────────────────────────────────────────────────

@app.get("/api/projects")
async def api_list_projects():
    with get_db() as conn:
        rows = conn.execute(
            "SELECT p.*, (SELECT COUNT(*) FROM chapters WHERE project_id=p.id AND status='done') as done_chapters,"
            "(SELECT COALESCE(SUM(word_count),0) FROM chapters WHERE project_id=p.id) as total_words "
            "FROM projects p ORDER BY p.id DESC"
        ).fetchall()
    return [dict(r) for r in rows]

@app.post("/api/projects")
async def api_create_project(req: Request):
    d = await req.json()
    if not d.get("idea","").strip():
        return JSONResponse({"error":"请输入灵感"}, 400)
    if not get_setting("api_key"):
        return JSONResponse({"error":"请先配置 API Key"}, 400)
    with get_db() as conn:
        cur = conn.execute(
            "INSERT INTO projects (name,idea,style,chapter_target) VALUES (?,?,?,?)",
            (d.get("name","新项目"), d["idea"].strip(), d.get("style","qidian"), int(d.get("chapter_target",20)))
        )
        pid = cur.lastrowid
    return {"id": pid}

@app.get("/api/projects/{pid}")
async def api_get_project(pid: int):
    with get_db() as conn:
        p = conn.execute("SELECT * FROM projects WHERE id=?", (pid,)).fetchone()
        if not p:
            return JSONResponse({"error": "not found"}, 404)
        chars = conn.execute("SELECT id,name,role,data,current_state FROM characters WHERE project_id=?", (pid,)).fetchall()
        chapters = conn.execute(
            "SELECT id,chapter_num,title,word_count,status FROM chapters WHERE project_id=? ORDER BY chapter_num", (pid,)
        ).fetchall()
        plots = conn.execute("SELECT * FROM plot_lines WHERE project_id=?", (pid,)).fetchall()
        prompts = conn.execute("SELECT * FROM prompts WHERE is_active=1 ORDER BY agent_name").fetchall()
    return {
        "project": dict(p),
        "characters": [dict(c) for c in chars],
        "chapters": [dict(c) for c in chapters],
        "plots": [dict(p) for p in plots],
        "world_state": get_world_state(pid),
        "stats": get_stats(pid),
        "prompts": [dict(p) for p in prompts],
    }

@app.delete("/api/projects/{pid}")
async def api_delete_project(pid: int):
    with get_db() as conn:
        for tbl in ["pipeline_runs","memory","world_state","characters","chapters","plot_lines","projects"]:
            col = "id" if tbl == "projects" else "project_id"
            conn.execute(f"DELETE FROM {tbl} WHERE {col}=?", (pid,))
    return {"ok": True}

# ── Pipeline ──────────────────────────────────────────────────────────────────

@app.post("/api/projects/{pid}/run")
async def api_run_pipeline(pid: int):
    return StreamingResponse(
        run_full_pipeline(pid),
        media_type="text/event-stream",
        headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"}
    )

@app.post("/api/projects/{pid}/chapters/{cid}/run")
async def api_run_chapter(pid: int, cid: int, request: Request):
    body = await request.json() if request.headers.get("content-type","").startswith("application/json") else {}
    reason = body.get("reason", "")
    return StreamingResponse(
        run_chapter_pipeline(pid, cid, reason=reason),
        media_type="text/event-stream",
        headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"}
    )

@app.post("/api/projects/{pid}/continue")
async def api_continue(pid: int, request: Request):
    body = await request.json()
    return StreamingResponse(
        run_continue_pipeline(pid, int(body.get("n", 10)), bool(body.get("ending", False))),
        media_type="text/event-stream",
        headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"}
    )

@app.post("/api/projects/{pid}/fix-meta")
async def api_fix_meta(pid: int):
    return StreamingResponse(
        run_fix_meta(pid),
        media_type="text/event-stream",
        headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"}
    )

# ── Chapters ──────────────────────────────────────────────────────────────────

@app.get("/api/projects/{pid}/chapters/{cid}")
async def api_get_chapter(pid: int, cid: int):
    with get_db() as conn:
        row = conn.execute("SELECT * FROM chapters WHERE id=? AND project_id=?", (cid, pid)).fetchone()
    return dict(row) if row else JSONResponse({"error":"not found"}, 404)

@app.patch("/api/projects/{pid}/chapters/{cid}")
async def api_update_chapter(pid: int, cid: int, req: Request):
    d = await req.json()
    with get_db() as conn:
        if "content" in d:
            wc = len(d["content"])
            conn.execute("UPDATE chapters SET content=?,word_count=? WHERE id=? AND project_id=?",
                         (d["content"], wc, cid, pid))
        if "title" in d:
            conn.execute("UPDATE chapters SET title=? WHERE id=? AND project_id=?", (d["title"], cid, pid))
    return {"ok": True}

# ── Characters ────────────────────────────────────────────────────────────────

@app.get("/api/projects/{pid}/characters")
async def api_get_characters(pid: int):
    with get_db() as conn:
        rows = conn.execute("SELECT * FROM characters WHERE project_id=?", (pid,)).fetchall()
    return [dict(r) for r in rows]

@app.patch("/api/projects/{pid}/characters/{cid}")
async def api_update_character(pid: int, cid: int, req: Request):
    d = await req.json()
    with get_db() as conn:
        if "data" in d:
            conn.execute("UPDATE characters SET data=?,updated_at=CURRENT_TIMESTAMP WHERE id=? AND project_id=?",
                         (json.dumps(d["data"], ensure_ascii=False), cid, pid))
    return {"ok": True}

# ── Memory ────────────────────────────────────────────────────────────────────

@app.get("/api/projects/{pid}/memory")
async def api_get_memory(pid: int):
    return get_memory(pid)

# ── Prompts ───────────────────────────────────────────────────────────────────

@app.get("/api/prompts")
async def api_get_prompts():
    with get_db() as conn:
        rows = conn.execute("SELECT * FROM prompts WHERE is_active=1 ORDER BY agent_name").fetchall()
    return [dict(r) for r in rows]

@app.patch("/api/prompts/{pid}")
async def api_update_prompt(pid: int, req: Request):
    d = await req.json()
    with get_db() as conn:
        conn.execute("UPDATE prompts SET system_prompt=? WHERE id=?", (d["system_prompt"], pid))
    return {"ok": True}

# ── Pipeline logs ─────────────────────────────────────────────────────────────

@app.get("/api/projects/{pid}/logs")
async def api_get_logs(pid: int):
    with get_db() as conn:
        rows = conn.execute(
            "SELECT node,status,tokens_used,cost_usd,started_at,finished_at FROM pipeline_runs "
            "WHERE project_id=? ORDER BY id DESC LIMIT 50", (pid,)
        ).fetchall()
    return [dict(r) for r in rows]

# ── Knowledge Base ─────────────────────────────────────────────────────────────

@app.get("/api/knowledge/{pid}/files")
async def kb_list(pid: int):
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id, name, created_at FROM knowledge_files WHERE project_id=? ORDER BY id DESC", (pid,)
        ).fetchall()
    return [dict(r) for r in rows]

@app.post("/api/knowledge/{pid}/files")
async def kb_upload(pid: int, file: UploadFile = File(...)):
    content = (await file.read()).decode("utf-8", errors="ignore")
    with get_db() as conn:
        conn.execute(
            "INSERT INTO knowledge_files (project_id, name, content) VALUES (?,?,?)",
            (pid, file.filename, content)
        )
    return {"ok": True}

@app.delete("/api/knowledge/{pid}/files/{fid}")
async def kb_delete(pid: int, fid: int):
    with get_db() as conn:
        conn.execute("DELETE FROM knowledge_files WHERE id=? AND project_id=?", (fid, pid))
    return {"ok": True}

# ── Frontend ──────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def index():
    with open("index.html", encoding="utf-8") as f:
        return f.read()
