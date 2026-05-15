# -*- coding: utf-8 -*-
import json
from db import get_db, parse_json, set_world_state, set_memory
from agents import call, stream, build_context, sse, get_stats

async def call_node(node, prompt, project_id, out):
    """Stream an agent call, yielding node_text SSE events. Appends (content, cost) to out[]."""
    parts = []
    async for delta in stream(node, prompt, project_id):
        parts.append(delta)
        yield sse({"type": "node_text", "node": node, "delta": delta})
    out.append(("".join(parts), 0))

async def run_full_pipeline(project_id: int):
    with get_db() as conn:
        proj = conn.execute("SELECT * FROM projects WHERE id=?", (project_id,)).fetchone()
    if not proj:
        yield sse({"type": "error", "msg": "项目不存在"})
        return
    idea, style, n_ch = proj["idea"], proj["style"], proj["chapter_target"]

    # 1 灵感解析
    yield sse({"type": "node_start", "node": "inspiration", "label": "灵感解析", "step": 1, "total": 10})
    out = []
    async for ev in call_node("world_builder",
        f"分析灵感，提取：核心概念、情感基调、小说类型、世界类型、主角原型、一个吸引人的小说名（name字段）、以及一段200字以内的吸引读者的作品简介（synopsis字段，用第三人称，突出爽点和钩子）。返回JSON。\n灵感：{idea}", project_id, out):
        yield ev
    r, cost = out[0]
    parsed = parse_json(r) or {}
    genre = parsed.get("type", parsed.get("genre", "玄幻"))
    novel_name = parsed.get("name", "").strip() or idea[:20]
    synopsis = parsed.get("synopsis", "").strip()
    with get_db() as conn:
        conn.execute("UPDATE projects SET genre=?, name=?, synopsis=? WHERE id=?", (genre, novel_name, synopsis, project_id))
    yield sse({"type": "node_done", "node": "inspiration", "cost": cost, "data": {"genre": genre, "name": novel_name, "synopsis": synopsis}})

    # 2 世界观
    yield sse({"type": "node_start", "node": "world", "label": "世界观构建", "step": 2, "total": 10})
    out = []
    async for ev in call_node("world_builder",
        f"灵感：{idea}\n类型：{genre}\n\n构建完整世界观，返回JSON，字段：name/background/history/rules/power_system/geography",
        project_id, out):
        yield ev
    r, cost = out[0]
    world = parse_json(r) or {"background": r[:400]}
    for k, v in world.items():
        set_world_state(project_id, "core", k, str(v)[:500])
    set_memory(project_id, "permanent", "world", json.dumps(world, ensure_ascii=False)[:1000], 10)
    yield sse({"type": "node_done", "node": "world", "cost": cost, "data": world})

    # 3 势力+时间线
    yield sse({"type": "node_start", "node": "factions", "label": "势力与时间线", "step": 3, "total": 10})
    out = []
    async for ev in call_node("world_builder",
        f"世界观：{json.dumps(world, ensure_ascii=False)[:600]}\n\n设计3-5个主要势力和故事时间线。返回JSON，字段：factions（数组，含name/goal/strength）/timeline（含start_point/key_events）",
        project_id, out):
        yield ev
    r, cost = out[0]
    ft = parse_json(r) or {}
    set_world_state(project_id, "factions", "list", json.dumps(ft.get("factions", []), ensure_ascii=False)[:800])
    set_world_state(project_id, "timeline", "main", json.dumps(ft.get("timeline", {}), ensure_ascii=False)[:800])
    yield sse({"type": "node_done", "node": "factions", "cost": cost, "data": ft})

    # 4 角色设计
    yield sse({"type": "node_start", "node": "characters", "label": "角色设计", "step": 4, "total": 10})
    ctx = build_context(project_id)
    out = []
    async for ev in call_node("character_designer",
        f"{ctx}\n\n设计主角+3-4个重要配角，返回JSON数组，每个角色：name/role/appearance/personality/trauma/desire/goal/speech_style/growth_arc",
        project_id, out):
        yield ev
    r, cost = out[0]
    chars = parse_json(r) or []
    if isinstance(chars, list):
        with get_db() as conn:
            for c in chars:
                conn.execute(
                    "INSERT OR IGNORE INTO characters (project_id,name,role,data,current_state) VALUES (?,?,?,?,?)",
                    (project_id, c.get("name", "未知"), c.get("role", "supporting"),
                     json.dumps(c, ensure_ascii=False),
                     json.dumps({"emotion": "平静", "goal": c.get("goal", "")}, ensure_ascii=False))
                )
    yield sse({"type": "node_done", "node": "characters", "cost": cost, "data": chars})

    # 5 剧情规划
    yield sse({"type": "node_start", "node": "plot", "label": "剧情规划", "step": 5, "total": 10})
    ctx = build_context(project_id)
    out = []
    async for ev in call_node("plot_architect",
        f"{ctx}\n\n设计完整剧情，返回JSON，字段：main_plot（含opening/development/climax/ending/hooks）、sub_plots（数组）、thrill_points（爽点分布）、foreshadowing（伏笔列表）",
        project_id, out):
        yield ev
    r, cost = out[0]
    plot = parse_json(r) or {"main_plot": {"opening": r[:300]}}
    with get_db() as conn:
        conn.execute("INSERT INTO plot_lines (project_id,type,title,data) VALUES (?,?,?,?)",
                     (project_id, "main", "主线", json.dumps(plot, ensure_ascii=False)))
    set_memory(project_id, "long_term", "plot", json.dumps(plot, ensure_ascii=False)[:800], 9)
    yield sse({"type": "node_done", "node": "plot", "cost": cost, "data": plot})

    # 6 章节规划
    yield sse({"type": "node_start", "node": "chapter_plan", "label": "章节规划", "step": 6, "total": 10})
    ctx = build_context(project_id)
    main_plot_str = json.dumps(plot.get("main_plot", {}), ensure_ascii=False)[:400]
    out = []
    async for ev in call_node("chapter_planner",
        f"{ctx}\n主线：{main_plot_str}\n\n规划{n_ch}章，返回JSON数组，每项：chapter_num/title/outline（含核心事件/爽点/伏笔/结尾钩子，100字）。【强制要求】这是连载小说的第一批章节，故事远未结束，第{n_ch}章绝对不能是结局章，必须留下强烈悬念让读者想看续集。",
        project_id, out):
        yield ev
    r, cost = out[0]
    plan = parse_json(r) or []
    if not isinstance(plan, list) or not plan:
        plan = [{"chapter_num": i + 1, "title": f"第{i + 1}章", "outline": ""} for i in range(n_ch)]
    with get_db() as conn:
        for ch in plan:
            conn.execute(
                "INSERT INTO chapters (project_id,chapter_num,title,outline,status) VALUES (?,?,?,?,?)",
                (project_id, ch.get("chapter_num", 0), ch.get("title", ""), ch.get("outline", ""), "pending")
            )
    yield sse({"type": "node_done", "node": "chapter_plan", "cost": cost, "data": {"count": len(plan)}})

    # 7-10 逐章写作
    with get_db() as conn:
        chapters = conn.execute(
            "SELECT * FROM chapters WHERE project_id=? AND status='pending' ORDER BY chapter_num",
            (project_id,)
        ).fetchall()
    for ch in chapters:
        yield sse({"type": "chapter_start", "chapter_num": ch["chapter_num"], "title": ch["title"]})
        async for ev in _write_chapter(project_id, ch, ending_note="故事仍在连载中，不要收尾，保持开放式结局，结尾留钩吸引读者继续看。"):
            yield ev

    with get_db() as conn:
        conn.execute("UPDATE projects SET status='done' WHERE id=?", (project_id,))
    yield sse({"type": "pipeline_done", "project_id": project_id, "stats": get_stats(project_id)})


async def run_chapter_pipeline(project_id: int, chapter_id: int, reason: str = ""):
    with get_db() as conn:
        ch = conn.execute("SELECT * FROM chapters WHERE id=? AND project_id=?",
                          (chapter_id, project_id)).fetchone()
    if not ch:
        yield sse({"type": "error", "msg": "章节不存在"})
        return
    yield sse({"type": "chapter_start", "chapter_num": ch["chapter_num"], "title": ch["title"]})
    async for ev in _write_chapter(project_id, ch, reason=reason, ending_note="故事仍在连载中，不要收尾，保持开放式结局，结尾留钩吸引读者继续看。"):
        yield ev
    yield sse({"type": "pipeline_done", "project_id": project_id, "stats": get_stats(project_id)})

async def _write_chapter(project_id: int, ch, reason: str = "", ending_note: str = ""):
    ch_num = ch["chapter_num"]

    # 7 写作（流式）
    yield sse({"type": "node_start", "node": "write", "label": f"写作第{ch_num}章", "step": 7, "total": 10})
    ctx = build_context(project_id, ch_num)
    prompt = (
        f"{ctx}\n\n现在写第{ch_num}章《{ch['title']}》\n"
        f"章节大纲：{ch['outline']}\n\n"
        + (f"重写要求（上一版问题）：{reason}\n\n" if reason else "")
        + (f"特别要求：{ending_note}\n\n" if ending_note else "")
        + "要求：开头抓人、节奏紧凑、爽点密集、对话自然、描写生动、结尾留钩。"
        "不少于3000字。直接输出正文，不要章节标题。"
    )
    parts = []
    async for delta in stream("writer", prompt, project_id, ch["id"]):
        parts.append(delta)
        yield sse({"type": "chapter_chunk", "chapter_num": ch_num, "delta": delta})
    content = "".join(parts)
    word_count = len(content)
    yield sse({"type": "node_done", "node": "write", "cost": 0})

    # 8 审核
    yield sse({"type": "node_start", "node": "audit", "label": f"审核第{ch_num}章", "step": 8, "total": 10})
    from db import get_memory
    mem = get_memory(project_id, "permanent")
    world_core = mem.get("world", "")[:400]
    r, cost = await call("auditor",
        f"世界观：{world_core}\n\n章节内容：\n{content[:2000]}\n\n检查问题，返回JSON：issues（数组，含type/description/severity）/score（1-10）/pass（bool）",
        project_id, ch["id"])
    audit = parse_json(r) or {"issues": [], "score": 8, "pass": True}
    yield sse({"type": "node_done", "node": "audit", "cost": cost, "data": audit})

    # 9 修复（仅当有严重问题时）
    serious = [i for i in audit.get("issues", []) if i.get("severity") in ("high", "critical")]
    if serious and not audit.get("pass", True):
        yield sse({"type": "node_start", "node": "fix", "label": f"修复第{ch_num}章", "step": 9, "total": 10})
        issues_str = json.dumps(serious, ensure_ascii=False)
        r, cost = await call("fixer",
            f"以下问题需要修复：\n{issues_str}\n\n原文：\n{content}\n\n请修复后输出完整正文。",
            project_id, ch["id"])
        content = r
        word_count = len(content)
        yield sse({"type": "node_done", "node": "fix", "cost": cost})

    # 10 记忆更新
    yield sse({"type": "node_start", "node": "memory", "label": "记忆更新", "step": 10, "total": 10})
    r, cost = await call("memory_manager",
        f"章节内容：\n{content[:1500]}\n\n提取需要长期记忆的信息，返回JSON：world_changes（数组，含key/value）/character_changes（数组，含name/emotion/goal）/foreshadowing（数组，含key/content）/events（数组，含key/value）",
        project_id, ch["id"])
    mem_data = parse_json(r) or {}
    for wc in mem_data.get("world_changes", []):
        if wc.get("key"):
            set_world_state(project_id, "events", wc["key"], str(wc.get("value", ""))[:200])
    for fc in mem_data.get("foreshadowing", []):
        if fc.get("key"):
            set_memory(project_id, "long_term", f"foreshadow_{fc['key']}", fc.get("content", "")[:200], 7)
    for ev in mem_data.get("events", []):
        if ev.get("key"):
            set_memory(project_id, "long_term", f"event_ch{ch_num}_{ev['key']}", str(ev.get("value", ""))[:200], 6)
    for cc in mem_data.get("character_changes", []):
        if cc.get("name"):
            with get_db() as conn:
                conn.execute(
                    "UPDATE characters SET current_state=?,updated_at=CURRENT_TIMESTAMP WHERE project_id=? AND name=?",
                    (json.dumps(cc, ensure_ascii=False), project_id, cc["name"])
                )
    yield sse({"type": "node_done", "node": "memory", "cost": cost})

    with get_db() as conn:
        conn.execute(
            "UPDATE chapters SET content=?,word_count=?,status='done',audit_result=? WHERE id=?",
            (content, word_count, json.dumps(audit, ensure_ascii=False), ch["id"])
        )
    yield sse({"type": "chapter_done", "chapter_num": ch_num, "cid": ch["id"], "word_count": word_count,
               "stats": get_stats(project_id)})


async def run_continue_pipeline(project_id: int, n: int, ending: bool):
    with get_db() as conn:
        proj = conn.execute("SELECT * FROM projects WHERE id=?", (project_id,)).fetchone()
        last = conn.execute("SELECT MAX(chapter_num) as m FROM chapters WHERE project_id=?", (project_id,)).fetchone()["m"] or 0
    if not proj:
        yield sse({"type": "error", "msg": "项目不存在"}); return

    # Plan new chapters
    yield sse({"type": "node_start", "node": "chapter_plan", "label": f"规划续写{n}章", "step": 1, "total": 2})
    ctx = build_context(project_id)
    ending_note = "这是全书最终章节，需要收束所有伏笔、完成主角成长弧、给出令人满意的结局。" if ending else ""
    continue_note = "" if ending else "故事仍在连载中，不要收尾，保持开放式结局，结尾留钩吸引读者继续看。"
    out = []
    async for ev in call_node("chapter_planner",
        f"{ctx}\n\n当前已写到第{last}章。续写第{last+1}章到第{last+n}章，共{n}章。{ending_note}{continue_note}\n返回JSON数组，每项：chapter_num/title/outline（含核心事件/爽点/伏笔/结尾钩子，100字）",
        project_id, out):
        yield ev
    r, cost = out[0]
    plan = parse_json(r) or []
    if not isinstance(plan, list) or not plan:
        plan = [{"chapter_num": last+i+1, "title": f"第{last+i+1}章", "outline": ""} for i in range(n)]
    with get_db() as conn:
        for ch in plan:
            conn.execute(
                "INSERT INTO chapters (project_id,chapter_num,title,outline,status) VALUES (?,?,?,?,?)",
                (project_id, ch.get("chapter_num", last+1), ch.get("title",""), ch.get("outline",""), "pending")
            )
        if ending:
            conn.execute("UPDATE projects SET status='writing' WHERE id=?", (project_id,))
    yield sse({"type": "node_done", "node": "chapter_plan", "cost": cost, "data": {"count": len(plan)}})

    # Write chapters
    with get_db() as conn:
        chapters = conn.execute(
            "SELECT * FROM chapters WHERE project_id=? AND chapter_num>? ORDER BY chapter_num",
            (project_id, last)
        ).fetchall()
    for ch in chapters:
        yield sse({"type": "chapter_start", "chapter_num": ch["chapter_num"], "title": ch["title"]})
        async for ev in _write_chapter(project_id, ch, ending_note=ending_note if ending and ch == chapters[-1] else continue_note):
            yield ev

    if ending:
        with get_db() as conn:
            conn.execute("UPDATE projects SET status='done' WHERE id=?", (project_id,))
    yield sse({"type": "pipeline_done", "project_id": project_id, "stats": get_stats(project_id)})


async def run_fix_meta(project_id: int):
    with get_db() as conn:
        proj = conn.execute("SELECT * FROM projects WHERE id=?", (project_id,)).fetchone()
    if not proj:
        yield sse({"type": "error", "msg": "项目不存在"}); return
    idea = proj["idea"]

    # 重新生成小说名+简介
    yield sse({"type": "node_start", "node": "fix_meta", "label": "生成小说名与简介", "step": 1, "total": 3})
    out = []
    async for ev in call_node("world_builder",
        f"分析灵感，提取：核心概念、情感基调、小说类型、世界类型、主角原型、一个吸引人的小说名（name字段）、以及一段200字以内的吸引读者的作品简介（synopsis字段，用第三人称，突出爽点和钩子）。返回JSON。\n灵感：{idea}",
        project_id, out):
        yield ev
    r, _ = out[0]
    parsed = parse_json(r) or {}
    novel_name = parsed.get("name", "").strip()
    synopsis = parsed.get("synopsis", "").strip()
    genre = parsed.get("type", parsed.get("genre", proj["genre"] or "玄幻"))
    if novel_name or synopsis:
        with get_db() as conn:
            conn.execute("UPDATE projects SET name=?,synopsis=?,genre=? WHERE id=?",
                         (novel_name or proj["name"], synopsis, genre, project_id))
    yield sse({"type": "node_done", "node": "fix_meta", "cost": 0,
               "data": {"name": novel_name, "synopsis": synopsis}})

    # 重新生成章节名
    yield sse({"type": "node_start", "node": "fix_chapters", "label": "生成章节名", "step": 2, "total": 3})
    with get_db() as conn:
        chapters = conn.execute(
            "SELECT id,chapter_num,outline FROM chapters WHERE project_id=? ORDER BY chapter_num",
            (project_id,)
        ).fetchall()
    if chapters:
        ch_list = [{"chapter_num": c["chapter_num"], "outline": c["outline"][:100]} for c in chapters]
        out = []
        async for ev in call_node("chapter_planner",
            f"灵感：{idea}\n\n以下是已有章节大纲，为每章生成一个有具体含义的章节名（不要只写第X章）。返回JSON数组，每项：chapter_num/title。\n{json.dumps(ch_list, ensure_ascii=False)}",
            project_id, out):
            yield ev
        r, _ = out[0]
        plan = parse_json(r) or []
        if isinstance(plan, list):
            with get_db() as conn:
                for item in plan:
                    num = item.get("chapter_num")
                    title = item.get("title", "").strip()
                    if num and title and (not title.startswith("第") or len(title) > 4):
                        conn.execute("UPDATE chapters SET title=? WHERE project_id=? AND chapter_num=?",
                                     (title, project_id, num))
    yield sse({"type": "node_done", "node": "fix_chapters", "cost": 0})

    # 重新生成角色（如果角色表为空）
    yield sse({"type": "node_start", "node": "fix_chars", "label": "生成角色", "step": 3, "total": 3})
    with get_db() as conn:
        char_count = conn.execute("SELECT COUNT(*) c FROM characters WHERE project_id=?", (project_id,)).fetchone()["c"]
    if char_count == 0:
        from agents import build_context
        ctx = build_context(project_id)
        out = []
        async for ev in call_node("character_designer",
            f"{ctx}\n\n设计主角+3-4个重要配角，返回JSON数组，每个角色：name/role/appearance/personality/trauma/desire/goal/speech_style/growth_arc",
            project_id, out):
            yield ev
        r, _ = out[0]
        chars = parse_json(r) or []
        if isinstance(chars, list):
            with get_db() as conn:
                for c in chars:
                    conn.execute(
                        "INSERT OR IGNORE INTO characters (project_id,name,role,data,current_state) VALUES (?,?,?,?,?)",
                        (project_id, c.get("name", "未知"), c.get("role", "supporting"),
                         json.dumps(c, ensure_ascii=False),
                         json.dumps({"emotion": "平静", "goal": c.get("goal", "")}, ensure_ascii=False))
                    )
    yield sse({"type": "node_done", "node": "fix_chars", "cost": 0})
    yield sse({"type": "pipeline_done", "project_id": project_id, "stats": get_stats(project_id)})
