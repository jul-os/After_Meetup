from __future__ import annotations

import base64, hashlib, json, mimetypes, os, shutil, subprocess, threading, time, uuid
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI, File, Form, HTTPException, UploadFile, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field, field_validator
import sqlite3

ROOT = Path(__file__).parent
DB_PATH = Path(os.getenv("DATABASE_URL", "sqlite:///./after_meeting.db").replace("sqlite:///", ""))
STORAGE = Path(os.getenv("STORAGE_DIR", str(ROOT / "storage")))
MAX_BYTES = int(float(os.getenv("MAX_FILE_MB", "250")) * 1024 * 1024)
MAX_SECONDS = int(float(os.getenv("MAX_AUDIO_MINUTES", "60")) * 60)
RETENTION_HOURS = int(os.getenv("AUDIO_RETENTION_HOURS", "24"))
ALLOWED = {".mp3", ".wav", ".m4a"}
LOCK = threading.Lock()

app = FastAPI(title="После встречи", version="1.0.0")
logger = logging.getLogger("after_meeting")
app.mount("/static", StaticFiles(directory=ROOT / "static"), name="static")

@app.exception_handler(RequestValidationError)
async def validation_error(_: Request, exc: RequestValidationError):
    return error("INVALID_FIELD", "Проверьте поля запроса", 422, {"validation": exc.errors()})

def db():
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys=ON")
    return con

def init_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True); STORAGE.mkdir(parents=True, exist_ok=True)
    with db() as c:
        c.executescript('''
        CREATE TABLE IF NOT EXISTS meetings (id TEXT PRIMARY KEY, title TEXT NOT NULL, meeting_date TEXT NOT NULL,
          participants TEXT NOT NULL, consent_at TEXT NOT NULL, status TEXT NOT NULL, stage TEXT NOT NULL,
          error_code TEXT, created_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS segments (id TEXT PRIMARY KEY, meeting_id TEXT NOT NULL REFERENCES meetings(id) ON DELETE CASCADE,
          start_ms INTEGER NOT NULL CHECK(start_ms>=0), end_ms INTEGER NOT NULL CHECK(end_ms>start_ms), text TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS decisions (id TEXT PRIMARY KEY, meeting_id TEXT NOT NULL REFERENCES meetings(id) ON DELETE CASCADE,
          text TEXT NOT NULL, source_segment_id TEXT NOT NULL REFERENCES segments(id), review_status TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS tasks (id TEXT PRIMARY KEY, meeting_id TEXT NOT NULL REFERENCES meetings(id) ON DELETE CASCADE,
          text TEXT NOT NULL, assignee TEXT, due_date TEXT, source_segment_id TEXT NOT NULL REFERENCES segments(id), review_status TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS jobs (id TEXT PRIMARY KEY, meeting_id TEXT NOT NULL REFERENCES meetings(id) ON DELETE CASCADE,
          attempts INTEGER NOT NULL, stage TEXT NOT NULL, audio_path TEXT, audio_delete_by TEXT);
        CREATE TABLE IF NOT EXISTS usage (id TEXT PRIMARY KEY, meeting_id TEXT NOT NULL REFERENCES meetings(id) ON DELETE CASCADE,
          duration_seconds INTEGER NOT NULL, estimated_cost REAL);
        ''')

@app.on_event("startup")
def startup(): init_db()

def error(code, message, status=422, details=None):
    return JSONResponse(status_code=status, content={"error":{"code":code,"message":message,"details":details or {}}})

def row_dict(r): return dict(r) if r else None
def now(): return datetime.now(timezone.utc).isoformat()
def get_meeting(mid):
    with db() as c: return c.execute("SELECT * FROM meetings WHERE id=?", (mid,)).fetchone()
def require_meeting(mid):
    m=get_meeting(mid)
    if not m: raise HTTPException(404, detail={"error":{"code":"NOT_FOUND","message":"Встреча не найдена","details":{}}})
    return m

def probe(path: Path):
    try:
        p=subprocess.run(["ffprobe","-v","error","-show_entries","format=duration,format_name","-of","json",str(path)], capture_output=True, text=True, timeout=30)
        data=json.loads(p.stdout); duration=float(data["format"]["duration"]); fmt=data["format"].get("format_name", "")
        if duration > MAX_SECONDS: raise ValueError("AUDIO_TOO_LONG")
        return duration, fmt
    except ValueError: raise
    except Exception: raise ValueError("INVALID_AUDIO")

class TaskPatch(BaseModel):
    model_config=ConfigDict(extra="forbid")
    text: str = Field(min_length=1)
    assignee: str | None = None
    due_date: str | None = None
    review_status: str
    @field_validator("review_status")
    @classmethod
    def valid_status(cls,v):
        if v not in {"pending","confirmed","needs_clarification"}: raise ValueError("invalid review_status")
        return v
    @field_validator("due_date")
    @classmethod
    def valid_date(cls,v):
        if v:
            datetime.strptime(v,"%Y-%m-%d")
        return v

class DecisionPatch(BaseModel):
    model_config=ConfigDict(extra="forbid")
    text: str = Field(min_length=1)
    review_status: str
    @field_validator("review_status")
    @classmethod
    def valid_status(cls,v):
        if v not in {"pending","confirmed","needs_clarification"}: raise ValueError("invalid review_status")
        return v

@app.post("/api/meetings", status_code=202)
async def create_meeting(title: str=Form(...), meeting_date: str=Form(...), participants: str=Form("[]"), consent: bool=Form(False), audio: UploadFile=File(...)):
    if not consent: return error("CONSENT_REQUIRED","Подтвердите право на обработку записи")
    try: datetime.strptime(meeting_date,"%Y-%m-%d"); people=json.loads(participants)
    except Exception: return error("INVALID_FIELD","Проверьте дату и участников")
    if not isinstance(people,list): return error("INVALID_FIELD","Участники должны быть JSON-массивом")
    ext=Path(audio.filename or "").suffix.lower()
    if ext not in ALLOWED: return error("INVALID_AUDIO","Неподдерживаемый формат записи",415)
    mid=str(uuid.uuid4()); path=STORAGE/(mid+ext)
    size=0
    try:
        with path.open("wb") as out:
            while chunk:=await audio.read(1024*1024):
                size+=len(chunk)
                if size>MAX_BYTES: path.unlink(missing_ok=True); return error("FILE_TOO_LARGE","Файл больше допустимых 250 МБ",413)
                out.write(chunk)
        try: duration,_=probe(path)
        except ValueError as e:
            path.unlink(missing_ok=True); code=str(e); return error(code,"Аудиофайл не прошёл проверку",400)
        with db() as c:
            c.execute("INSERT INTO meetings VALUES (?,?,?,?,?,?,?,?,?)",(mid,title.strip(),meeting_date,json.dumps(people,ensure_ascii=False),now(),"queued","queued",None,now()))
            c.execute("INSERT INTO jobs VALUES (?,?,?,?,?,?)",(str(uuid.uuid4()),mid,0,"queued",str(path), (datetime.now(timezone.utc)+timedelta(hours=RETENTION_HOURS)).isoformat()))
            c.execute("INSERT INTO usage VALUES (?,?,?,?)",(str(uuid.uuid4()),mid,int(duration),None))
        threading.Thread(target=process_meeting,args=(mid,path,duration),daemon=True).start()
        return {"id":mid,"status":"queued"}
    except Exception:
        path.unlink(missing_ok=True); return error("INVALID_AUDIO","Не удалось сохранить аудио",500)

def fallback_segments():
    return [{"start_ms":0,"end_ms":1000,"text":"Расшифровка будет доступна после настройки STT API."}]

def process_meeting(mid,path,duration):
    try:
        with db() as c: c.execute("UPDATE meetings SET status='processing',stage='transcription' WHERE id=?",(mid,))
        gemini_cards=None
        if os.getenv("GEMINI_API_KEY"):
            segments, gemini_cards = gemini_extract(path, get_meeting(mid)["meeting_date"])
        else:
            segments=stt(path) if os.getenv("STT_API_URL") and os.getenv("STT_API_KEY") else fallback_segments()
        if not segments: raise RuntimeError("PROCESSING_FAILED")
        ids=[]
        with db() as c:
            for s in segments:
                sid=str(uuid.uuid4()); ids.append(sid); c.execute("INSERT INTO segments VALUES (?,?,?,?,?)",(sid,mid,int(s["start_ms"]),int(s["end_ms"]),s["text"]))
            c.execute("UPDATE meetings SET stage='extraction' WHERE id=?",(mid,))
        if gemini_cards is not None:
            cards={"decisions":[],"tasks":[]}
            for key in ("decisions","tasks"):
                for card in gemini_cards.get(key,[]):
                    index=card.pop("source_segment_index", None)
                    if isinstance(index,int) and 0 <= index < len(ids): card["source_segment_id"]=ids[index]; cards[key].append(card)
        else:
            cards=llm(segments,ids,get_meeting(mid)["meeting_date"]) if os.getenv("LLM_API_URL") and os.getenv("LLM_API_KEY") else {"decisions":[],"tasks":[]}
        with db() as c:
            valid=set(ids)
            for d in cards.get("decisions",[]):
                if d.get("text") and d.get("source_segment_id") in valid: c.execute("INSERT INTO decisions VALUES (?,?,?,?,?)",(str(uuid.uuid4()),mid,d["text"],d["source_segment_id"],"pending"))
            for t in cards.get("tasks",[]):
                if t.get("text") and t.get("source_segment_id") in valid: c.execute("INSERT INTO tasks VALUES (?,?,?,?,?,?,?)",(str(uuid.uuid4()),mid,t["text"],t.get("assignee"),t.get("due_date"),t["source_segment_id"],"pending"))
            c.execute("UPDATE meetings SET status='ready',stage='ready' WHERE id=?",(mid,))
    except Exception as exc:
        if isinstance(exc, httpx.HTTPStatusError):
            logger.error("Gemini/API request failed: status=%s body=%s", exc.response.status_code, exc.response.text[:800])
        else:
            logger.exception("Processing failed for meeting %s: %s", mid, type(exc).__name__)
        code="API_UNAVAILABLE" if isinstance(exc, (httpx.HTTPError, json.JSONDecodeError)) else "PROCESSING_FAILED"
        with db() as c: c.execute("UPDATE meetings SET status='failed',stage='failed',error_code=? WHERE id=?",(code,mid))
    finally: path.unlink(missing_ok=True)

def stt(path):
    with httpx.Client(timeout=300) as h:
        r=h.post(os.environ["STT_API_URL"],headers={"Authorization":"Bearer "+os.environ["STT_API_KEY"]},files={"file":(path.name,path.open("rb"),"audio/wav")},data={"model":os.getenv("STT_MODEL","whisper-large-v3-turbo"),"response_format":"verbose_json"}); r.raise_for_status(); data=r.json()
    return [{"start_ms":int(x["start"]*1000),"end_ms":int(x["end"]*1000),"text":x["text"].strip()} for x in data.get("segments",[]) if x.get("text"," ").strip()]

def gemini_extract(path, meeting_date):
    """Transcribe audio and extract cards in one Gemini request."""
    mime={".wav":"audio/wav",".mp3":"audio/mpeg",".m4a":"audio/mp4"}.get(path.suffix.lower(), "audio/wav")
    encoded=base64.b64encode(path.read_bytes()).decode("ascii")
    prompt=f'''Ты обрабатываешь русскоязычную запись встречи от {meeting_date}.
Верни только валидный JSON без markdown и комментариев:
{{"segments":[{{"start_ms":0,"end_ms":1000,"text":"..."}}],"decisions":[{{"text":"...","source_segment_index":0}}],"tasks":[{{"text":"...","assignee":null,"due_date":null,"source_segment_index":0}}]}}
Разбей расшифровку на последовательные смысловые сегменты с временными метками в миллисекундах. Индекс источника начинается с 0. Не выдумывай исполнителя и срок: неизвестные значения null. Относительные сроки указывай в YYYY-MM-DD только если дата однозначна, иначе null.'''
    model=os.getenv("GEMINI_MODEL","gemini-3.5-flash-lite")
    url=f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
    payload={"contents":[{"parts":[{"text":prompt},{"inline_data":{"mime_type":mime,"data":encoded}}]}],"generationConfig":{"responseMimeType":"application/json","temperature":0.1}}
    retries=int(os.getenv("MAX_RETRIES","2"))
    with httpx.Client(timeout=300) as h:
        for attempt in range(retries+1):
            try:
                response=h.post(url,params={"key":os.environ["GEMINI_API_KEY"]},json=payload)
                response.raise_for_status()
                data=response.json()
                break
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code not in {429,500,502,503,504} or attempt >= retries:
                    raise
                time.sleep(2 ** attempt)
    text=data["candidates"][0]["content"]["parts"][0]["text"].strip()
    if text.startswith("```"): text=text.split("\n",1)[1].rsplit("```",1)[0].strip()
    result=json.loads(text)
    segments=[]
    for s in result.get("segments",[]):
        if s.get("text","").strip() and int(s.get("end_ms",0)) > int(s.get("start_ms",0)):
            segments.append({"start_ms":max(0,int(s["start_ms"])),"end_ms":int(s["end_ms"]),"text":s["text"].strip()})
    if not segments: raise RuntimeError("PROCESSING_FAILED")
    return segments,{"decisions":result.get("decisions",[]),"tasks":result.get("tasks",[])}

def llm(segments,ids,date):
    prompt={"meeting_date":date,"segments":[{"id":i,"text":s["text"],"start_ms":s["start_ms"],"end_ms":s["end_ms"]} for i,s in zip(ids,segments)]}
    with httpx.Client(timeout=300) as h:
        r=h.post(os.environ["LLM_API_URL"],headers={"Authorization":"Bearer "+os.environ["LLM_API_KEY"]},json={"model":os.getenv("LLM_MODEL",""),"messages":[{"role":"user","content":"Извлеки решения и задачи. Верни JSON {decisions:[{text,source_segment_id}],tasks:[{text,assignee,due_date,source_segment_id}]}. Не выдумывай поля. Данные: "+json.dumps(prompt,ensure_ascii=False)}],"response_format":{"type":"json_object"}}); r.raise_for_status(); raw=r.json(); content=raw.get("choices",[{}])[0].get("message",{}).get("content",raw); return json.loads(content) if isinstance(content,str) else content

@app.get("/api/meetings/{mid}")
def meeting(mid: str):
    m=require_meeting(mid)
    with db() as c:
        return {"meeting":{**row_dict(m),"participants":json.loads(m["participants"])},"segments":[row_dict(x) for x in c.execute("SELECT * FROM segments WHERE meeting_id=? ORDER BY start_ms",(mid,))],"decisions":[row_dict(x) for x in c.execute("SELECT * FROM decisions WHERE meeting_id=?",(mid,))],"tasks":[row_dict(x) for x in c.execute("SELECT * FROM tasks WHERE meeting_id=?",(mid,))]}

@app.get("/api/meetings/{mid}/status")
def status(mid: str):
    m=require_meeting(mid); return {"status":m["status"],"stage":m["stage"],"error":m["error_code"]}

def patch_error(exc):
    return error("INVALID_FIELD","Проверьте поля запроса",422,{"validation":str(exc)})

@app.patch("/api/meetings/{mid}/tasks/{tid}")
def update_task(mid: str, tid: str, body: TaskPatch):
    require_meeting(mid)
    with db() as c:
        task=c.execute("SELECT * FROM tasks WHERE id=? AND meeting_id=?",(tid,mid)).fetchone()
        if not task: return error("NOT_FOUND","Задача не найдена",404)
        c.execute("UPDATE tasks SET text=?,assignee=?,due_date=?,review_status=? WHERE id=?",(body.text,body.assignee,body.due_date,body.review_status,tid))
        return {"task":row_dict(c.execute("SELECT * FROM tasks WHERE id=?",(tid,)).fetchone())}

@app.patch("/api/meetings/{mid}/decisions/{did}")
def update_decision(mid: str, did: str, body: DecisionPatch):
    require_meeting(mid)
    with db() as c:
        d=c.execute("SELECT * FROM decisions WHERE id=? AND meeting_id=?",(did,mid)).fetchone()
        if not d: return error("NOT_FOUND","Решение не найдено",404)
        c.execute("UPDATE decisions SET text=?,review_status=? WHERE id=?",(body.text,body.review_status,did))
        return {"decision":row_dict(c.execute("SELECT * FROM decisions WHERE id=?",(did,)).fetchone())}

def source_time(ms):
    s=ms//1000; return f"{s//3600:02d}:{s%3600//60:02d}:{s%60:02d}"

@app.get("/api/meetings/{mid}/export", response_class=PlainTextResponse)
def export(mid: str):
    m=require_meeting(mid)
    if m["status"] != "ready": return error("NOT_READY","Черновик ещё не готов",409)
    with db() as c:
        seg={x["id"]:x for x in c.execute("SELECT * FROM segments WHERE meeting_id=?",(mid,))}
        decisions=list(c.execute("SELECT * FROM decisions WHERE meeting_id=?",(mid,))); tasks=list(c.execute("SELECT * FROM tasks WHERE meeting_id=?",(mid,)))
    out=[f"# {m['title']}",f"Дата: {m['meeting_date']}","","## Решения"]
    out += [f"- {d['text']} _(источник {source_time(seg[d['source_segment_id']]['start_ms'])}, статус: {d['review_status']})_" for d in decisions] or ["Нет решений."]
    out += ["","## Задачи"]
    for t in tasks:
        assignee=t["assignee"] or "не указан"; due=t["due_date"] or "не указан"; mark=" ⚠️ требует уточнения" if t["review_status"]=="needs_clarification" else ""
        out.append(f"- {t['text']} — исполнитель: {assignee}; срок: {due}; статус: {t['review_status']}; источник: {source_time(seg[t['source_segment_id']]['start_ms'])}{mark}")
    unclear=[t for t in tasks if t["review_status"]=="needs_clarification"]
    out += ["","## Требуют уточнения"] + ([f"- {t['text']}" for t in unclear] or ["Нет пунктов, требующих уточнения."])
    return PlainTextResponse("\n".join(out),media_type="text/markdown; charset=utf-8",headers={"Content-Disposition":f'attachment; filename="meeting-{mid}.md"'})

@app.delete("/api/meetings/{mid}", status_code=204)
def delete_meeting(mid: str):
    require_meeting(mid)
    with db() as c: c.execute("DELETE FROM meetings WHERE id=?",(mid,))
    return None

@app.get("/", response_class=HTMLResponse)
def index(): return FileResponse(ROOT/"static/index.html")

@app.get("/health")
def health(): return {"status": "ok"}
