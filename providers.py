"""upstream HTTP: keep-alive pool, bounded reads, kind dispatch. stdlib only."""
import http.client
import json
import threading
import urllib.parse

MAX_BODY = 10 * 1024 * 1024  # ponytail: fixed cap, per-target override when proven needed

_pool = {}
_plock = threading.Lock()


def _take(scheme, host, port, timeout):
    # -> (conn, reused). Reused conns may be half-open corpses; callers retry once.
    with _plock:
        lst = _pool.get((scheme, host, port)) or []
        while lst:
            c = lst.pop()
            try:
                c.sock.getpeername()
                return c, True
            except (OSError, AttributeError):
                try:
                    c.close()
                except OSError:
                    pass
    cls = http.client.HTTPSConnection if scheme == "https" else http.client.HTTPConnection
    return cls(host, port, timeout=timeout), False


def _close(conn):
    try:
        conn.close()
    except OSError:
        pass


def _give(scheme, host, port, conn):
    # capped: extra idle conns are closed, not hoarded (fd ceiling).
    with _plock:
        lst = _pool.setdefault((scheme, host, port), [])
        if len(lst) >= 4:
            try:
                conn.close()
            except OSError:
                pass
            return
        lst.append(conn)


def post(url, headers, body, timeout=30, max_body=MAX_BODY):
    """-> (status, lowercased headers dict, body bytes). Never raises."""
    sp = urllib.parse.urlsplit(url)
    scheme, host = sp.scheme, sp.hostname
    port = sp.port or (443 if scheme == "https" else 80)
    path = sp.path or "/"
    if sp.query:
        path += "?" + sp.query
    for _try in (0, 1):
        conn, reused = _take(scheme, host, port, timeout)
        try:
            conn.request("POST", path, body=body, headers=headers)
            r = conn.getresponse()
            h = {k.lower(): v for k, v in r.getheaders()}
            data = r.read(max_body + 1)
            if len(data) > max_body:
                _close(conn)
                return 502, {}, b'{"error":{"message":"upstream body too large"}}'
            if r.will_close:
                _close(conn)
            else:
                _give(scheme, host, port, conn)
            return r.status, h, data
        except TimeoutError:
            _close(conn)
            if reused and _try == 0:
                continue  # idle death mid-read; one fresh retry
            return 504, {}, b'{"error":{"message":"upstream timeout"}}'
        except (OSError, http.client.HTTPException):
            _close(conn)
            if reused and _try == 0:
                continue  # half-open corpse; one fresh retry
            return 503, {}, b'{"error":{"message":"upstream unreachable"}}'


def t_headers(t, base):
    """user headers/cookies first, kind auth wins, Content-Type always ours."""
    h = {}
    for k, v in (t.get("headers") or {}).items():
        if k.lower() != "content-length":
            h[k] = str(v)
    if t.get("cookies"):
        prior = h.get("Cookie", "")
        h["Cookie"] = (prior + "; " + t["cookies"]).strip("; ") if prior else t["cookies"]
    h.update(base)
    h["Content-Type"] = "application/json"
    return h


def get(url, headers, timeout=15, max_body=MAX_BODY):
    # same pool as post, GET flavor. Never raises.
    sp = urllib.parse.urlsplit(url)
    scheme, host = sp.scheme, sp.hostname
    if not host:
        return 502, {}, b'{"error":{"message":"bad url"}}'
    port = sp.port or (443 if scheme == "https" else 80)
    path = sp.path or "/"
    if sp.query:
        path += "?" + sp.query
    for _try in (0, 1):
        conn, reused = _take(scheme, host, port, timeout)
        try:
            conn.request("GET", path, headers=headers)
            r = conn.getresponse()
            h = {k.lower(): v for k, v in r.getheaders()}
            data = r.read(max_body + 1)
            if len(data) > max_body:
                _close(conn)
                return 502, {}, b'{"error":{"message":"upstream body too large"}}'
            if r.will_close:
                _close(conn)
            else:
                _give(scheme, host, port, conn)
            return r.status, h, data
        except TimeoutError:
            _close(conn)
            if reused and _try == 0:
                continue  # idle death mid-read; one fresh retry
            return 504, {}, b'{"error":{"message":"upstream timeout"}}'
        except (OSError, http.client.HTTPException):
            _close(conn)
            if reused and _try == 0:
                continue  # half-open corpse; one fresh retry
            return 503, {}, b'{"error":{"message":"upstream unreachable"}}'


def discover_models(kind, base_url, api_key, timeout=15):
    # ask the company what it serves right now. -> (ids|None, err|None)
    import json as _j
    base = (base_url or "").rstrip("/")
    if not base.startswith(("https://", "http://")):
        return None, "bad url"
    auth = {"Authorization": f"Bearer {api_key}"} if api_key and api_key != "-" else {}
    if kind in ("openai", "ollama"):
        code, _, body = get(base + "/v1/models", auth, timeout)
    elif kind == "azure":
        code, _, body = get(base + "/openai/models?api-version=2024-06-01", {"api-key": api_key or ""}, timeout)
    elif kind == "qwen-web":
        code, _, body = get(base + "/api/models",
                            {"Authorization": f"Bearer {api_key}"} if api_key and api_key != "-" else {}, timeout)
        if code == 200:
            try:
                ids = [m["id"] for m in _j.loads(body).get("data", [])
                       if "t2t" in (((m.get("info") or {}).get("meta") or {}).get("chat_type") or "")]
                return (ids[:200] or None), (None if ids else "no chat models")
            except (ValueError, AttributeError, TypeError):
                return None, "bad shape"
    elif kind == "gemini":
        import urllib.parse as _up
        code, _, body = get(base + "/v1beta/models?key=" + _up.quote(api_key or ""),
                            {"x-goog-api-key": api_key or ""}, timeout)
    else:
        return None, "no list API for this kind; type the names"
    if code != 200:
        return None, "http " + str(code)
    try:
        d = _j.loads(body)
    except ValueError:
        return None, "bad shape"
    try:
        if kind == "gemini":
            ids = [m["name"].split("/")[-1] for m in d.get("models", []) if m.get("name")]
        else:
            ids = [m["id"] for m in d.get("data", []) if m.get("id")]
    except (AttributeError, TypeError):
        return None, "bad shape"
    return ids[:200], None


def forward(base_url, api_key, path, payload, timeout=30, extra=None):
    url = base_url.rstrip("/") + path
    auth = {"Authorization": f"Bearer {api_key}"} if api_key and api_key != "-" else {}
    return post(url, t_headers(extra or {}, auth), json.dumps(payload).encode(), timeout)[:3]




def valid_chat(body):
    """True if body is OpenAI-shape chat with usable text. Rejects HTML/error pages."""
    try:
        import json as _j
        ch = _j.loads(body).get("choices")
        if not ch:
            return False
        msg = ch[0].get("message", {})
        return isinstance(msg, dict) and (
            isinstance(msg.get("content"), (str, list))
            or isinstance(msg.get("tool_calls"), list)
            or isinstance(msg.get("reasoning"), str)
            or isinstance(msg.get("reasoning_content"), str)
        )
    except (ValueError, AttributeError, IndexError, TypeError):
        return False


def probe_ok(body):
    """Looser: any parsed choices+message (probe uses max_tokens 1, content may be empty)."""
    try:
        import json as _j
        ch = _j.loads(body).get("choices")
        return bool(ch) and isinstance(ch[0].get("message"), dict)
    except (ValueError, AttributeError, IndexError, TypeError):
        return False


def to_anthropic(payload):
    """OpenAI chat -> Anthropic messages. ponytail: text-only, images when proven needed."""
    system = "\n".join(m.get("content", "") for m in payload.get("messages", []) if m.get("role") == "system")
    msgs = [{"role": m["role"], "content": m.get("content") or ""} for m in payload.get("messages", []) if m.get("role") in ("user", "assistant")]
    body = {"model": payload.get("model", ""), "max_tokens": payload.get("max_tokens", 1024), "messages": msgs}
    if system:
        body["system"] = system
    return body


def from_anthropic(body):
    """Anthropic response -> OpenAI chat shape."""
    try:
        d = json.loads(body) if isinstance(body, (bytes, bytearray)) else body
        text = "".join(b.get("text", "") for b in d.get("content", []) if b.get("type") == "text")
        return {"choices": [{"message": {"role": "assistant", "content": text}, "finish_reason": d.get("stop_reason", "stop")}]}
    except (ValueError, AttributeError, TypeError):
        return {"choices": []}


def to_gemini(payload):
    # OpenAI chat -> Gemini contents. Assistant turns kept as model; same-role runs merged.
    contents = []
    for m in payload.get("messages", []) or []:
        role = "model" if m.get("role") == "assistant" else "user"
        part = {"text": m.get("content") or ""}
        if contents and contents[-1]["role"] == role:
            contents[-1]["parts"].append(part)
        else:
            contents.append({"role": role, "parts": [part]})
    if not contents or contents[0]["role"] != "user":
        contents.insert(0, {"role": "user", "parts": [{"text": ""}]})
    return {"contents": contents}


def from_gemini(body):
    """Gemini response -> OpenAI chat shape. Never raises; [] means unparseable."""
    try:
        d = json.loads(body) if isinstance(body, (bytes, bytearray)) else body
        text = "".join(p.get("text", "") for p in d["candidates"][0]["content"]["parts"])
    except (KeyError, IndexError, ValueError, AttributeError, TypeError):
        return {"choices": []}
    return {"choices": [{"message": {"role": "assistant", "content": text}, "finish_reason": "stop"}]}


def _oauth_token(t, force=False):
    import oauth as _o
    return _o.valid_token(t.get("name", ""), force=force)


def _call_claude_oauth(t, payload, timeout=30):
    # Claude subscription via public CLI OAuth (mirrors claudeIdentity beta contract).
    token = _oauth_token(t)
    if not token:
        return 401, {}, b'{"error":{"message":"claude oauth not connected"}}'
    betas = ["oauth-2025-04-20"]
    if isinstance(payload.get("tools"), list) and payload["tools"]:
        betas.insert(0, "claude-code-20250219")
    def _do(tok):
        return post((t.get("base_url") or "https://api.anthropic.com").rstrip("/") + "/v1/messages",
                    t_headers(t, {"Authorization": "Bearer " + tok,
                                  "anthropic-version": "2023-06-01",
                                  "anthropic-beta": ",".join(betas)}),
                    json.dumps(to_anthropic(payload)).encode(), timeout)
    code, h, body = _do(token)
    if code == 401 and _oauth_token(t, force=True):
        code, h, body = _do(_oauth_token(t))
    if code == 200:
        body = json.dumps(from_anthropic(body)).encode()
    return code, h, body


def responses_text(body):
    # Responses-API SSE -> (text|None, err|None). Accumulates output_text deltas.
    import json as _j
    raw = body.decode("utf-8", "replace") if isinstance(body, (bytes, bytearray)) else ""
    text = []
    for ev in raw.split("\n\n"):
        line = next((ln for ln in ev.split("\n") if ln.startswith("data:")), "")
        d = line[5:].strip()
        if not d or d == "[DONE]":
            continue
        try:
            o = _j.loads(d)
        except ValueError:
            continue
        if o.get("type") == "response.output_text.delta" and isinstance(o.get("delta"), str):
            text.append(o["delta"])
        if o.get("type") == "response.failed":
            err = ((o.get("response") or {}).get("error") or {}).get("message")
            return None, err or "response failed"
    out = "".join(text)
    return (out, None) if out else (None, "empty reply")


def _call_codex(t, payload, timeout=30):
    # ChatGPT subscription via Codex device OAuth -> backend-api Responses face.
    import json as _j
    token = _oauth_token(t)
    if not token:
        return 401, {}, b'{"error":{"message":"codex oauth not connected"}}'
    items = []
    instructions = ""
    for m in payload.get("messages", []) or []:
        if not isinstance(m, dict):
            continue
        c = m.get("content") or ""
        if m.get("role") == "system" and isinstance(c, str):
            instructions += c
            continue
        if m.get("role") in ("user", "assistant") and isinstance(c, str) and c:
            items.append({"role": m["role"], "content": [{"type": "input_text", "text": c}]})
    if not items:
        return 400, {}, b'{"error":{"message":"empty prompt"}}'
    req = {"model": payload.get("model", ""), "input": items, "stream": True, "store": False}
    if instructions:
        req["instructions"] = instructions
    def _do(tok):
        return post_stream((t.get("base_url") or "https://chatgpt.com/backend-api/codex/responses").rstrip("/"),
                           t_headers(t, {"Authorization": "Bearer " + tok,
                                         "Origin": "https://chatgpt.com",
                                         "Referer": "https://chatgpt.com/"}),
                           _j.dumps(req).encode(), timeout)
    code, h, chunks = _do(token)
    if code == 401 and _oauth_token(t, force=True):
        code, h, chunks = _do(_oauth_token(t))
    if code != 200:
        return code, h, chunks
    data = b"".join(chunks)
    out, err = responses_text(data)
    if err:
        return 502, h, ('{"error":{"message":"codex: %s"}}' % err).encode()
    return 200, h, _j.dumps({"choices": [{"message": {"role": "assistant", "content": out}, "finish_reason": "stop"}]}).encode()


QWEN_UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"


def _qwen_midtoken(timeout=10):
    # bx-umidtoken for WAF; best-effort (None = proceed without, WAF decides).
    import re
    try:
        code, _, data = get("https://sg-wum.alibaba.com/w/wu.json", {}, timeout)
        if code != 200:
            return None
        m = re.search(r"(?:umx\.wu|__fycb)\('([^']+)'\)", data.decode("utf-8", "replace"))
        return m.group(1) if m else None
    except Exception:
        return None


def qwen_headers(t, mid=None):
    import uuid as _u
    h = {"User-Agent": QWEN_UA, "Accept": "*/*", "Accept-Language": "en-US,en;q=0.5",
         "Origin": "https://chat.qwen.ai", "Referer": "https://chat.qwen.ai/",
         "Content-Type": "application/json", "Sec-Fetch-Dest": "empty", "Sec-Fetch-Mode": "cors",
         "Sec-Fetch-Site": "same-origin", "X-Requested-With": "XMLHttpRequest",
         "source": "web", "version": "0.2.66", "X-Accel-Buffering": "no",
         "x-request-id": str(_u.uuid4())}
    key = (t.get("api_key") or "").strip()
    if key and key != "-":
        h["Authorization"] = "Bearer " + key
        h["Cookie"] = "token=" + key
    if mid:
        h["bx-umidtoken"] = mid
        h["bx-v"] = "2.5.36"
    return t_headers(t, h)


def qwen_text(body):
    # SSE choices[].delta: answer phase content accumulated; think phase skipped.
    import json as _j
    raw = body.decode("utf-8", "replace") if isinstance(body, (bytes, bytearray)) else ""
    text = []
    for ev in raw.split("\n\n"):
        line = next((ln for ln in ev.split("\n") if ln.startswith("data:")), "")
        d = line[5:].strip()
        if not d or d == "[DONE]":
            continue
        try:
            o = _j.loads(d)
        except ValueError:
            continue
        if o.get("error"):
            e = o["error"]
            return None, (e.get("details") or e.get("code") or "qwen error") if isinstance(e, dict) else str(e)
        for ch in o.get("choices", []) or []:
            delta = ch.get("delta") or {}
            if delta.get("phase") == "answer" and isinstance(delta.get("content"), str):
                text.append(delta["content"])
            elif not delta.get("phase") and isinstance(delta.get("content"), str):
                text.append(delta["content"])
    out = "".join(text)
    return (out, None) if out else (None, "empty reply")


def _call_qwen(t, payload, timeout=30):
    # chat.qwen.ai v2 flow: chats/new -> completions?chat_id= (single-shot, fresh chat).
    import json as _j
    import time as _t
    import uuid as _u
    base = (t.get("base_url") or "https://chat.qwen.ai").rstrip("/")
    model = payload.get("model", "")
    user_text, system = "", ""
    for m in payload.get("messages", []) or []:
        if not isinstance(m, dict):
            continue
        c = m.get("content")
        s = c if isinstance(c, str) else ""
        if m.get("role") == "system" and s:
            system += s
        elif m.get("role") == "user" and s:
            user_text = s
    prompt = ((system + "\n\n" + user_text) if system else user_text).strip()
    if not prompt:
        return 400, {}, b'{"error":{"message":"empty prompt"}}'
    mid = _qwen_midtoken()
    h = qwen_headers(t, mid)
    now = int(_t.time() * 1000)
    code, _, data = post(base + "/api/v2/chats/new",
                         dict(h, Referer="https://chat.qwen.ai/"),
                         _j.dumps({"title": "New Chat", "models": [model], "chat_mode": "normal",
                                   "chat_type": "t2t", "timestamp": now, "project_id": ""}).encode(), timeout)
    if code == 401:
        return 401, {}, b'{"error":{"message":"qwen token expired"}}'
    if code != 200:
        return code, {}, data
    try:
        chat_id = _j.loads(data)["data"]["id"]
    except (ValueError, KeyError, TypeError):
        return 502, {}, b'{"error":{"message":"qwen: no chat id"}}'
    fid = str(_u.uuid4())
    msg = {"fid": fid, "parentId": None, "childrenIds": [], "role": "user", "content": prompt,
           "user_action": "chat", "files": [], "timestamp": now, "models": [model], "chat_type": "t2t",
           "feature_config": {"thinking_enabled": False, "output_schema": "phase", "thinking_budget": 81920},
           "extra": {"meta": {"subChatType": "t2t"}}, "sub_chat_type": "t2t"}
    req = {"stream": True, "version": "2.1", "incremental_output": True, "chat_id": chat_id,
           "chat_mode": "normal", "model": model, "parent_id": None, "messages": [msg], "timestamp": now}
    code, hdrs, sdata = post_stream(base + "/api/v2/chat/completions?chat_id=" + chat_id, h,
                                    _j.dumps(req).encode(), timeout)
    if code != 200:
        return code, hdrs, sdata
    out, err = qwen_text(b"".join(sdata))
    if err:
        return 502, hdrs, ('{"error":{"message":"qwen: %s"}}' % err).encode()
    return 200, hdrs, _j.dumps({"choices": [{"message": {"role": "assistant", "content": out}, "finish_reason": "stop"}]}).encode()


def _call_grok(t, payload, timeout=30):
    # grok.com cookie flow (mirrors open-sse/executors/grok-web.ts minus TLS impersonation).
    import json as _j
    import uuid as _u
    cookie = grok_cookie(t.get("api_key", "")) or t.get("cookies", "")
    if not cookie:
        return 401, {}, b'{"error":{"message":"grok sso cookie missing"}}'
    mode = GROK_MODES.get(payload.get("model", ""), "fast")
    text = web_transcript(payload.get("messages", []))
    if not text.strip():
        return 400, {}, b'{"error":{"message":"empty prompt"}}'
    body = _j.dumps({"temporary": True, "modeId": mode, "message": text, "fileAttachments": [],
                     "imageAttachments": [], "disableSearch": False, "toolOverrides": {},
                     "isReasoning": False, "disableMemory": True, "sendFinalMetadata": True}).encode()
    h = t_headers(t, {"Origin": "https://grok.com", "Referer": "https://grok.com/",
                      "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36",
                      "x-xai-request-id": str(_u.uuid4()), "Cookie": cookie})
    code, hdrs, data = post("https://grok.com/rest/app-chat/conversations/new", h, body, timeout)
    if code == 401:
        return 401, hdrs, b'{"error":{"message":"grok sso cookie expired"}}'
    if code != 200:
        return code, hdrs, data
    out, err = grok_text(data)
    if err:
        return 502, hdrs, ('{"error":{"message":"grok: %s"}}' % err).encode()
    return 200, hdrs, _j.dumps({"choices": [{"message": {"role": "assistant", "content": out}, "finish_reason": "stop"}]}).encode()


def _call_claude(t, payload, timeout=30):
    # claude.ai cookie flow: organizations -> fresh conversation completion (SSE).
    import json as _j
    import urllib.parse as _up
    import uuid as _u
    cookie = claude_cookie(t.get("api_key", "")) or t.get("cookies", "")
    if not cookie:
        return 401, {}, b'{"error":{"message":"claude sessionKey cookie missing"}}'
    h = t_headers(t, {"Origin": "https://claude.ai", "Referer": "https://claude.ai/new",
                      "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36",
                      "Accept": "text/event-stream", "anthropic-client-platform": "web_claude_ai",
                      "Cookie": cookie})
    code, _, org_body = get("https://claude.ai/api/organizations", h, timeout)
    if code == 401:
        return 401, {}, b'{"error":{"message":"claude sessionKey expired"}}'
    if code != 200:
        return code, {}, org_body
    try:
        orgs = _j.loads(org_body)
        org = orgs[0].get("uuid") or orgs[0].get("id")
    except (ValueError, IndexError, AttributeError, TypeError):
        org = None
    if not org:
        return 502, {}, b'{"error":{"message":"claude: no organization"}}'
    text = web_transcript(payload.get("messages", []))
    if not text.strip():
        return 400, {}, b'{"error":{"message":"empty prompt"}}'
    model = payload.get("model", "")
    body = _j.dumps({"prompt": text, "model": model, "timezone": "UTC", "locale": "en-US",
                     "turn_message_uuids": {"human_message_uuid": str(_u.uuid4()),
                                            "assistant_message_uuid": str(_u.uuid4())},
                     "attachments": [], "files": [], "rendering_mode": "messages",
                     "create_conversation_params": {"name": "", "model": model}}).encode()
    url = ("https://claude.ai/api/organizations/" + _up.quote(org) + "/chat_conversations/"
           + str(_u.uuid4()) + "/completion")
    code, hdrs, data = post(url, h, body, timeout)
    if code != 200:
        return code, hdrs, data
    out, err = claude_text(data)
    if err:
        return 502, hdrs, ('{"error":{"message":"claude: %s"}}' % err).encode()
    return 200, hdrs, _j.dumps({"choices": [{"message": {"role": "assistant", "content": out}, "finish_reason": "stop"}]}).encode()


def _call_raw(t, payload, timeout=30):
    """kind-aware dispatch -> (status, headers, body). kinds: openai | ollama | anthropic | gemini | azure | grok-web | claude-web."""
    kind = t.get("kind", "openai")
    if kind == "grok-web":
        return _call_grok(t, payload, timeout)
    if kind == "qwen-web":
        return _call_qwen(t, payload, timeout)
    if kind == "claude-oauth":
        return _call_claude_oauth(t, payload, timeout)
    if kind == "codex-oauth":
        return _call_codex(t, payload, timeout)
    if kind == "claude-web":
        return _call_claude(t, payload, timeout)
    if kind == "anthropic":
        code, h, body = post(t["base_url"].rstrip("/") + "/v1/messages",
            t_headers(t, {"x-api-key": t["api_key"], "anthropic-version": "2023-06-01"}),
            json.dumps(to_anthropic(payload)).encode(), timeout)
        if code == 200:
            body = json.dumps(from_anthropic(body)).encode()
        return code, h, body
    if kind == "gemini":
        code, h, body = forward(t["base_url"], t["api_key"],
            "/v1beta/models/" + payload.get("model", "") + ":generateContent", to_gemini(payload), timeout, extra=t)
        if code == 200:
            body = json.dumps(from_gemini(body)).encode()
        return code, h, body
    if kind == "azure":
        ver = t.get("api_version", "2024-06-01")
        code, h, body = post(t["base_url"].rstrip("/") + "/openai/deployments/" + payload.get("model", "") + "/chat/completions?api-version=" + ver,
            t_headers(t, {"api-key": t["api_key"]}),
            json.dumps(payload).encode(), timeout)
        return code, h, body
    # openai + ollama (ollama: api_key empty/"-" omits auth)
    return forward(t["base_url"], t.get("api_key", "-"), "/v1/chat/completions", payload, timeout, extra=t)


def post_stream(url, headers, body, timeout=30, chunk=65536):
    # -> (200, headers, chunk-iterator) or (errcode, headers, error-bytes). Never raises.
    import urllib.parse as _up
    import http.client as _hc
    sp = _up.urlsplit(url)
    scheme, host = sp.scheme, sp.hostname
    port = sp.port or (443 if scheme == "https" else 80)
    path = sp.path or "/"
    if sp.query:
        path += "?" + sp.query
    r = None
    for _try in (0, 1):
        conn, reused = _take(scheme, host, port, timeout)
        try:
            conn.request("POST", path, body=body, headers=headers)
            r = conn.getresponse()
            break
        except (OSError, _hc.HTTPException):
            _close(conn)
            if reused and _try == 0:
                continue  # half-open corpse; one fresh retry
            return 503, {}, b'{"error":{"message":"upstream unreachable"}}'
    try:
        h = {k.lower(): v for k, v in r.getheaders()}
        if r.status != 200:
            data = r.read(MAX_BODY + 1)
            _close(conn)
            return r.status, h, data

        def gen():
            try:
                while True:
                    b = r.read(chunk)
                    if not b:
                        break
                    yield b
            finally:
                _close(conn)
        return r.status, h, gen()
    except TimeoutError:
        _close(conn)
        return 504, {}, b'{"error":{"message":"upstream timeout"}}'
    except (OSError, _hc.HTTPException):
        _close(conn)
        return 503, {}, b'{"error":{"message":"upstream unreachable"}}'


def stream_target(t, payload, timeout=30):
    """openai-shape SSE relay -> (status, headers, iterator|bytes)."""
    kind = t.get("kind", "openai")
    if kind == "azure":
        ver = t.get("api_version", "2024-06-01")
        return post_stream(t["base_url"].rstrip("/") + "/openai/deployments/" + payload.get("model", "") + "/chat/completions?api-version=" + ver,
            t_headers(t, {"api-key": t["api_key"]}),
            json.dumps({**payload, "stream": True}).encode(), timeout)
    return post_stream(t["base_url"].rstrip("/") + "/v1/chat/completions",
        t_headers(t, dict({"Authorization": f"Bearer {t['api_key']}"} if t.get("api_key") and t.get("api_key") != "-" else {})),
        json.dumps({**payload, "stream": True}).encode(), timeout)


def valid_embed(body):
    # True if body carries a non-empty embedding vector.
    try:
        import json as _j
        d = _j.loads(body).get("data")
        return bool(d) and isinstance(d[0].get("embedding"), list) and bool(d[0]["embedding"])
    except (ValueError, AttributeError, IndexError, TypeError):
        return False


def embed_target(t, payload, timeout=30):
    """embeddings -> (status, headers, body). gemini: str input only."""
    kind = t.get("kind", "openai")
    if kind == "azure":
        ver = t.get("api_version", "2024-06-01")
        code, h, body = post(t["base_url"].rstrip("/") + "/openai/deployments/" + payload.get("model", "") + "/embeddings?api-version=" + ver,
            t_headers(t, {"api-key": t["api_key"]}),
            json.dumps(payload).encode(), timeout)
        return _gate_embed(code, h, body)
    if kind == "gemini":
        text = payload.get("input", "")
        if not isinstance(text, str):
            return 400, {}, b'{"error":{"message":"gemini kind takes str input; use openai-kind target for lists"}}'
        code, h, body = post(t["base_url"].rstrip("/") + "/v1beta/models/" + payload.get("model", "") + ":embedContent",
            t_headers(t, {"x-goog-api-key": t["api_key"]}),
            json.dumps({"content": {"parts": [{"text": text}]}}).encode(), timeout)
        if code == 200:
            try:
                vec = json.loads(body)["embedding"]["values"]
            except (KeyError, ValueError, TypeError, AttributeError):
                vec = []
            body = json.dumps({"data": [{"embedding": vec, "index": 0}], "model": payload.get("model", "")}).encode()
        return _gate_embed(code, h, body)
    code, h, body = forward(t["base_url"], t.get("api_key", "-"), "/v1/embeddings", payload, timeout, extra=t)
    return _gate_embed(code, h, body)


def _gate_embed(code, h, body):
    if code == 200 and not valid_embed(body):
        return 502, h, b'{"error":{"message":"upstream bad shape"}}'
    return code, h, body


GROK_MODES = {"fast": "fast", "expert": "expert", "heavy": "heavy",
              "grok-420-computer-use-sa": "grok-420-computer-use-sa",
              "grok-4": "fast", "grok-4.1-fast": "fast", "grok-4.1-expert": "expert",
              "grok-4-heavy": "heavy", "grok-4.20": "expert", "grok-4.20-heavy": "heavy",
              "grok-4.3": "grok-420-computer-use-sa"}
# mode map mirrors open-sse/executors/grok-web.ts MODEL_MAP (UI modes, not public IDs)


def _cookie_val(raw, name):
    import re
    m = re.search(r"(?:^|;\s*)" + re.escape(name) + r"\s*=\s*([^;]+)", raw or "")
    return m.group(1).strip() if m else ""


def grok_cookie(raw):
    # bare value, sso= pair, or full DevTools blob -> minimal sso[; sso-rw][; cf] header
    raw = (raw or "").strip()
    sso = _cookie_val(raw, "sso") or (raw if raw and "=" not in raw and ";" not in raw else "")
    if not sso:
        return ""
    parts = ["sso=" + sso]
    for n in ("sso-rw", "cf_clearance", "__cf_bm"):
        v = _cookie_val(raw, n)
        if v:
            parts.append(n + "=" + v)
    return "; ".join(parts)


def claude_cookie(raw):
    # sessionKey pair, bare value, or full blob passthrough
    raw = (raw or "").strip()
    v = _cookie_val(raw, "sessionKey")
    if v:
        return "sessionKey=" + v
    if raw and "=" not in raw and ";" not in raw:
        return "sessionKey=" + raw
    return raw


def web_transcript(messages):
    # plain-text transcript (mirrors buildPromptFromMessages: user/tool turns joined)
    parts = []
    for m in messages or []:
        if not isinstance(m, dict):
            continue
        c = m.get("content")
        text = c if isinstance(c, str) else ""
        if text and m.get("role") in ("system", "user", "tool"):
            parts.append(text)
    return "\n\n".join(parts)


def grok_text(body):
    # NDJSON result.response.token deltas; modelResponse.message wins when present.
    # -> (text|None, err|None)
    import json as _j
    text, final = [], None
    try:
        lines = body.decode("utf-8", "replace").splitlines() if isinstance(body, (bytes, bytearray)) else []
    except Exception:
        return None, "unreadable"
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            d = _j.loads(line)
        except ValueError:
            continue
        r = ((d.get("result") or {}).get("response") or {}) if isinstance(d, dict) else {}
        if not isinstance(r, dict):
            continue
        if r.get("error"):
            e = r["error"]
            return None, e.get("message") if isinstance(e, dict) else str(e)
        tok = r.get("token")
        if isinstance(tok, str):
            text.append(tok)
        mr = r.get("modelResponse") or {}
        if isinstance(mr.get("message"), str) and mr["message"]:
            final = mr["message"]
    out = final if final else "".join(text)
    return (out, None) if out else (None, "empty reply")


def claude_text(body):
    # SSE content_block_delta text_delta aggregation.
    import json as _j
    raw = body.decode("utf-8", "replace") if isinstance(body, (bytes, bytearray)) else ""
    text = []
    for ev in raw.split("\n\n"):
        line = next((ln for ln in ev.split("\n") if ln.startswith("data:")), "")
        d = line[5:].strip()
        if not d or d == "[DONE]":
            continue
        try:
            o = _j.loads(d)
        except ValueError:
            continue
        delta = o.get("delta") or {}
        if delta.get("type") == "text_delta" and isinstance(delta.get("text"), str):
            text.append(delta["text"])
    out = "".join(text)
    return (out, None) if out else (None, "empty reply")


def challenge_hint(code, body):
    # Cloudflare/bot-wall fingerprint: honest reason instead of a bare 403.
    if code == 403 and isinstance(body, (bytes, bytearray)):
        low = body[:4000].lower()
        if b"cloudflare" in low or b"just a moment" in low or b"attention required" in low:
            return "blocked, needs browser"
    return None


def call_target(t, payload, timeout=30):
    # shape gate: HTTP 200 with a non-chat body becomes 502 (failover).
    code, h, body = _call_raw(t, payload, timeout)
    if code == 200 and not valid_chat(body):
        return 502, h, b'{"error":{"message":"upstream bad shape"}}'
    return code, h, body
