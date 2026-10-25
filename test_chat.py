import json, unittest
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
import store, router
from lite import Handler


class Up(BaseHTTPRequestHandler):
    def do_POST(self):
        n = int(self.headers.get("Content-Length", 0))
        self.rfile.read(n)
        out = json.dumps({"choices": [{"message": {"content": "hi"}}]}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(out)))
        self.end_headers()
        self.wfile.write(out)

    def log_message(self, *a):
        pass


class T(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.up = ThreadingHTTPServer(("127.0.0.1", 0), Up)
        cls.uport = cls.up.server_address[1]
        Thread(target=cls.up.serve_forever, daemon=True).start()
        cls.srv = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.port = cls.srv.server_address[1]
        Thread(target=cls.srv.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls):
        cls.up.shutdown(); cls.srv.shutdown()

    def post(self, payload):
        c = HTTPConnection("127.0.0.1", self.port, timeout=5)
        c.request("POST", "/v1/chat/completions", json.dumps(payload), {"Content-Type": "application/json"})
        r = c.getresponse()
        return r.status, json.loads(r.read())

    def test_no_targets_503(self):
        orig = store.load_targets
        store.load_targets = lambda *a: []
        try:
            code, body = self.post({"model": "x", "messages": []})
            self.assertIn(code, (503, 502))
        finally:
            store.load_targets = orig

    def test_passthrough(self):
        orig = store.load_targets
        store.load_targets = lambda *a: [{"name": "t", "base_url": f"http://127.0.0.1:{self.uport}/v1", "api_key": "k", "models": ["m"]}]
        try:
            code, body = self.post({"model": "m", "messages": [{"role": "user", "content": "hi"}]})
            self.assertEqual(code, 200)
            self.assertEqual(body["choices"][0]["message"]["content"], "hi")
        finally:
            store.load_targets = orig


if __name__ == "__main__":
    unittest.main()


class TStream(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.srv = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.port = cls.srv.server_address[1]
        import threading
        threading.Thread(target=cls.srv.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls):
        cls.srv.shutdown()

    def test_stream_bytes(self):
        import store as _s
        orig = _s.load_targets
        _s.load_targets = lambda *a: [{"name": "u", "kind": "anthropic", "base_url": "http://127.0.0.1:1/v1", "api_key": "k", "models": ["m"]}]
        import providers as _p
        o = _p.call_target
        _p.call_target = lambda t, p, timeout=30: (200, {}, json.dumps({"choices": [{"message": {"content": "hello"}}]}).encode())
        try:
            c = HTTPConnection("127.0.0.1", self.port, timeout=5)
            c.request("POST", "/v1/chat/completions", json.dumps({"model": "m", "messages": [], "stream": True}), {"Content-Type": "application/json"})
            r = c.getresponse()
            body = r.read().decode()
            self.assertEqual(r.status, 200)
            self.assertIn("text/event-stream", r.getheader("Content-Type"))
            self.assertIn("data: [DONE]", body)
            self.assertIn("hello", body)
        finally:
            _s.load_targets = orig; _p.call_target = o

    def test_stream_shape(self):
        import store as _s
        orig = _s.load_targets
        _s.load_targets = lambda *a: []
        try:
            c = HTTPConnection("127.0.0.1", self.port, timeout=5)
            c.request("POST", "/v1/chat/completions", json.dumps({"model": "x", "messages": [], "stream": True}), {"Content-Type": "application/json"})
            r = c.getresponse()
            self.assertEqual(r.status, 503)  # no targets -> no SSE, proves wiring point
        finally:
            _s.load_targets = orig


class TCacheHit(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.srv = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.port = cls.srv.server_address[1]
        import threading
        threading.Thread(target=cls.srv.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls):
        cls.srv.shutdown()

    def test_cache_hit_skips_upstream(self):
        import store as _s, providers as _p, tempfile, os
        tmp = os.path.join(tempfile.mkdtemp(), "c.db")
        orig_home = os.path.expanduser
        os.path.expanduser = lambda p: tmp if p == "~" else orig_home(p)
        calls = []
        o_call = _p.call_target
        _p.call_target = lambda t, p, timeout=30: (calls.append(1) or (200, {}, b'{"choices":[{"message":{"content":"cached"}}]}'))
        o_load = _s.load_targets
        _s.load_targets = lambda *a: [{"name": "u", "base_url": "http://x/v1", "api_key": "k", "models": ["m"]}]
        try:
            body = json.dumps({"model": "m", "messages": [{"role": "user", "content": "hi"}], "cache_ttl": 60})
            for _ in range(2):
                c = HTTPConnection("127.0.0.1", self.port, timeout=5)
                c.request("POST", "/v1/chat/completions", body, {"Content-Type": "application/json"})
                self.assertEqual(c.getresponse().status, 200)
            self.assertEqual(len(calls), 1)
        finally:
            os.path.expanduser = orig_home; _p.call_target = o_call; _s.load_targets = o_load


class TFailover(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.srv = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.port = cls.srv.server_address[1]
        import threading
        threading.Thread(target=cls.srv.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls):
        cls.srv.shutdown()

    def setUp(self):
        router._reset()

    def _post(self, payload, targets, by_name):
        import store as _s, providers as _p
        o_load, o_call = _s.load_targets, _p.call_target
        _s.load_targets = lambda *a: targets
        _p.call_target = lambda t, p, timeout=30: by_name[t["name"]]
        try:
            c = HTTPConnection("127.0.0.1", self.port, timeout=5)
            c.request("POST", "/v1/chat/completions", json.dumps(payload), {"Content-Type": "application/json"})
            r = c.getresponse()
            return r.status, r.read()
        finally:
            _s.load_targets, _p.call_target = o_load, o_call

    def test_401_fails_over_to_next_key(self):
        ts = [{"name": "a", "models": ["m"]}, {"name": "b", "models": ["m"]}]
        code, body = self._post({"model": "m", "messages": []}, ts,
            {"a": (401, {}, b"nope"), "b": (200, {}, b'{"choices":[]}')} )
        self.assertEqual(code, 200)
        self.assertTrue(router.is_cool("a"))

    def test_404_locks_model_only(self):
        ts = [{"name": "a", "models": ["m"]}, {"name": "b", "models": ["m"]}]
        code, _ = self._post({"model": "m", "messages": []}, ts,
            {"a": (404, {}, b"nope"), "b": (200, {}, b'{"choices":[]}')})
        self.assertEqual(code, 200)
        self.assertTrue(router.model_locked("a", "m"))
        self.assertFalse(router.is_cool("a"))  # key itself still usable


class TPassthrough(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from http.server import BaseHTTPRequestHandler

        class SSEUp(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_POST(self):
                n = int(self.headers.get("Content-Length", 0))
                self.rfile.read(n)
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Content-Length", str(len(b"data: 1\n\ndata: 2\n\ndata: [DONE]\n\n")))
                self.end_headers()
                for part in (b"data: 1\n\n", b"data: 2\n\n", b"data: [DONE]\n\n"):
                    self.wfile.write(part)
                    self.wfile.flush()

            def log_message(self, *a):
                pass

        cls.up = ThreadingHTTPServer(("127.0.0.1", 0), SSEUp)
        cls.uport = cls.up.server_address[1]
        cls.srv = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.port = cls.srv.server_address[1]
        import threading
        threading.Thread(target=cls.up.serve_forever, daemon=True).start()
        threading.Thread(target=cls.srv.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls):
        cls.up.shutdown(); cls.srv.shutdown()

    def test_relay_order_and_done(self):
        import store as _s
        o = _s.load_targets
        _s.load_targets = lambda *a: [{"name": "u", "kind": "openai", "base_url": f"http://127.0.0.1:{self.uport}/v1", "api_key": "k", "models": ["m"]}]
        try:
            c = HTTPConnection("127.0.0.1", self.port, timeout=5)
            c.request("POST", "/v1/chat/completions", json.dumps({"model": "m", "messages": [], "stream": True}), {"Content-Type": "application/json"})
            r = c.getresponse()
            body = r.read().decode()
            self.assertEqual(r.status, 200)
            self.assertEqual(body, "data: 1\n\ndata: 2\n\ndata: [DONE]\n\n")
        finally:
            _s.load_targets = o


class TChain(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.srv = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.port = cls.srv.server_address[1]
        import threading
        threading.Thread(target=cls.srv.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls):
        cls.srv.shutdown()

    def setUp(self):
        router._reset()

    def _post(self, payload, targets, chains, by_name):
        import store as _s, providers as _p
        o = (_s.load_targets, _s.load_chains, _p.call_target)
        _s.load_targets = lambda *a: targets
        _s.load_chains = lambda: chains
        seen = []
        def fake(t, p, timeout=30):
            seen.append((t["name"], p.get("model")))
            return by_name[t["name"]]
        _p.call_target = fake
        try:
            c = HTTPConnection("127.0.0.1", self.port, timeout=5)
            c.request("POST", "/v1/chat/completions", json.dumps(payload), {"Content-Type": "application/json"})
            r = c.getresponse()
            return r.status, r.read(), seen
        finally:
            _s.load_targets, _s.load_chains, _p.call_target = o

    def test_chain_order_and_model_rewrite(self):
        ts = [{"name": "a", "models": ["x"]}, {"name": "b", "models": ["y"]}]
        code, _, seen = self._post({"model": "chain:c", "messages": []}, ts, [{"name": "c", "members": ["b", "a"]}],
            {"a": (200, {}, b'{"choices":[]}'), "b": (500, {}, b"err")})
        self.assertEqual(code, 200)
        self.assertEqual(seen, [("b", "y"), ("a", "x")])  # declared order, real models upstream

    def test_unknown_chain_404(self):
        code, _, _ = self._post({"model": "chain:nope", "messages": []}, [], [], {})
        self.assertEqual(code, 404)


class TBadShape(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.srv = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.port = cls.srv.server_address[1]
        import threading
        threading.Thread(target=cls.srv.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls):
        cls.srv.shutdown()

    def setUp(self):
        router._reset()

    def test_html_200_fails_over(self):
        import store as _s, providers as _p
        o = (_s.load_targets, _s.load_chains, _p._call_raw)
        _s.load_targets = lambda *a: [{"name": "web", "models": ["m"]}, {"name": "real", "models": ["m"]}]
        _s.load_chains = lambda: []
        seen = []

        def fake(t, p, timeout=30):
            seen.append(t["name"])
            if t["name"] == "web":
                return 200, {}, b"<html>login page</html>"
            return 200, {}, b'{"choices":[{"message":{"content":"hi"}}]}'

        _p._call_raw = fake
        try:
            c = HTTPConnection("127.0.0.1", self.port, timeout=5)
            c.request("POST", "/v1/chat/completions", json.dumps({"model": "m", "messages": []}),
                      {"Content-Type": "application/json"})
            r = c.getresponse()
            body = json.loads(r.read())
            self.assertEqual(r.status, 200)
            self.assertEqual(body["choices"][0]["message"]["content"], "hi")
            self.assertEqual(seen, ["web", "real"])
        finally:
            _s.load_targets, _s.load_chains, _p._call_raw = o


class TRobust(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.srv = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.port = cls.srv.server_address[1]
        import threading
        threading.Thread(target=cls.srv.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls):
        cls.srv.shutdown()

    def setUp(self):
        router._reset()

    def _post(self, path, payload):
        c = HTTPConnection("127.0.0.1", self.port, timeout=5)
        c.request("POST", path, json.dumps(payload), {"Content-Type": "application/json"})
        r = c.getresponse()
        return r.status, r.read()

    def test_null_model_400_not_crash(self):
        code, _ = self._post("/v1/chat/completions", {"model": None, "messages": []})
        self.assertEqual(code, 400)

    def test_missing_model_400(self):
        code, _ = self._post("/v1/chat/completions", {"messages": []})
        self.assertEqual(code, 400)

    def test_chain_embeddings_rejected(self):
        import store as _s
        o = _s.load_targets
        _s.load_targets = lambda *a: [{"name": "u", "base_url": "http://x/v1", "api_key": "k", "models": ["m"]}]
        try:
            code, body = self._post("/v1/embeddings", {"model": "chain:x", "input": "hi"})
            self.assertEqual(code, 400)
            self.assertIn("chain", body.decode())
        finally:
            _s.load_targets = o

    def test_vision_content_passes_gate(self):
        import providers as _p
        body = b'{"choices":[{"message":{"role":"assistant","content":[{"type":"text","text":"hi"}]}}]}'
        self.assertTrue(_p.valid_chat(body))

    def test_cache_hit_counted(self):
        import store as _s, providers as _p, stats as _st, tempfile, os
        tmp = os.path.join(tempfile.mkdtemp(), "c.db")
        orig_home = os.path.expanduser
        os.path.expanduser = lambda p: tmp if p == "~" else orig_home(p)
        _st.reset()
        calls = []
        o_call, o_load = _p.call_target, _s.load_targets
        _p.call_target = lambda t, p, timeout=30: (calls.append(1) or (200, {}, b'{"choices":[{"message":{"content":"v"}}]}'))
        _s.load_targets = lambda *a: [{"name": "u", "base_url": "http://x/v1", "api_key": "k", "models": ["m"]}]
        try:
            payload = {"model": "m", "messages": [{"role": "user", "content": "hi"}], "cache_ttl": 60}
            for _ in range(2):
                c = HTTPConnection("127.0.0.1", self.port, timeout=5)
                c.request("POST", "/v1/chat/completions", json.dumps(payload), {"Content-Type": "application/json"})
                self.assertEqual(c.getresponse().status, 200)
            s = _st.snap()
            self.assertEqual(s["req"], 2)  # hits count too
            self.assertIn("cache", s["by_target"])
        finally:
            os.path.expanduser = orig_home
            _p.call_target, _s.load_targets = o_call, o_load


class TCacheTtl(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.srv = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.port = cls.srv.server_address[1]
        import threading
        threading.Thread(target=cls.srv.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls):
        cls.srv.shutdown()

    def test_garbage_ttl_400_not_hang(self):
        import store as _s, providers as _p
        o = (_s.load_targets, _p.call_target)
        _s.load_targets = lambda *a: [{"name": "u", "base_url": "http://x/v1", "api_key": "k", "models": ["m"]}]
        _p.call_target = lambda t, p, timeout=30: (200, {}, b'{"choices":[{"message":{"content":"v"}}]}')
        try:
            c = HTTPConnection("127.0.0.1", self.port, timeout=5)
            c.request("POST", "/v1/chat/completions",
                      json.dumps({"model": "m", "messages": [], "cache_ttl": "abc"}),
                      {"Content-Type": "application/json"})
            self.assertEqual(c.getresponse().status, 400)
        finally:
            _s.load_targets, _p.call_target = o


class TPayloadType(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.srv = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.port = cls.srv.server_address[1]
        import threading
        threading.Thread(target=cls.srv.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls):
        cls.srv.shutdown()

    def _post(self, path, raw):
        c = HTTPConnection("127.0.0.1", self.port, timeout=5)
        c.request("POST", path, raw, {"Content-Type": "application/json"})
        r = c.getresponse()
        return r.status, r.read()

    def test_array_body_chat_400(self):
        code, _ = self._post("/v1/chat/completions", "[1,2]")
        self.assertEqual(code, 400)

    def test_string_body_embeddings_400(self):
        code, _ = self._post("/v1/embeddings", '"hi"')
        self.assertEqual(code, 400)

    def test_null_body_probe_400(self):
        code, _ = self._post("/v1/test/call", "null")
        self.assertEqual(code, 400)


class TExactModel(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.srv = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.port = cls.srv.server_address[1]
        import threading
        threading.Thread(target=cls.srv.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls):
        cls.srv.shutdown()

    def setUp(self):
        router._reset()

    def test_no_silent_substitution(self):
        import store as _s, providers as _p
        o = (_s.load_targets, _s.load_chains, _p.call_target)
        _s.load_targets = lambda *a: [{"name": "a", "models": ["x"]},
                                      {"name": "b", "models": ["y"]}]
        _s.load_chains = lambda: []
        seen = []
        _p.call_target = lambda t, p, timeout=30: (
            seen.append((t["name"], p.get("model"))) or
            (200, {}, b'{"choices":[{"message":{"content":"ok"}}]}'))
        try:
            c = HTTPConnection("127.0.0.1", self.port, timeout=5)
            c.request("POST", "/v1/chat/completions", json.dumps({"model": "y", "messages": []}),
                      {"Content-Type": "application/json"})
            self.assertEqual(c.getresponse().status, 200)
            self.assertEqual(seen, [("b", "y")])  # a never touched, model exact
        finally:
            _s.load_targets, _s.load_chains, _p.call_target = o
