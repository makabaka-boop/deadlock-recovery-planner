"""极简 HTTP 后端: POST /resolve 接收状态 JSON, 返回回放 JSON.

仅依赖标准库. 运行: python3 server.py [host] [port]
"""

import json
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer

from resolver import InvalidState, resolve


class _Handler(BaseHTTPRequestHandler):
    def _send(self, code, payload):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        if self.path != "/resolve":
            self._send(404, {"status": "error", "error": "未知路径"})
            return
        try:
            length = int(self.headers.get("Content-Length") or 0)
            state = json.loads(self.rfile.read(length) or b"{}")
        except (ValueError, UnicodeDecodeError):
            self._send(400, {"status": "error", "error": "请求体不是合法 JSON"})
            return
        try:
            self._send(200, resolve(state))
        except InvalidState as exc:
            self._send(400, {"status": "error", "error": str(exc)})

    def log_message(self, *args):
        pass


def make_server(host="127.0.0.1", port=8000):
    return HTTPServer((host, port), _Handler)


def main():
    host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 8000
    server = make_server(host, port)
    print("listening on http://%s:%d/resolve" % server.server_address[:2])
    server.serve_forever()


if __name__ == "__main__":
    main()
