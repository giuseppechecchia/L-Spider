import os
import sys
import threading
import re
import time
import atexit
import signal
import shutil

try:
    from wcwidth import wcswidth, wcwidth
except ImportError:
    def wcswidth(s): return len(s)
    def wcwidth(c): return 1


ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def strip_ansi(s):
    return ANSI_RE.sub("", s)


def truncate_visual(s, max_width):
    out = []
    w = 0
    i = 0
    while i < len(s):
        if s[i] == "\x1b":
            m = ANSI_RE.match(s, i)
            if m:
                out.append(m.group(0))
                i = m.end()
                continue
        ch = s[i]
        cw = wcwidth(ch)
        if cw < 0:
            cw = 0
        if w + cw > max_width:
            break
        out.append(ch)
        w += cw
        i += 1
    return "".join(out)


class Logger:
    RESET = "\x1b[0m"
    BOLD = "\x1b[1m"
    DIM = "\x1b[2m"
    CYAN = "\x1b[36m"
    GREEN = "\x1b[32m"
    YELLOW = "\x1b[33m"
    RED = "\x1b[31m"

    def __init__(
        self,
        enable_status=True,
        enable_colors=True,
        enable_ansi=None,
        status_interval=None,
        status_tag="[ DHT ]",
        **_ignored,
    ):
        self.enable_status = bool(enable_status)
        self.enable_colors = bool(enable_colors)

        detected = self._detect_ansi()
        self._ansi = detected if enable_ansi is None else bool(enable_ansi)

        self._lock = threading.Lock()
        self._last_status_msg = ""
        self._last_status_ts = 0.0

        self.status_tag = str(status_tag)
        self.status_color = self.GREEN

        if status_interval is None:
            self.status_interval = None
        else:
            try:
                self.status_interval = float(status_interval)
            except Exception:
                self.status_interval = None

        self._scroll_region_active = False
        self._signals_installed = False
        self._sig_prev_int = None
        self._sig_prev_term = None

        if self._ansi and self.enable_status:
            self._install_signal_handlers()
            self._init_scroll_region()

    def _detect_ansi(self):
        if os.environ.get("NO_COLOR"):
            return False
        term = os.environ.get("TERM", "")
        if term in ("", "dumb"):
            return False
        try:
            return sys.stdout.isatty()
        except Exception:
            return False

    def _c(self, s, color):
        if not (self._ansi and self.enable_colors and color):
            return s
        return f"{color}{s}{self.RESET}"

    def _install_signal_handlers(self):
        if self._signals_installed:
            return
        try:
            self._sig_prev_int = signal.getsignal(signal.SIGINT)
            self._sig_prev_term = signal.getsignal(signal.SIGTERM)
            signal.signal(signal.SIGINT, self._handle_sigint)
            signal.signal(signal.SIGTERM, self._handle_sigterm)
            self._signals_installed = True
            atexit.register(self._restore_signal_handlers)
        except Exception:
            self._signals_installed = False

    def _restore_signal_handlers(self):
        if not self._signals_installed:
            return
        try:
            if self._sig_prev_int is not None:
                signal.signal(signal.SIGINT, self._sig_prev_int)
            if self._sig_prev_term is not None:
                signal.signal(signal.SIGTERM, self._sig_prev_term)
        except Exception:
            pass
        finally:
            self._signals_installed = False

    def _handle_sigint(self, signum, frame):
        try:
            self._restore_scroll_region()
        finally:
            prev = self._sig_prev_int
            if callable(prev):
                prev(signum, frame)
                return
            if prev == signal.SIG_IGN:
                return
            raise KeyboardInterrupt

    def _handle_sigterm(self, signum, frame):
        try:
            self._restore_scroll_region()
        finally:
            prev = self._sig_prev_term
            if callable(prev):
                prev(signum, frame)
                return
            if prev == signal.SIG_IGN:
                return
            raise SystemExit(0)

    def _term_rows(self):
        try:
            sz = shutil.get_terminal_size(fallback=(80, 24))
            return int(sz.lines) if sz.lines else 24
        except Exception:
            return 24

    def _init_scroll_region(self):
        if self._scroll_region_active:
            return
        rows = self._term_rows()
        if rows < 3:
            return

        self._scroll_region_active = True
        atexit.register(self._restore_scroll_region)

        sys.stdout.write(f"\x1b[2;{rows}r")
        sys.stdout.write(f"\x1b[{rows};1H")
        sys.stdout.flush()

    def _restore_scroll_region(self):
        if not self._scroll_region_active:
            return
        self._scroll_region_active = False
        try:
            sys.stdout.write("\x1b[r")
            sys.stdout.flush()
        except Exception:
            pass

    def _should_redraw_status_locked(self):
        if not self.status_interval:
            return True
        now = time.time()
        if now - self._last_status_ts >= self.status_interval:
            self._last_status_ts = now
            return True
        return False

    def _draw_status_line_locked(self, msg, force=False):
        if not self.enable_status:
            return

        self._last_status_msg = str(msg)
        if not force and not self._should_redraw_status_locked():
            return

        line = f"{self.status_tag} {self._last_status_msg}".rstrip()

        if self._ansi:
            line = self._c(line, self.status_color)
            sys.stdout.write("\x1b7")
            sys.stdout.write("\x1b[1;1H\x1b[2K")
            sys.stdout.write(line[:800])
            sys.stdout.write("\x1b8")
            sys.stdout.flush()
            return

        sys.stdout.write("\r" + line[:800])
        sys.stdout.flush()

    def status(self, *args):
        msg = " ".join(str(a) for a in args)
        with self._lock:
            if self._ansi and self.enable_status:
                self._init_scroll_region()
            self._draw_status_line_locked(msg)

    def line(self, *args):
        text = " ".join(str(a) for a in args)
        with self._lock:
            if self._ansi and self.enable_status:
                self._init_scroll_region()
            sys.stdout.write(text + "\n")
            sys.stdout.flush()
            if self._ansi and self.enable_status and self._last_status_msg:
                self._draw_status_line_locked(self._last_status_msg, force=True)

    def info(self, *args):
        self.line("[INFO]", *args)

    def warn(self, *args):
        p = "[WARN]"
        if self._ansi:
            p = self._c(p, self.YELLOW)
        self.line(p, *args)

    def error(self, *args):
        p = "[ERROR]"
        if self._ansi:
            p = self._c(p, self.RED)
        self.line(p, *args)

    def meta(self, *args):
        p = "[INFO][META]"
        if self._ansi:
            p = self._c(p, self.CYAN)
        self.line(p, *args)

    def _box(self, title, lines, max_width=140):
        clean_lines = [strip_ansi(str(x)) for x in lines if x is not None]

        w_title = wcswidth(strip_ansi(title))
        w_lines = max((wcswidth(s) for s in clean_lines), default=0)
        w = min(max(w_title, w_lines), max_width)

        out = []
        top = "┌─ " + title + " " * (w - wcswidth(strip_ansi(title))) + " ─┐"
        out.append(top)

        for raw in lines:
            if raw is None:
                continue
            truncated = truncate_visual(str(raw), w)
            pad = " " * (w - wcswidth(strip_ansi(truncated)))
            out.append("│  " + truncated + pad + "  │")

        out.append("└" + "─" * (w + 4) + "┘")
        return out

    def torrent_block(self, name, sender, infohash_hex):
        l_bt = self._c("BT Name:", self.DIM)
        l_sender = self._c("Sender:", self.DIM)
        l_infohash = self._c("infohash:", self.DIM)
        l_magnet = self._c("magnet:", self.DIM)

        h = str(infohash_hex)
        h_col = self._c(h, self.CYAN)
        magnet = f"magnet:?xt=urn:btih:{h}"
        magnet_col = self._c(magnet, self.GREEN)

        title = "TORRENT"
        if self._ansi:
            title = self._c(title, self.BOLD)

        lines = [
            f"{l_bt} {name}",
            f"{l_sender} {sender}",
            f"{l_infohash} {h_col}",
            f"{l_magnet} {magnet_col}",
        ]

        for row in self._box(title, lines):
            self.line(row)
