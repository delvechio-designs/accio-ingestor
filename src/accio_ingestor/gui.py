from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import Optional

import requests
from PySide6.QtCore import Qt
from PySide6.QtGui import QIcon
from PySide6.QtWidgets import (
    QApplication,
    QMainWindow,
    QWidget,
    QLabel,
    QPushButton,
    QLineEdit,
    QHBoxLayout,
    QVBoxLayout,
    QFileDialog,
    QTextEdit,
    QStatusBar,
    QFrame,
    QTabWidget,
    QFormLayout,
    QCheckBox,
    QMessageBox,
)

from .config import settings, ensure_dirs, reload_from_env
from .queue import JobQueue
from .watcher import WatchRunner
from .settings_store import load_env_file, save_env_file, masked_preview
from .s3uploader import S3Uploader
from .importer import AccioClient
from PySide6.QtCore import Qt, QTimer


def nice_card(widget: QWidget) -> QWidget:
    widget.setObjectName("Card")
    frame = QFrame()
    layout = QVBoxLayout(frame)
    layout.addWidget(widget)
    return frame


def valid_license(key: Optional[str]) -> bool:
    if not key:
        return False
    s = key.strip()
    parts = s.split("-")
    return len(s) >= 16 and len(parts) >= 4


class MainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("Accio Ingestor")
        self.setWindowIcon(QIcon(str(Path(__file__).parent / "assets" / "icon.png")))
        self.resize(960, 720)

        self.queue = JobQueue()
        self.runner: Optional[WatchRunner] = None
        self.heartbeat_timer = QTimer()
        self.heartbeat_timer.timeout.connect(self.on_heartbeat)


        self.tabs = QTabWidget()
        self.setCentralWidget(self.tabs)

        self.dashboard = self._build_dashboard_tab()
        self.setup = self._build_setup_tab()
        self.diagnostics = self._build_diag_tab()

        self.tabs.addTab(self.dashboard, "Dashboard")
        self.tabs.addTab(self.setup, "Setup")
        self.tabs.addTab(self.diagnostics, "Diagnostics")

        self.status = QStatusBar()
        self.setStatusBar(self.status)
        self.job_count_label = QLabel("Jobs: 0")
        self.heartbeat_label = QLabel("⏺ idle")
        self.status.addPermanentWidget(self.job_count_label)
        self.status.addPermanentWidget(self.heartbeat_label)

        qss = Path(__file__).parent / "theme.qss"
        if qss.exists():
            self.setStyleSheet(qss.read_text(encoding="utf-8"))

        ensure_dirs()
        self.queue.start_worker()
        self._update_license_gate()

    # ---------- DASHBOARD ----------
    def _build_dashboard_tab(self) -> QWidget:
        w = QWidget()
        layout = QVBoxLayout(w)

        folders = QWidget()
        fform = QFormLayout(folders)
        self.inp_watch = QLineEdit(settings.WATCH_DIR)
        self.inp_processed = QLineEdit(settings.PROCESSED_DIR)
        self.inp_failed = QLineEdit(settings.FAILED_DIR)
        btn_watch = QPushButton("Choose…")
        btn_proc = QPushButton("Choose…")
        btn_fail = QPushButton("Choose…")
        btn_watch.clicked.connect(lambda: self._pick_dir(self.inp_watch))
        btn_proc.clicked.connect(lambda: self._pick_dir(self.inp_processed))
        btn_fail.clicked.connect(lambda: self._pick_dir(self.inp_failed))

        frow1 = QWidget(); h1 = QHBoxLayout(frow1); h1.addWidget(self.inp_watch); h1.addWidget(btn_watch)
        frow2 = QWidget(); h2 = QHBoxLayout(frow2); h2.addWidget(self.inp_processed); h2.addWidget(btn_proc)
        frow3 = QWidget(); h3 = QHBoxLayout(frow3); h3.addWidget(self.inp_failed); h3.addWidget(btn_fail)

        fform.addRow(QLabel("Watch folder"), frow1)
        fform.addRow(QLabel("Processed folder"), frow2)
        fform.addRow(QLabel("Failed folder"), frow3)
        layout.addWidget(nice_card(folders))

        controls = QWidget()
        ch = QHBoxLayout(controls)
        self.btn_start = QPushButton("Start Watching")
        self.btn_stop = QPushButton("Stop")
        self.btn_start.clicked.connect(self.on_start)
        self.btn_stop.clicked.connect(self.on_stop)
        ch.addWidget(self.btn_start)
        ch.addWidget(self.btn_stop)
        layout.addWidget(nice_card(controls))

        self.log = QTextEdit()
        self.log.setReadOnly(True)
        layout.addWidget(nice_card(self.log), 1)

        return w

    # ---------- SETUP ----------
    def _build_setup_tab(self) -> QWidget:
        w = QWidget()
        layout = QVBoxLayout(w)

        intro = QLabel(
            "<b>Welcome to Accio Ingestor</b><br>"
            "1) Fill in your webhooks and credentials.<br>"
            "2) Click <i>Save</i>.<br>"
            "3) Use the <i>Test</i> buttons to verify connectivity.<br>"
            "4) Go to <i>Dashboard</i> and Start Watching."
        )
        intro.setWordWrap(True)
        layout.addWidget(nice_card(intro))

        formw = QWidget()
        form = QFormLayout(formw)

        self.inp_license = QLineEdit(settings.LICENSE_KEY or "")
        form.addRow(QLabel("License key"), self.inp_license)

        self.inp_slack = QLineEdit(str(settings.SLACK_WEBHOOK_URL or ""))
        btn_slack_test = QPushButton("Test Slack")
        btn_slack_test.clicked.connect(self._test_slack)
        row_slack = QWidget(); hs = QHBoxLayout(row_slack); hs.addWidget(self.inp_slack); hs.addWidget(btn_slack_test)
        form.addRow(QLabel("Slack webhook URL"), row_slack)

        self.inp_accio_url = QLineEdit(str(settings.ACCIO_ENDPOINT))
        self.inp_accio_token = QLineEdit(settings.ACCIO_TOKEN or "")
        self.inp_accio_token.setEchoMode(QLineEdit.EchoMode.Password)
        btn_accio_test = QPushButton("Test Accio")
        btn_accio_test.clicked.connect(self._test_accio)
        row_accio1 = QWidget(); ha1 = QHBoxLayout(row_accio1); ha1.addWidget(self.inp_accio_url); ha1.addWidget(btn_accio_test)
        form.addRow(QLabel("Accio endpoint"), row_accio1)
        form.addRow(QLabel("Accio token"), self.inp_accio_token)

        self.inp_region = QLineEdit(settings.AWS_REGION)
        self.inp_bucket = QLineEdit(settings.S3_BUCKET or "")
        self.inp_prefix = QLineEdit(settings.S3_PREFIX)
        self.inp_kms = QLineEdit(settings.S3_SSE_KMS_KEY_ID or "")
        self.inp_lock_mode = QLineEdit(settings.S3_OBJECT_LOCK_MODE or "")
        self.inp_lock_days = QLineEdit(str(settings.S3_OBJECT_LOCK_DAYS))
        self.inp_ak = QLineEdit(settings.AWS_ACCESS_KEY_ID or "")
        self.inp_sk = QLineEdit(settings.AWS_SECRET_ACCESS_KEY or "")
        self.inp_sk.setEchoMode(QLineEdit.EchoMode.Password)

        form.addRow(QLabel("AWS region"), self.inp_region)
        form.addRow(QLabel("S3 bucket"), self.inp_bucket)
        form.addRow(QLabel("S3 prefix"), self.inp_prefix)
        form.addRow(QLabel("SSE-KMS key id (optional)"), self.inp_kms)
        form.addRow(QLabel("Object Lock mode (GOVERNANCE/COMPLIANCE)"), self.inp_lock_mode)
        form.addRow(QLabel("Object Lock days"), self.inp_lock_days)
        form.addRow(QLabel("AWS access key id (optional)"), self.inp_ak)
        form.addRow(QLabel("AWS secret access key (optional)"), self.inp_sk)

        self.inp_tess = QLineEdit(settings.TESSERACT_CMD or "")
        btn_tess = QPushButton("Browse…")
        btn_tess.clicked.connect(lambda: self._pick_file(self.inp_tess))
        row_tess = QWidget(); ht = QHBoxLayout(row_tess); ht.addWidget(self.inp_tess); ht.addWidget(btn_tess)
        form.addRow(QLabel("Tesseract path"), row_tess)

        layout.addWidget(nice_card(formw))

        acts = QWidget()
        ha = QHBoxLayout(acts)
        self.chk_save_env = QCheckBox("Save to .env in project root")
        self.chk_save_env.setChecked(True)
        btn_save = QPushButton("Save")
        btn_reload = QPushButton("Reload from .env")
        btn_s3_test = QPushButton("Test S3")
        btn_save.clicked.connect(self.on_save_settings)
        btn_reload.clicked.connect(self.on_reload_settings)
        btn_s3_test.clicked.connect(self._test_s3)

        ha.addWidget(self.chk_save_env)
        ha.addWidget(btn_save)
        ha.addWidget(btn_reload)
        ha.addStretch(1)
        ha.addWidget(btn_s3_test)
        layout.addWidget(nice_card(acts))

        return w

    # ---------- DIAGNOSTICS ----------
    def _build_diag_tab(self) -> QWidget:
        w = QWidget()
        layout = QVBoxLayout(w)
        out = QTextEdit()
        out.setReadOnly(True)
        layout.addWidget(nice_card(out))

        def refresh():
            envp = Path(".env")
            kv = load_env_file(envp)
            masked = masked_preview(kv)
            payload = {
                "env_path": str(envp.resolve()),
                "settings_preview": masked,
                "watch_dirs": {
                    "watch": settings.WATCH_DIR,
                    "processed": settings.PROCESSED_DIR,
                    "failed": settings.FAILED_DIR,
                },
            }
            out.setPlainText(json.dumps(payload, indent=2))

        btn = QPushButton("Refresh")
        btn.clicked.connect(refresh)
        layout.addWidget(btn, alignment=Qt.AlignmentFlag.AlignRight)
        refresh()
        return w

    # ---------- Helpers ----------
    def _pick_dir(self, target: QLineEdit) -> None:
        d = QFileDialog.getExistingDirectory(self, "Choose folder")
        if d:
            target.setText(d)

    def _pick_file(self, target: QLineEdit) -> None:
        f, _ = QFileDialog.getOpenFileName(self, "Choose file")
        if f:
            target.setText(f)

    def _append_log(self, text: str) -> None:
        self.log.append(text)

    def on_heartbeat(self) -> None:
        self.job_count_label.setText(f"Jobs: {self.queue.count_jobs()}")
        self.heartbeat_label.setText("❤ running")

    # ---------- Tests ----------
    def _test_slack(self) -> None:
        url = self.inp_slack.text().strip()
        if not url:
            QMessageBox.warning(self, "Slack", "Webhook URL is empty.")
            return
        try:
            resp = requests.post(url, json={"text": "Accio Ingestor test message (sanitized)."}, timeout=10)
            if 200 <= resp.status_code < 300:
                QMessageBox.information(self, "Slack", "Sent test message (check your channel).")
            else:
                QMessageBox.critical(self, "Slack", f"Webhook returned HTTP {resp.status_code}")
        except Exception as e:
            QMessageBox.critical(self, "Slack", f"Request failed: {e}")

    def _test_accio(self) -> None:
        url = self.inp_accio_url.text().strip()
        tok = self.inp_accio_token.text().strip()
        # Temporarily override via env + reload so AccioClient() picks it up
        old_url = os.environ.get("ACCIO_ENDPOINT")
        old_tok = os.environ.get("ACCIO_TOKEN")
        try:
            os.environ["ACCIO_ENDPOINT"] = url
            os.environ["ACCIO_TOKEN"] = tok
            reload_from_env()  # rebuild settings from env
            AccioClient().post_document({"ping": "ok"})
            QMessageBox.information(self, "Accio", "POST succeeded.")
        except Exception as e:
            QMessageBox.critical(self, "Accio", f"POST failed: {e}")
        finally:
            # restore previous env and settings
            if old_url is None:
                os.environ.pop("ACCIO_ENDPOINT", None)
            else:
                os.environ["ACCIO_ENDPOINT"] = old_url
            if old_tok is None:
                os.environ.pop("ACCIO_TOKEN", None)
            else:
                os.environ["ACCIO_TOKEN"] = old_tok
            reload_from_env()

    def _test_s3(self) -> None:
        try:
            up = S3Uploader()
            key = f"{settings.S3_PREFIX.rstrip('/')}/__self_test.txt"
            up.put_object_with_retention(key, b"ok", "text/plain", {"source": "accio-ingestor"})
            QMessageBox.information(self, "S3", "PutObject succeeded.")
        except Exception as e:
            QMessageBox.critical(self, "S3", f"S3 put failed: {e}")

    def _update_license_gate(self) -> None:
        ok = valid_license(settings.LICENSE_KEY)
        self.btn_start.setEnabled(ok)
        self.setWindowTitle("Accio Ingestor" + ("" if ok else " — LICENSE REQUIRED"))

    # ---------- Save/Reload ----------
    def on_save_settings(self) -> None:
        kv = {
            "WATCH_DIR": self.inp_watch.text().strip() or settings.WATCH_DIR,
            "PROCESSED_DIR": self.inp_processed.text().strip() or settings.PROCESSED_DIR,
            "FAILED_DIR": self.inp_failed.text().strip() or settings.FAILED_DIR,
            "SLACK_WEBHOOK_URL": self.inp_slack.text().strip(),
            "ACCIO_ENDPOINT": self.inp_accio_url.text().strip(),
            "ACCIO_TOKEN": self.inp_accio_token.text().strip(),
            "AWS_REGION": self.inp_region.text().strip(),
            "S3_BUCKET": self.inp_bucket.text().strip(),
            "S3_PREFIX": self.inp_prefix.text().strip(),
            "S3_SSE_KMS_KEY_ID": self.inp_kms.text().strip(),
            "S3_OBJECT_LOCK_MODE": self.inp_lock_mode.text().strip(),
            "S3_OBJECT_LOCK_DAYS": self.inp_lock_days.text().strip(),
            "AWS_ACCESS_KEY_ID": self.inp_ak.text().strip(),
            "AWS_SECRET_ACCESS_KEY": self.inp_sk.text().strip(),
            "TESSERACT_CMD": self.inp_tess.text().strip(),
            "LICENSE_KEY": self.inp_license.text().strip(),
            "LOG_FILE": settings.LOG_FILE,
            "LOG_JSONL": settings.LOG_JSONL,
            "LOG_LEVEL": settings.LOG_LEVEL,
        }
        envp = Path(".env")
        save_env_file(envp, kv)
        reload_from_env(str(envp))
        self._update_license_gate()
        QMessageBox.information(self, "Settings", "Saved and reloaded from .env.")

    def on_reload_settings(self) -> None:
        reload_from_env(".env")
        self.inp_license.setText(settings.LICENSE_KEY or "")
        self.inp_slack.setText(str(settings.SLACK_WEBHOOK_URL or ""))
        self.inp_accio_url.setText(str(settings.ACCIO_ENDPOINT))
        self.inp_accio_token.setText(settings.ACCIO_TOKEN or "")
        self.inp_region.setText(settings.AWS_REGION)
        self.inp_bucket.setText(settings.S3_BUCKET or "")
        self.inp_prefix.setText(settings.S3_PREFIX)
        self.inp_kms.setText(settings.S3_SSE_KMS_KEY_ID or "")
        self.inp_lock_mode.setText(settings.S3_OBJECT_LOCK_MODE or "")
        self.inp_lock_days.setText(str(settings.S3_OBJECT_LOCK_DAYS))
        self.inp_ak.setText(settings.AWS_ACCESS_KEY_ID or "")
        self.inp_sk.setText(settings.AWS_SECRET_ACCESS_KEY or "")
        self.inp_tess.setText(settings.TESSERACT_CMD or "")
        QMessageBox.information(self, "Settings", "Reloaded from .env.")

    # ---------- Start/Stop ----------
    def on_start(self) -> None:
        if not valid_license(settings.LICENSE_KEY):
            QMessageBox.critical(self, "License", "Enter a valid license in Setup to start.")
            return
        ensure_dirs()
        if self.runner is None:
            self.runner = WatchRunner(self.queue)
        t = threading.Thread(target=self.runner.start_blocking, daemon=True)
        t.start()
        self.heartbeat_timer.start(1000)
        self._append_log("[INFO] watcher started")

    def on_stop(self) -> None:
        if self.runner:
            self.runner.stop()
            self.runner = None
            self._append_log("[INFO] watcher stopped")
        self.heartbeat_timer.stop()


def run_gui() -> None:
    app = QApplication([])
    win = MainWindow()
    win.show()
    app.exec()
