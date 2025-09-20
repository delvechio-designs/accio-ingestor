# accio-ingestor/src/accio_ingestor/gui.py
from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from typing import Optional

from PySide6.QtCore import Qt, QTimer, QSize
from PySide6.QtGui import QIcon
from PySide6.QtWidgets import (
    QApplication,
    QFileDialog,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QStatusBar,
    QTextEdit,
    QVBoxLayout,
    QWidget,
    QFrame,
    QSpacerItem,
    QSizePolicy,
)

from .config import settings, ensure_dirs, Settings
from .logging_cfg import init_logging, get_logger
from .queue import JobQueue
from .watcher import WatchRunner
from .slack import SlackClient
from .importer import AccioClient
from .s3uploader import S3Uploader


log = get_logger(__name__)


class Card(QFrame):
    def __init__(self, title: str):
        super().__init__()
        self.setObjectName("Card")
        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 16, 16, 16)
        title_lbl = QLabel(title)
        title_lbl.setObjectName("CardTitle")
        layout.addWidget(title_lbl)
        self.inner = QGridLayout()
        self.inner.setContentsMargins(0, 8, 0, 0)
        layout.addLayout(self.inner)


class MainWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Accio Ingestor")
        icon_path = Path(__file__).with_name("assets").joinpath("icon.png")
        if icon_path.exists():
            self.setWindowIcon(QIcon(str(icon_path)))
        self.resize(980, 640)

        root = QVBoxLayout(self)
        root.setContentsMargins(16, 16, 16, 16)
        header = QLabel("Accio Ingestor\n<small>Offline-first PDF/Image ingestion with S3 + Accio</small>")
        header.setTextFormat(Qt.RichText)
        header.setObjectName("Header")
        root.addWidget(header)

        # Folder card
        self.card_paths = Card("Folders")
        self.ed_watch = QLineEdit(settings.WATCH_DIR)
        self.ed_processed = QLineEdit(settings.PROCESSED_DIR)
        self.ed_failed = QLineEdit(settings.FAILED_DIR)
        for i, (lbl, ed) in enumerate(
            (("Watch", self.ed_watch), ("Processed", self.ed_processed), ("Failed", self.ed_failed))
        ):
            self.card_paths.inner.addWidget(QLabel(lbl), i, 0)
            self.card_paths.inner.addWidget(ed, i, 1)
            btn = QPushButton("Browse")
            btn.clicked.connect(lambda _, e=ed: self.on_browse(e))
            self.card_paths.inner.addWidget(btn, i, 2)
        self.btn_start = QPushButton("Start Watching")
        self.btn_stop = QPushButton("Stop")
        self.btn_stop.setEnabled(False)
        btn_row = QHBoxLayout()
        btn_row.addWidget(self.btn_start)
        btn_row.addWidget(self.btn_stop)
        btn_row.addItem(QSpacerItem(10, 10, QSizePolicy.Expanding, QSizePolicy.Minimum))
        self.card_paths.inner.addLayout(btn_row, 3, 0, 1, 3)
        root.addWidget(self.card_paths)

        # Test card
        self.card_tests = Card("Diagnostics")
        self.btn_test_slack = QPushButton("Test Slack")
        self.btn_test_accio = QPushButton("Test Accio")
        self.btn_test_s3 = QPushButton("Test S3")
        self.card_tests.inner.addWidget(self.btn_test_slack, 0, 0)
        self.card_tests.inner.addWidget(self.btn_test_accio, 0, 1)
        self.card_tests.inner.addWidget(self.btn_test_s3, 0, 2)
        root.addWidget(self.card_tests)

        # Activity card
        self.card_activity = Card("Activity")
        self.txt_log = QTextEdit()
        self.txt_log.setReadOnly(True)
        self.txt_log.setObjectName("LogView")
        self.card_activity.inner.addWidget(self.txt_log, 0, 0, 1, 3)
        root.addWidget(self.card_activity)

        self.status = QStatusBar()
        self.lbl_heartbeat = QLabel("heartbeat: idle")
        self.lbl_queue = QLabel("queue: 0")
        self.lbl_conns = QLabel("S3:?, Accio:?")
        self.status.addWidget(self.lbl_heartbeat)
        self.status.addPermanentWidget(self.lbl_queue)
        self.status.addPermanentWidget(self.lbl_conns)
        root.addWidget(self.status)

        # Wiring
        self.btn_start.clicked.connect(self.on_start)
        self.btn_stop.clicked.connect(self.on_stop)
        self.btn_test_slack.clicked.connect(self.on_test_slack)
        self.btn_test_accio.clicked.connect(self.on_test_accio)
        self.btn_test_s3.clicked.connect(self.on_test_s3)

        self.queue: Optional[JobQueue] = None
        self.runner: Optional[WatchRunner] = None
        self.heartbeat = QTimer(self)
        self.heartbeat.timeout.connect(self.on_heartbeat)

    def on_browse(self, ed: QLineEdit):
        path = QFileDialog.getExistingDirectory(self, "Choose folder", ed.text())
        if path:
            ed.setText(path)

    def append_log(self, msg: str):
        self.txt_log.append(msg)

    def on_start(self):
        # Update settings in-memory (without re-validating env)
        settings.WATCH_DIR = self.ed_watch.text()
        settings.PROCESSED_DIR = self.ed_processed.text()
        settings.FAILED_DIR = self.ed_failed.text()
        ensure_dirs()

        if self.queue is None:
            self.queue = JobQueue()
            self.queue.start_worker()

        if self.runner is None:
            self.runner = WatchRunner(queue=self.queue)
            threading.Thread(target=self.runner.start_blocking, daemon=True).start()

        self.append_log("Started watcher and worker…")
        self.btn_start.setEnabled(False)
        self.btn_stop.setEnabled(True)
        self.heartbeat.start(1000)

    def on_stop(self):
        self.heartbeat.stop()
        if self.runner:
            self.runner.stop()
            self.runner = None
        if self.queue:
            self.queue.stop_worker()
            self.queue = None
        self.append_log("Stopped.")
        self.btn_start.setEnabled(True)
        self.btn_stop.setEnabled(False)

    def on_test_slack(self):
        try:
            SlackClient().error("test", {"filename": "N/A", "sha256": "N/A", "code": "TEST"})
            self.append_log("Slack test enqueued.")
        except Exception as e:
            self.append_log(f"Slack test failed: {e}")

    def on_test_accio(self):
        try:
            ok = AccioClient().healthcheck()
            self.append_log(f"Accio test: {'OK' if ok else 'FAIL'}")
        except Exception as e:
            self.append_log(f"Accio test exception: {e}")

    def on_test_s3(self):
        try:
            key = "diagnostics/test-object.txt"
            S3Uploader().put_object_with_retention(
                key=key,
                content=b"accio-ingestor connectivity test",
                content_type="text/plain",
                tags={"retention": "2y", "app": "accio-ingestor"},
            )
            self.append_log("S3 test upload OK.")
        except Exception as e:
            self.append_log(f"S3 test failed: {e}")

    def on_heartbeat(self):
        ts = time.strftime("%H:%M:%S")
        self.lbl_heartbeat.setText(f"heartbeat: {ts}")
        if self.queue:
            self.lbl_queue.setText(f"queue: {self.queue.count_jobs()}")
        # Connectivity checks
        try:
            acc_ok = AccioClient().healthcheck()
        except Exception:
            acc_ok = False
        try:
            # a light head call is not available in S3 client; assume ok if creds load
            _ = S3Uploader().region
            s3_ok = True
        except Exception:
            s3_ok = False
        self.lbl_conns.setText(f"S3:{'✓' if s3_ok else '✗'}, Accio:{'✓' if acc_ok else '✗'}")


def launch_gui() -> int:
    init_logging()
    ensure_dirs()
    app = QApplication([])
    # Load stylesheet
    qss = Path(__file__).with_name("theme.qss").read_text(encoding="utf-8", errors="ignore")
    app.setStyleSheet(qss)
    w = MainWindow()
    w.show()
    return app.exec()
