from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import Callable, Optional

import requests
from PySide6.QtCore import Qt, QTimer, QUrl
from PySide6.QtGui import QDesktopServices, QIcon, QPixmap
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
    QFormLayout,
    QCheckBox,
    QMessageBox,
    QListWidget,
    QListWidgetItem,
    QStackedWidget,
    QTableWidget,
    QTableWidgetItem,
    QAbstractItemView,
    QHeaderView,
)

from . import __version__
from .config import settings, ensure_dirs, reload_from_env
from .importer import AccioClient
from .jobs import enqueue_ingest_jobs
from .pdf_utils import process_file_to_payload
from .queue import JobQueue
from .settings_store import load_env_file, masked_preview, save_env_file
from .s3uploader import S3Uploader
from .watcher import WatchRunner

ALLOWED_MANUAL_EXTS = {".pdf", ".jpg", ".jpeg", ".png"}
SHA_PLACEHOLDER = "—"
ELLIPSIS = "…"


class DropTable(QTableWidget):
    """Table widget that accepts drag-and-drop of supported files."""

    def __init__(self, on_files: Callable[[list[Path]], None], *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._on_files = on_files
        self.setAcceptDrops(True)

    def dragEnterEvent(self, event) -> None:  # type: ignore[override]
        if event.mimeData().hasUrls():
            event.acceptProposedAction()

    def dragMoveEvent(self, event) -> None:  # type: ignore[override]
        if event.mimeData().hasUrls():
            event.acceptProposedAction()

    def dropEvent(self, event) -> None:  # type: ignore[override]
        from pathlib import Path as _Path

        paths: list[_Path] = []
        for url in event.mimeData().urls():
            p = _Path(url.toLocalFile())
            if p.exists() and p.is_file():
                paths.append(p)
        if paths:
            self._on_files(paths)
            event.acceptProposedAction()


def nice_card(widget: QWidget) -> QWidget:
    frame = QFrame()
    frame.setObjectName("Card")
    layout = QVBoxLayout(frame)
    layout.setContentsMargins(16, 16, 16, 16)
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
        self.setWindowTitle("ByteVault Ingestor")
        self.setWindowIcon(QIcon(str(Path(__file__).parent / "assets" / "icon.png")))
        self.resize(1080, 760)

        self.queue = JobQueue()
        self.runner: Optional[WatchRunner] = None
        self.heartbeat_timer = QTimer()
        self.heartbeat_timer.timeout.connect(self.on_heartbeat)

        self._staged_paths: list[Path] = []
        self._staged_lookup: set[str] = set()
        self._nav_lists: list[QListWidget] = []
        self._page_indexes: dict[str, int] = {}
        self._nav_signal_guard = False

        central = QWidget()
        main_layout = QHBoxLayout(central)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)

        nav_container = QWidget()
        nav_container.setObjectName("SideNav")
        nav_container.setFixedWidth(240)
        self._nav_layout = QVBoxLayout(nav_container)
        self._nav_layout.setContentsMargins(20, 24, 20, 24)
        self._nav_layout.setSpacing(16)

        header = QWidget()
        header_layout = QHBoxLayout(header)
        header_layout.setContentsMargins(0, 0, 0, 0)
        header_layout.setSpacing(12)
        logo_label = QLabel()
        pixmap = QPixmap(str(Path(__file__).parent / "assets" / "icon.png"))
        if not pixmap.isNull():
            logo_label.setPixmap(pixmap.scaled(32, 32, Qt.AspectRatioMode.KeepAspectRatio, Qt.TransformationMode.SmoothTransformation))
        title_label = QLabel("ByteVault Ingestor")
        title_label.setStyleSheet("font-size: 18px; font-weight: 600;")
        header_layout.addWidget(logo_label)
        header_layout.addWidget(title_label)
        header_layout.addStretch(1)
        self._nav_layout.addWidget(header)

        nav_structure = [
            ("GENERAL", [("Dashboard", "dashboard")]),
            ("INGEST", [("Manual Upload", "manual_upload"), ("Watch Folder", "watch_folder")]),
            ("INTEGRATIONS", [("Accio", "accio")]),
            ("SYSTEM", [("Logs", "logs"), ("Help / About", "help")]),
        ]
        for section, entries in nav_structure:
            label = QLabel(section)
            label.setProperty("class", "section")
            self._nav_layout.addWidget(label)
            nav_list = QListWidget()
            nav_list.setSelectionMode(QListWidget.SelectionMode.SingleSelection)
            nav_list.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
            nav_list.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
            nav_list.setFrameShape(QFrame.Shape.NoFrame)
            nav_list.setFocusPolicy(Qt.FocusPolicy.NoFocus)
            nav_list.setSpacing(4)
            for text, key in entries:
                item = QListWidgetItem(text)
                item.setData(Qt.ItemDataRole.UserRole, key)
                nav_list.addItem(item)
            nav_list.currentItemChanged.connect(self._on_nav_item_changed)
            self._nav_lists.append(nav_list)
            self._nav_layout.addWidget(nav_list)

        self._nav_layout.addStretch(1)

        self.stack = QStackedWidget()
        content_wrapper = QWidget()
        content_layout = QVBoxLayout(content_wrapper)
        content_layout.setContentsMargins(24, 24, 24, 24)
        content_layout.setSpacing(0)
        content_layout.addWidget(self.stack)

        main_layout.addWidget(nav_container)
        main_layout.addWidget(content_wrapper, 1)
        self.setCentralWidget(central)

        self._register_page("dashboard", self._build_dashboard_page())
        self._register_page("manual_upload", self._build_manual_upload_page())
        self._register_page("watch_folder", self._build_watch_folder_page())
        self._register_page("accio", self._build_accio_page())
        self._register_page("logs", self._build_logs_page())
        self._register_page("help", self._build_help_page())
        self._select_page("dashboard")

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

    # ---------- Page registration / navigation ----------
    def _register_page(self, key: str, widget: QWidget) -> None:
        index = self.stack.addWidget(widget)
        self._page_indexes[key] = index

    def _select_page(self, key: str) -> None:
        if key not in self._page_indexes:
            return
        self.stack.setCurrentIndex(self._page_indexes[key])
        self._nav_signal_guard = True
        try:
            for nav in self._nav_lists:
                matched = False
                for row in range(nav.count()):
                    item = nav.item(row)
                    if item.data(Qt.ItemDataRole.UserRole) == key:
                        nav.setCurrentRow(row)
                        matched = True
                        break
                if not matched:
                    nav.clearSelection()
                    nav.setCurrentRow(-1)
        finally:
            self._nav_signal_guard = False

    def _on_nav_item_changed(self, current: Optional[QListWidgetItem]) -> None:
        if self._nav_signal_guard or current is None:
            return
        key = current.data(Qt.ItemDataRole.UserRole)
        if key:
            self._select_page(key)

    # ---------- DASHBOARD ----------
    def _build_dashboard_page(self) -> QWidget:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(16)

        intro = QLabel(
            "<b>Welcome to ByteVault Ingestor</b><br>"
            "Configure notifications and storage, then use the sidebar to start ingesting."
        )
        intro.setWordWrap(True)
        layout.addWidget(nice_card(intro))

        form_widget = QWidget()
        form = QFormLayout(form_widget)
        form.setFieldGrowthPolicy(QFormLayout.FieldGrowthPolicy.AllNonFixedFieldsGrow)

        self.inp_license = QLineEdit(settings.LICENSE_KEY or "")
        form.addRow(QLabel("License key"), self.inp_license)

        self.inp_slack = QLineEdit(str(settings.SLACK_WEBHOOK_URL or ""))
        btn_slack_test = QPushButton("Test Slack")
        btn_slack_test.setProperty("class", "primary")
        btn_slack_test.clicked.connect(self._test_slack)
        row_slack = QWidget()
        hs = QHBoxLayout(row_slack)
        hs.setContentsMargins(0, 0, 0, 0)
        hs.setSpacing(8)
        hs.addWidget(self.inp_slack)
        hs.addWidget(btn_slack_test)
        form.addRow(QLabel("Slack webhook URL"), row_slack)

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
        row_tess = QWidget()
        ht = QHBoxLayout(row_tess)
        ht.setContentsMargins(0, 0, 0, 0)
        ht.setSpacing(8)
        ht.addWidget(self.inp_tess)
        ht.addWidget(btn_tess)
        form.addRow(QLabel("Tesseract path"), row_tess)

        layout.addWidget(nice_card(form_widget))

        actions = QWidget()
        ha = QHBoxLayout(actions)
        ha.setContentsMargins(0, 0, 0, 0)
        ha.setSpacing(12)
        self.chk_save_env = QCheckBox("Save to .env in project root")
        self.chk_save_env.setChecked(True)
        btn_save = QPushButton("Save settings")
        btn_save.setProperty("class", "primary")
        btn_reload = QPushButton("Reload from .env")
        btn_s3_test = QPushButton("Test S3")
        btn_s3_test.setProperty("class", "primary")
        btn_save.clicked.connect(self.on_save_settings)
        btn_reload.clicked.connect(self.on_reload_settings)
        btn_s3_test.clicked.connect(self._test_s3)
        ha.addWidget(self.chk_save_env)
        ha.addStretch(1)
        ha.addWidget(btn_save)
        ha.addWidget(btn_reload)
        ha.addWidget(btn_s3_test)
        layout.addWidget(nice_card(actions))
        layout.addStretch(1)
        return page

    # ---------- MANUAL UPLOAD ----------
    def _build_manual_upload_page(self) -> QWidget:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(16)

        info = QLabel(
            "Use the button or drag and drop to stage PDF and image documents for ingestion."
        )
        info.setWordWrap(True)
        layout.addWidget(nice_card(info))

        controls = QWidget()
        controls_layout = QHBoxLayout(controls)
        controls_layout.setContentsMargins(0, 0, 0, 0)
        controls_layout.setSpacing(10)
        btn_add = QPushButton("Add files…")
        btn_add.setProperty("class", "primary")
        btn_add.clicked.connect(self._prompt_add_files)
        btn_start = QPushButton("Start Ingest")
        btn_start.setProperty("class", "primary")
        btn_start.clicked.connect(self.on_start_ingest)
        btn_remove = QPushButton("Remove Selected")
        btn_remove.setProperty("class", "danger")
        btn_remove.clicked.connect(self._on_remove_selected_staged)
        btn_clear = QPushButton("Clear All")
        btn_clear.setProperty("class", "danger")
        btn_clear.clicked.connect(self._on_clear_staged)
        controls_layout.addWidget(btn_add)
        controls_layout.addWidget(btn_start)
        controls_layout.addStretch(1)
        controls_layout.addWidget(btn_remove)
        controls_layout.addWidget(btn_clear)
        layout.addWidget(nice_card(controls))

        self.manual_table = DropTable(self._staged_add_many)
        self.manual_table.setColumnCount(5)
        self.manual_table.setHorizontalHeaderLabels(["Filename", "Size", "Type", "SHA-256", "Status"])
        self.manual_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.manual_table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.manual_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.manual_table.verticalHeader().setVisible(False)
        header = self.manual_table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        for i in range(1, 5):
            header.setSectionResizeMode(i, QHeaderView.ResizeMode.ResizeToContents)
        layout.addWidget(nice_card(self.manual_table), 1)
        return page

    # ---------- WATCH FOLDER ----------
    def _build_watch_folder_page(self) -> QWidget:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(16)

        folders = QWidget()
        fform = QFormLayout(folders)
        fform.setFieldGrowthPolicy(QFormLayout.FieldGrowthPolicy.AllNonFixedFieldsGrow)

        self.inp_watch = QLineEdit(settings.WATCH_DIR)
        self.inp_processed = QLineEdit(settings.PROCESSED_DIR)
        self.inp_failed = QLineEdit(settings.FAILED_DIR)
        btn_watch = QPushButton("Choose…")
        btn_proc = QPushButton("Choose…")
        btn_fail = QPushButton("Choose…")
        btn_watch.clicked.connect(lambda: self._pick_dir(self.inp_watch))
        btn_proc.clicked.connect(lambda: self._pick_dir(self.inp_processed))
        btn_fail.clicked.connect(lambda: self._pick_dir(self.inp_failed))

        frow1 = QWidget()
        h1 = QHBoxLayout(frow1)
        h1.setContentsMargins(0, 0, 0, 0)
        h1.setSpacing(8)
        h1.addWidget(self.inp_watch)
        h1.addWidget(btn_watch)

        frow2 = QWidget()
        h2 = QHBoxLayout(frow2)
        h2.setContentsMargins(0, 0, 0, 0)
        h2.setSpacing(8)
        h2.addWidget(self.inp_processed)
        h2.addWidget(btn_proc)

        frow3 = QWidget()
        h3 = QHBoxLayout(frow3)
        h3.setContentsMargins(0, 0, 0, 0)
        h3.setSpacing(8)
        h3.addWidget(self.inp_failed)
        h3.addWidget(btn_fail)

        fform.addRow(QLabel("Watch folder"), frow1)
        fform.addRow(QLabel("Processed folder"), frow2)
        fform.addRow(QLabel("Failed folder"), frow3)
        layout.addWidget(nice_card(folders))

        controls = QWidget()
        ch = QHBoxLayout(controls)
        ch.setContentsMargins(0, 0, 0, 0)
        ch.setSpacing(12)
        self.btn_start = QPushButton("Start Watching")
        self.btn_start.setProperty("class", "primary")
        self.btn_stop = QPushButton("Stop")
        self.btn_stop.setProperty("class", "danger")
        self.btn_start.clicked.connect(self.on_start)
        self.btn_stop.clicked.connect(self.on_stop)
        ch.addWidget(self.btn_start)
        ch.addWidget(self.btn_stop)
        layout.addWidget(nice_card(controls))

        self.log = QTextEdit()
        self.log.setReadOnly(True)
        layout.addWidget(nice_card(self.log), 1)
        return page

    # ---------- ACCIO ----------
    def _build_accio_page(self) -> QWidget:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(16)

        info = QLabel(
            "Configure the Accio integration used for API-based ingestion."
        )
        info.setWordWrap(True)
        layout.addWidget(nice_card(info))

        form_widget = QWidget()
        form = QFormLayout(form_widget)
        form.setFieldGrowthPolicy(QFormLayout.FieldGrowthPolicy.AllNonFixedFieldsGrow)

        self.inp_accio_url = QLineEdit(str(settings.ACCIO_ENDPOINT))
        self.inp_accio_token = QLineEdit(settings.ACCIO_TOKEN or "")
        self.inp_accio_token.setEchoMode(QLineEdit.EchoMode.Password)

        form.addRow(QLabel("Accio endpoint"), self.inp_accio_url)
        form.addRow(QLabel("Accio token"), self.inp_accio_token)
        layout.addWidget(nice_card(form_widget))

        actions = QWidget()
        ha = QHBoxLayout(actions)
        ha.setContentsMargins(0, 0, 0, 0)
        ha.setSpacing(12)
        btn_test = QPushButton("Test")
        btn_test.setProperty("class", "primary")
        btn_save = QPushButton("Save")
        btn_save.setProperty("class", "primary")
        btn_test.clicked.connect(self._test_accio)
        btn_save.clicked.connect(self.on_save_settings)
        ha.addStretch(1)
        ha.addWidget(btn_test)
        ha.addWidget(btn_save)
        layout.addWidget(nice_card(actions))
        layout.addStretch(1)
        return page

    # ---------- LOGS ----------
    def _build_logs_page(self) -> QWidget:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(16)

        info = QLabel("View recent log output and open the logs directory for deeper inspection.")
        info.setWordWrap(True)
        layout.addWidget(nice_card(info))

        controls = QWidget()
        hc = QHBoxLayout(controls)
        hc.setContentsMargins(0, 0, 0, 0)
        hc.setSpacing(12)
        btn_refresh = QPushButton("Refresh")
        btn_refresh.setProperty("class", "primary")
        btn_refresh.clicked.connect(self._load_logs)
        btn_open = QPushButton("Open logs folder")
        btn_open.setProperty("class", "primary")
        btn_open.clicked.connect(self._open_logs_folder)
        hc.addWidget(btn_refresh)
        hc.addStretch(1)
        hc.addWidget(btn_open)
        layout.addWidget(nice_card(controls))

        self.logs_view = QTextEdit()
        self.logs_view.setReadOnly(True)
        layout.addWidget(nice_card(self.logs_view), 1)
        self._load_logs()
        return page

    # ---------- HELP ----------
    def _build_help_page(self) -> QWidget:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(16)

        about_widget = QWidget()
        about_layout = QVBoxLayout(about_widget)
        about_layout.setContentsMargins(0, 0, 0, 0)
        about_layout.setSpacing(6)
        title = QLabel("<h2>ByteVault Ingestor</h2>")
        subtitle = QLabel(f"Version {__version__}")
        blurb = QLabel(
            "A secure desktop companion for staging, validating, and uploading documents to ByteVault services."
        )
        blurb.setWordWrap(True)
        about_layout.addWidget(title)
        about_layout.addWidget(subtitle)
        about_layout.addWidget(blurb)

        buttons = QWidget()
        hb = QHBoxLayout(buttons)
        hb.setContentsMargins(0, 0, 0, 0)
        hb.setSpacing(12)
        btn_env = QPushButton("Open .env")
        btn_env.setProperty("class", "primary")
        btn_env.clicked.connect(self._open_env_file)
        btn_docs = QPushButton("Open docs")
        btn_docs.setProperty("class", "primary")
        btn_docs.clicked.connect(self._open_docs)
        hb.addWidget(btn_env)
        hb.addWidget(btn_docs)
        hb.addStretch(1)
        about_layout.addWidget(buttons)
        layout.addWidget(nice_card(about_widget))

        diag_widget = QWidget()
        diag_layout = QVBoxLayout(diag_widget)
        diag_layout.setContentsMargins(0, 0, 0, 0)
        diag_layout.setSpacing(8)
        self.diag_view = QTextEdit()
        self.diag_view.setReadOnly(True)
        btn_refresh = QPushButton("Refresh diagnostics")
        btn_refresh.setProperty("class", "primary")
        btn_refresh.clicked.connect(self._refresh_diagnostics)
        diag_layout.addWidget(self.diag_view)
        diag_layout.addWidget(btn_refresh, alignment=Qt.AlignmentFlag.AlignRight)
        layout.addWidget(nice_card(diag_widget), 1)

        self._refresh_diagnostics()
        return page

    # ---------- Helpers ----------
    def _pick_dir(self, target: QLineEdit) -> None:
        directory = QFileDialog.getExistingDirectory(self, "Choose folder")
        if directory:
            target.setText(directory)

    def _pick_file(self, target: QLineEdit) -> None:
        file_path, _ = QFileDialog.getOpenFileName(self, "Choose file")
        if file_path:
            target.setText(file_path)

    def _append_log(self, text: str) -> None:
        self.log.append(text)

    def on_heartbeat(self) -> None:
        self.job_count_label.setText(f"Jobs: {self.queue.count_jobs()}")
        self.heartbeat_label.setText("❤ running")

    # ---------- Manual upload helpers ----------
    def _prompt_add_files(self) -> None:
        files, _ = QFileDialog.getOpenFileNames(
            self,
            "Select documents",
            "",
            "Documents (*.pdf *.PDF *.jpg *.jpeg *.png)",
        )
        if files:
            self._staged_add_many([Path(f) for f in files])

    def _staged_add_many(self, paths: list[Path]) -> None:
        added = False
        for path in paths:
            if not path.exists() or not path.is_file():
                continue
            if path.suffix.lower() not in ALLOWED_MANUAL_EXTS:
                continue
            resolved = str(path.resolve())
            if resolved in self._staged_lookup:
                continue
            self._staged_paths.append(path)
            self._staged_lookup.add(resolved)
            row = self.manual_table.rowCount()
            self.manual_table.insertRow(row)
            name_item = self._table_item(path.name)
            name_item.setData(Qt.ItemDataRole.UserRole, resolved)
            self.manual_table.setItem(row, 0, name_item)
            try:
                size = path.stat().st_size
            except OSError:
                size = 0
            self.manual_table.setItem(row, 1, self._table_item(self._format_size(size)))
            self.manual_table.setItem(row, 2, self._table_item(path.suffix.lstrip(".").upper()))
            self.manual_table.setItem(row, 3, self._table_item(SHA_PLACEHOLDER))
            self.manual_table.setItem(row, 4, self._table_item("Queued"))
            added = True
        if added:
            self.manual_table.resizeColumnsToContents()

    def _on_remove_selected_staged(self) -> None:
        selection = self.manual_table.selectionModel().selectedRows()
        if not selection:
            return
        for index in sorted(selection, key=lambda idx: idx.row(), reverse=True):
            row = index.row()
            if 0 <= row < len(self._staged_paths):
                resolved = str(self._staged_paths[row].resolve())
                self._staged_lookup.discard(resolved)
                self._staged_paths.pop(row)
            self.manual_table.removeRow(row)

    def _on_clear_staged(self) -> None:
        self.manual_table.setRowCount(0)
        self._staged_paths.clear()
        self._staged_lookup.clear()

    def on_start_ingest(self) -> None:
        if not self._staged_paths:
            QMessageBox.information(self, "Manual Upload", "Add files to ingest before starting.")
            return
        any_failure = False
        for row, path in enumerate(list(self._staged_paths)):
            sha_item = self.manual_table.item(row, 3)
            status_item = self.manual_table.item(row, 4)
            if status_item is None:
                continue
            try:
                payload, original_bytes, content_type, sha256 = process_file_to_payload(path)
                enqueue_ingest_jobs(self.queue, payload, sha256, original_bytes, content_type, path.name)
                if sha_item is not None:
                    sha_item.setText(f"{sha256[:12]}{ELLIPSIS}")
                status_item.setText("Enqueued")
                self._append_log(f"[INFO] manual enqueue: {path.name}")
            except Exception:
                status_item.setText("Failed")
                any_failure = True
                self._append_log(f"[ERROR] manual enqueue failed for {path.name}")
        if any_failure:
            QMessageBox.warning(self, "Manual Upload", "Some files failed to enqueue. Check the status column.")
        else:
            QMessageBox.information(self, "Manual Upload", "All files enqueued for ingestion.")

    # ---------- Logs & Help helpers ----------
    def _load_logs(self) -> None:
        log_path = Path(settings.LOG_FILE)
        if log_path.exists():
            try:
                content = log_path.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                content = "Unable to read log file."
        else:
            content = "Log file not found."
        self.logs_view.setPlainText(content)

    def _open_logs_folder(self) -> None:
        log_path = Path(settings.LOG_FILE)
        QDesktopServices.openUrl(QUrl.fromLocalFile(str(log_path.resolve().parent)))

    def _open_env_file(self) -> None:
        env_path = Path(".env")
        if not env_path.exists():
            env_path.touch()
        QDesktopServices.openUrl(QUrl.fromLocalFile(str(env_path.resolve())))

    def _open_docs(self) -> None:
        docs_path = Path("docs")
        if docs_path.exists():
            QDesktopServices.openUrl(QUrl.fromLocalFile(str(docs_path.resolve())))
            return
        readme = Path("README.md")
        if readme.exists():
            QDesktopServices.openUrl(QUrl.fromLocalFile(str(readme.resolve())))
        else:
            QMessageBox.information(self, "Documentation", "No documentation folder found.")

    def _refresh_diagnostics(self) -> None:
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
        self.diag_view.setPlainText(json.dumps(payload, indent=2))

    # ---------- Utility ----------
    @staticmethod
    def _table_item(text: str) -> QTableWidgetItem:
        item = QTableWidgetItem(text)
        item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsEditable)
        return item

    @staticmethod
    def _format_size(size: int) -> str:
        value = float(size)
        units = ["B", "KB", "MB", "GB", "TB"]
        for unit in units:
            if value < 1024 or unit == units[-1]:
                if unit == "B":
                    return f"{int(value)} B"
                return f"{value:.1f} {unit}"
            value /= 1024.0
        return f"{value:.1f} TB"

    # ---------- Tests ----------
    def _test_slack(self) -> None:
        url = self.inp_slack.text().strip()
        if not url:
            QMessageBox.warning(self, "Slack", "Webhook URL is empty.")
            return
        try:
            resp = requests.post(url, json={"text": "ByteVault Ingestor test message (sanitized)."}, timeout=10)
            if 200 <= resp.status_code < 300:
                QMessageBox.information(self, "Slack", "Sent test message (check your channel).")
            else:
                QMessageBox.critical(self, "Slack", f"Webhook returned HTTP {resp.status_code}")
        except Exception as exc:
            QMessageBox.critical(self, "Slack", f"Request failed: {exc}")

    def _test_accio(self) -> None:
        url = self.inp_accio_url.text().strip()
        tok = self.inp_accio_token.text().strip()
        old_url = os.environ.get("ACCIO_ENDPOINT")
        old_tok = os.environ.get("ACCIO_TOKEN")
        try:
            os.environ["ACCIO_ENDPOINT"] = url
            os.environ["ACCIO_TOKEN"] = tok
            reload_from_env()
            AccioClient().post_document({"ping": "ok"})
            QMessageBox.information(self, "Accio", "POST succeeded.")
        except Exception as exc:
            QMessageBox.critical(self, "Accio", f"POST failed: {exc}")
        finally:
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
            uploader = S3Uploader()
            key = f"{settings.S3_PREFIX.rstrip('/')}/__self_test.txt"
            uploader.put_object_with_retention(key, b"ok", "text/plain", {"source": "bytevault-ingestor"})
            QMessageBox.information(self, "S3", "PutObject succeeded.")
        except Exception as exc:
            QMessageBox.critical(self, "S3", f"S3 put failed: {exc}")

    def _update_license_gate(self) -> None:
        ok = valid_license(settings.LICENSE_KEY)
        base_title = "ByteVault Ingestor"
        self.btn_start.setEnabled(ok)
        self.setWindowTitle(base_title if ok else f"{base_title} — LICENSE REQUIRED")

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
        self.inp_watch.setText(settings.WATCH_DIR)
        self.inp_processed.setText(settings.PROCESSED_DIR)
        self.inp_failed.setText(settings.FAILED_DIR)
        self._update_license_gate()
        QMessageBox.information(self, "Settings", "Reloaded from .env.")

    # ---------- Start/Stop ----------
    def on_start(self) -> None:
        if not valid_license(settings.LICENSE_KEY):
            QMessageBox.critical(self, "License", "Enter a valid license in Dashboard to start.")
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
        self.heartbeat_label.setText("⏺ idle")


def run_gui() -> None:
    app = QApplication([])
    win = MainWindow()
    win.show()
    app.exec()
