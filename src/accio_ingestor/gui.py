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
    QSizePolicy,
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
    layout.setContentsMargins(24, 24, 24, 24)
    layout.setSpacing(0)
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
        self.setWindowIcon(QIcon(str(Path(__file__).parent / "assets" / "bytevault_logo.png")))
        self.resize(1080, 760)

        self.queue = JobQueue()
        self.runner: Optional[WatchRunner] = None
        self.heartbeat_timer = QTimer()
        self.heartbeat_timer.timeout.connect(self.on_heartbeat)

        self._staged_paths: list[Path] = []
        self._staged_lookup: set[str] = set()
        self._nav_order: list[str] = []
        self._page_indexes: dict[str, int] = {}
        self._page_titles: dict[str, str] = {}
        self._page_subtitles: dict[str, str] = {}
        self._nav_signal_guard = False

        central = QWidget()
        main_layout = QHBoxLayout(central)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)

        nav_container = QWidget()
        nav_container.setObjectName("SideNav")
        nav_container.setFixedWidth(240)
        self._nav_layout = QVBoxLayout(nav_container)
        self._nav_layout.setContentsMargins(24, 32, 24, 32)
        self._nav_layout.setSpacing(24)

        header = QWidget()
        header_layout = QHBoxLayout(header)
        header_layout.setContentsMargins(0, 0, 0, 0)
        header_layout.setSpacing(12)
        logo_label = QLabel()
        pixmap = QPixmap(str(Path(__file__).parent / "assets" / "bytevault_logo.png"))
        if not pixmap.isNull():
            logo_label.setPixmap(
                pixmap.scaled(
                    36,
                    36,
                    Qt.AspectRatioMode.KeepAspectRatio,
                    Qt.TransformationMode.SmoothTransformation,
                )
            )
        title_box = QVBoxLayout()
        title_box.setContentsMargins(0, 0, 0, 0)
        title_box.setSpacing(2)
        title_label = QLabel("bytevault")
        title_label.setObjectName("BrandName")
        subtitle_label = QLabel("Ingestor")
        subtitle_label.setObjectName("BrandTagline")
        title_box.addWidget(title_label)
        title_box.addWidget(subtitle_label)
        header_layout.addWidget(logo_label)
        header_layout.addLayout(title_box)
        header_layout.addStretch(1)
        self._nav_layout.addWidget(header)

        self.nav_list = QListWidget()
        self.nav_list.setObjectName("NavList")
        self.nav_list.setSelectionMode(QListWidget.SelectionMode.SingleSelection)
        self.nav_list.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self.nav_list.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self.nav_list.setFrameShape(QFrame.Shape.NoFrame)
        self.nav_list.setFocusPolicy(Qt.FocusPolicy.NoFocus)
        self.nav_list.setSpacing(6)
        self.nav_list.currentItemChanged.connect(self._on_nav_item_changed)
        self._nav_layout.addWidget(self.nav_list)
        self._nav_layout.addStretch(1)

        self.stack = QStackedWidget()
        content_wrapper = QWidget()
        self._content_layout = QVBoxLayout(content_wrapper)
        self._content_layout.setContentsMargins(40, 32, 40, 32)
        self._content_layout.setSpacing(16)

        self._page_header = QWidget()
        page_header_layout = QHBoxLayout(self._page_header)
        page_header_layout.setContentsMargins(0, 0, 0, 0)
        page_header_layout.setSpacing(12)

        title_container = QVBoxLayout()
        title_container.setContentsMargins(0, 0, 0, 0)
        title_container.setSpacing(4)
        self.page_title_label = QLabel("")
        self.page_title_label.setObjectName("PageTitle")
        self.page_subtitle_label = QLabel("")
        self.page_subtitle_label.setObjectName("PageSubtitle")
        self.page_subtitle_label.setWordWrap(True)
        self.page_subtitle_label.setVisible(False)
        title_container.addWidget(self.page_title_label)
        title_container.addWidget(self.page_subtitle_label)
        page_header_layout.addLayout(title_container, 1)

        status_container = QHBoxLayout()
        status_container.setContentsMargins(0, 0, 0, 0)
        status_container.setSpacing(8)
        self.status_badge = QLabel("idle")
        self.status_badge.setObjectName("StatusBadge")
        self.status_badge.setProperty("state", "idle")
        self.job_badge = QLabel("0 jobs")
        self.job_badge.setObjectName("JobBadge")
        status_container.addWidget(self.status_badge)
        status_container.addWidget(self.job_badge)
        page_header_layout.addLayout(status_container)

        self._content_layout.addWidget(self._page_header)
        self._content_layout.addWidget(self.stack, 1)

        main_layout.addWidget(nav_container)
        main_layout.addWidget(content_wrapper, 1)
        self.setCentralWidget(central)

        pages: list[tuple[str, str, str, Callable[[], QWidget]]] = [
            (
                "Settings",
                "Configure core ByteVault options and directories.",
                "settings",
                self._build_settings_page,
            ),
            (
                "Manual Upload",
                "Stage and enqueue PDFs or images for ingestion.",
                "manual_upload",
                self._build_manual_upload_page,
            ),
            (
                "Watch Folder",
                "Manage the automated folder watcher and review activity.",
                "watch_folder",
                self._build_watch_folder_page,
            ),
            (
                "Integrations",
                "Manage API connectors like Accio.",
                "integrations",
                self._build_integrations_page,
            ),
            (
                "Logs",
                "Inspect recent application logs and open the log directory.",
                "logs",
                self._build_logs_page,
            ),
            (
                "Help",
                "Version info, diagnostics, and useful shortcuts.",
                "help",
                self._build_help_page,
            ),
        ]

        for title, subtitle, key, builder in pages:
            self._register_page(key, builder(), title, subtitle)
            self._add_nav_item(title, key)

        self._select_page("settings")

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
    def _register_page(self, key: str, widget: QWidget, title: str, subtitle: str = "") -> None:
        index = self.stack.addWidget(widget)
        self._page_indexes[key] = index
        self._page_titles[key] = title
        self._page_subtitles[key] = subtitle

    def _add_nav_item(self, text: str, key: str) -> None:
        item = QListWidgetItem(text)
        item.setData(Qt.ItemDataRole.UserRole, key)
        self.nav_list.addItem(item)
        self._nav_order.append(key)

    def _select_page(self, key: str) -> None:
        if key not in self._page_indexes:
            return
        self.stack.setCurrentIndex(self._page_indexes[key])
        title = self._page_titles.get(key, "")
        subtitle = self._page_subtitles.get(key, "")
        self.page_title_label.setText(title)
        self.page_subtitle_label.setText(subtitle)
        self.page_subtitle_label.setVisible(bool(subtitle))
        self._nav_signal_guard = True
        try:
            if key in self._nav_order:
                row = self._nav_order.index(key)
                self.nav_list.setCurrentRow(row)
        finally:
            self._nav_signal_guard = False

    def _on_nav_item_changed(self, current: Optional[QListWidgetItem]) -> None:
        if self._nav_signal_guard or current is None:
            return
        key = current.data(Qt.ItemDataRole.UserRole)
        if key:
            self._select_page(key)


    # ---------- SETTINGS ----------
    def _build_settings_page(self) -> QWidget:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(24)

        # Watch folder + Accio connector card
        primary_content = QWidget()
        primary_layout = QVBoxLayout(primary_content)
        primary_layout.setContentsMargins(0, 0, 0, 0)
        primary_layout.setSpacing(24)

        watch_section = QWidget()
        watch_layout = QVBoxLayout(watch_section)
        watch_layout.setContentsMargins(0, 0, 0, 0)
        watch_layout.setSpacing(12)
        watch_layout.addWidget(
            self._section_header(
                "Watch Folder",
                "Monitor a folder for new documents to ingest automatically.",
            )
        )

        watch_form = QFormLayout()
        watch_form.setFieldGrowthPolicy(QFormLayout.FieldGrowthPolicy.AllNonFixedFieldsGrow)
        watch_form.setLabelAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
        watch_form.setFormAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop)

        self.inp_watch = QLineEdit(settings.WATCH_DIR)
        btn_watch = QPushButton("Browse…")
        btn_watch.clicked.connect(lambda: self._pick_dir(self.inp_watch))
        watch_form.addRow(QLabel("Folder path"), self._input_row(self.inp_watch, btn_watch))

        self.inp_processed = QLineEdit(settings.PROCESSED_DIR)
        btn_proc = QPushButton("Browse…")
        btn_proc.clicked.connect(lambda: self._pick_dir(self.inp_processed))
        watch_form.addRow(QLabel("Processed folder"), self._input_row(self.inp_processed, btn_proc))

        self.inp_failed = QLineEdit(settings.FAILED_DIR)
        btn_fail = QPushButton("Browse…")
        btn_fail.clicked.connect(lambda: self._pick_dir(self.inp_failed))
        watch_form.addRow(QLabel("Failed folder"), self._input_row(self.inp_failed, btn_fail))

        watch_layout.addLayout(watch_form)
        primary_layout.addWidget(watch_section)
        primary_layout.addWidget(self._card_divider())

        accio_section = QWidget()
        accio_layout = QVBoxLayout(accio_section)
        accio_layout.setContentsMargins(0, 0, 0, 0)
        accio_layout.setSpacing(12)
        accio_layout.addWidget(
            self._section_header(
                "Accio Connector",
                "Configure the API endpoint and token used for ingestion.",
            )
        )

        accio_form = QFormLayout()
        accio_form.setFieldGrowthPolicy(QFormLayout.FieldGrowthPolicy.AllNonFixedFieldsGrow)
        accio_form.setLabelAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
        accio_form.setFormAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop)

        self.inp_accio_url = QLineEdit(str(settings.ACCIO_ENDPOINT))
        self.inp_accio_token = QLineEdit(settings.ACCIO_TOKEN or "")
        self.inp_accio_token.setEchoMode(QLineEdit.EchoMode.Password)
        accio_form.addRow(QLabel("Webhook URL"), self.inp_accio_url)
        accio_form.addRow(QLabel("API token"), self.inp_accio_token)
        accio_layout.addLayout(accio_form)

        accio_actions = QWidget()
        accio_actions_layout = QHBoxLayout(accio_actions)
        accio_actions_layout.setContentsMargins(0, 0, 0, 0)
        accio_actions_layout.setSpacing(12)
        accio_actions_layout.addStretch(1)
        btn_accio_test = QPushButton("Test connection")
        btn_accio_test.setProperty("class", "primary")
        btn_accio_test.clicked.connect(self._test_accio)
        accio_actions_layout.addWidget(btn_accio_test)
        accio_layout.addWidget(accio_actions)

        primary_layout.addWidget(accio_section)
        layout.addWidget(nice_card(primary_content))

        # Detailed configuration card
        config_content = QWidget()
        config_layout = QVBoxLayout(config_content)
        config_layout.setContentsMargins(0, 0, 0, 0)
        config_layout.setSpacing(24)

        app_section = QWidget()
        app_layout = QVBoxLayout(app_section)
        app_layout.setContentsMargins(0, 0, 0, 0)
        app_layout.setSpacing(12)
        app_layout.addWidget(
            self._section_header(
                "Application",
                "License and notification preferences.",
            )
        )

        app_form = QFormLayout()
        app_form.setFieldGrowthPolicy(QFormLayout.FieldGrowthPolicy.AllNonFixedFieldsGrow)
        app_form.setLabelAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
        app_form.setFormAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop)

        self.inp_license = QLineEdit(settings.LICENSE_KEY or "")
        app_form.addRow(QLabel("License key"), self.inp_license)

        self.inp_slack = QLineEdit(str(settings.SLACK_WEBHOOK_URL or ""))
        btn_slack_test = QPushButton("Test Slack")
        btn_slack_test.setProperty("class", "primary")
        btn_slack_test.clicked.connect(self._test_slack)
        app_form.addRow(QLabel("Slack webhook URL"), self._input_row(self.inp_slack, btn_slack_test))
        app_layout.addLayout(app_form)

        config_layout.addWidget(app_section)
        config_layout.addWidget(self._card_divider())

        storage_section = QWidget()
        storage_layout = QVBoxLayout(storage_section)
        storage_layout.setContentsMargins(0, 0, 0, 0)
        storage_layout.setSpacing(12)
        storage_layout.addWidget(
            self._section_header(
                "Storage",
                "S3 configuration for processed assets.",
            )
        )

        storage_form = QFormLayout()
        storage_form.setFieldGrowthPolicy(QFormLayout.FieldGrowthPolicy.AllNonFixedFieldsGrow)
        storage_form.setLabelAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
        storage_form.setFormAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop)

        self.inp_region = QLineEdit(settings.AWS_REGION)
        self.inp_bucket = QLineEdit(settings.S3_BUCKET or "")
        self.inp_prefix = QLineEdit(settings.S3_PREFIX)
        self.inp_kms = QLineEdit(settings.S3_SSE_KMS_KEY_ID or "")
        self.inp_lock_mode = QLineEdit(settings.S3_OBJECT_LOCK_MODE or "")
        self.inp_lock_days = QLineEdit(str(settings.S3_OBJECT_LOCK_DAYS))
        self.inp_ak = QLineEdit(settings.AWS_ACCESS_KEY_ID or "")
        self.inp_sk = QLineEdit(settings.AWS_SECRET_ACCESS_KEY or "")
        self.inp_sk.setEchoMode(QLineEdit.EchoMode.Password)

        storage_form.addRow(QLabel("AWS region"), self.inp_region)
        storage_form.addRow(QLabel("S3 bucket"), self.inp_bucket)
        storage_form.addRow(QLabel("S3 prefix"), self.inp_prefix)
        storage_form.addRow(QLabel("SSE-KMS key ID"), self.inp_kms)
        storage_form.addRow(QLabel("Object Lock mode"), self.inp_lock_mode)
        storage_form.addRow(QLabel("Object Lock days"), self.inp_lock_days)
        storage_form.addRow(QLabel("AWS access key ID"), self.inp_ak)
        storage_form.addRow(QLabel("AWS secret access key"), self.inp_sk)
        storage_layout.addLayout(storage_form)

        config_layout.addWidget(storage_section)
        config_layout.addWidget(self._card_divider())

        ocr_section = QWidget()
        ocr_layout = QVBoxLayout(ocr_section)
        ocr_layout.setContentsMargins(0, 0, 0, 0)
        ocr_layout.setSpacing(12)
        ocr_layout.addWidget(
            self._section_header(
                "OCR",
                "Executable path for local Tesseract processing.",
            )
        )

        ocr_form = QFormLayout()
        ocr_form.setFieldGrowthPolicy(QFormLayout.FieldGrowthPolicy.AllNonFixedFieldsGrow)
        ocr_form.setLabelAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
        ocr_form.setFormAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop)

        self.inp_tess = QLineEdit(settings.TESSERACT_CMD or "")
        btn_tess = QPushButton("Browse…")
        btn_tess.clicked.connect(lambda: self._pick_file(self.inp_tess))
        ocr_form.addRow(QLabel("Tesseract path"), self._input_row(self.inp_tess, btn_tess))
        ocr_layout.addLayout(ocr_form)

        config_layout.addWidget(ocr_section)
        layout.addWidget(nice_card(config_content))

        actions = QWidget()
        actions_layout = QHBoxLayout(actions)
        actions_layout.setContentsMargins(0, 0, 0, 0)
        actions_layout.setSpacing(12)
        self.chk_save_env = QCheckBox("Save to .env in project root")
        self.chk_save_env.setChecked(True)
        btn_save = QPushButton("Save changes")
        btn_save.setProperty("class", "primary")
        btn_reload = QPushButton("Reload from .env")
        btn_s3_test = QPushButton("Test S3")
        btn_s3_test.setProperty("class", "primary")
        btn_save.clicked.connect(self.on_save_settings)
        btn_reload.clicked.connect(self.on_reload_settings)
        btn_s3_test.clicked.connect(self._test_s3)
        actions_layout.addWidget(self.chk_save_env)
        actions_layout.addStretch(1)
        actions_layout.addWidget(btn_save)
        actions_layout.addWidget(btn_reload)
        actions_layout.addWidget(btn_s3_test)
        layout.addWidget(nice_card(actions))
        layout.addStretch(1)
        return page
    # ---------- MANUAL UPLOAD ----------
    def _build_manual_upload_page(self) -> QWidget:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(24)

        card_content = QWidget()
        card_layout = QVBoxLayout(card_content)
        card_layout.setContentsMargins(0, 0, 0, 0)
        card_layout.setSpacing(16)

        card_layout.addWidget(
            self._section_header(
                "Manual Upload",
                "Drag in PDFs or images or add them manually, then enqueue to the ByteVault queue.",
            )
        )

        controls = QWidget()
        controls_layout = QHBoxLayout(controls)
        controls_layout.setContentsMargins(0, 0, 0, 0)
        controls_layout.setSpacing(12)
        btn_add = QPushButton("Add Files")
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
        card_layout.addWidget(controls)

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
        card_layout.addWidget(self.manual_table, 1)

        layout.addWidget(nice_card(card_content), 1)
        return page

    # ---------- WATCH FOLDER ----------
    def _build_watch_folder_page(self) -> QWidget:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(24)

        summary_content = QWidget()
        summary_layout = QVBoxLayout(summary_content)
        summary_layout.setContentsMargins(0, 0, 0, 0)
        summary_layout.setSpacing(16)

        summary_layout.addWidget(
            self._section_header(
                "Directories",
                "These paths are used for watching, processing, and failure routing.",
            )
        )

        def path_row(label_text: str, target_line: QLineEdit) -> QWidget:
            row = QWidget()
            row_layout = QHBoxLayout(row)
            row_layout.setContentsMargins(0, 0, 0, 0)
            row_layout.setSpacing(12)
            label = QLabel(label_text)
            value = QLabel(target_line.text() or "—")
            value.setObjectName("PathValue")
            value.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
            value.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
            value.setAlignment(Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignLeft)
            browse = QPushButton("Browse…")
            browse.clicked.connect(lambda: self._pick_dir(target_line))
            row_layout.addWidget(label)
            row_layout.addWidget(value, 1)
            row_layout.addWidget(browse)
            target_line.textChanged.connect(lambda text, lab=value: lab.setText(text or "—"))
            return row

        summary_layout.addWidget(path_row("Watch folder", self.inp_watch))
        summary_layout.addWidget(path_row("Processed folder", self.inp_processed))
        summary_layout.addWidget(path_row("Failed folder", self.inp_failed))

        layout.addWidget(nice_card(summary_content))

        control_content = QWidget()
        control_layout = QVBoxLayout(control_content)
        control_layout.setContentsMargins(0, 0, 0, 0)
        control_layout.setSpacing(16)

        control_layout.addWidget(
            self._section_header(
                "Watcher",
                "Start or stop the background watcher and review recent activity.",
            )
        )

        actions = QWidget()
        actions_layout = QHBoxLayout(actions)
        actions_layout.setContentsMargins(0, 0, 0, 0)
        actions_layout.setSpacing(12)
        self.btn_start = QPushButton("Start Watching")
        self.btn_start.setProperty("class", "primary")
        self.btn_stop = QPushButton("Stop")
        self.btn_stop.setProperty("class", "danger")
        self.btn_start.clicked.connect(self.on_start)
        self.btn_stop.clicked.connect(self.on_stop)
        actions_layout.addWidget(self.btn_start)
        actions_layout.addWidget(self.btn_stop)
        control_layout.addWidget(actions)

        self.log = QTextEdit()
        self.log.setReadOnly(True)
        control_layout.addWidget(self.log, 1)

        layout.addWidget(nice_card(control_content), 1)
        return page

    # ---------- INTEGRATIONS ----------
    def _build_integrations_page(self) -> QWidget:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(24)

        card_content = QWidget()
        card_layout = QVBoxLayout(card_content)
        card_layout.setContentsMargins(0, 0, 0, 0)
        card_layout.setSpacing(16)

        card_layout.addWidget(
            self._section_header(
                "Accio",
                "Manage the Accio API connector used for document ingestion.",
            )
        )

        self.accio_url_field = QLineEdit(self.inp_accio_url.text())
        self.accio_token_field = QLineEdit(self.inp_accio_token.text())
        self.accio_token_field.setEchoMode(QLineEdit.EchoMode.Password)

        form = QFormLayout()
        form.setFieldGrowthPolicy(QFormLayout.FieldGrowthPolicy.AllNonFixedFieldsGrow)
        form.setLabelAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
        form.setFormAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop)
        form.addRow(QLabel("Webhook URL"), self.accio_url_field)
        form.addRow(QLabel("API token"), self.accio_token_field)
        card_layout.addLayout(form)

        actions = QWidget()
        actions_layout = QHBoxLayout(actions)
        actions_layout.setContentsMargins(0, 0, 0, 0)
        actions_layout.setSpacing(12)
        actions_layout.addStretch(1)
        btn_test = QPushButton("Test connection")
        btn_test.setProperty("class", "primary")
        btn_save = QPushButton("Save changes")
        btn_save.setProperty("class", "primary")
        btn_test.clicked.connect(self._test_accio)
        btn_save.clicked.connect(self.on_save_settings)
        actions_layout.addWidget(btn_test)
        actions_layout.addWidget(btn_save)
        card_layout.addWidget(actions)

        layout.addWidget(nice_card(card_content))
        layout.addStretch(1)

        self._sync_line_edits(self.inp_accio_url, self.accio_url_field)
        self._sync_line_edits(self.inp_accio_token, self.accio_token_field)

        return page
    # ---------- LOGS ----------
    def _build_logs_page(self) -> QWidget:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(24)

        card_content = QWidget()
        card_layout = QVBoxLayout(card_content)
        card_layout.setContentsMargins(0, 0, 0, 0)
        card_layout.setSpacing(16)

        card_layout.addWidget(
            self._section_header(
                "Application logs",
                "Review recent entries and jump directly to the log directory.",
            )
        )

        controls = QWidget()
        controls_layout = QHBoxLayout(controls)
        controls_layout.setContentsMargins(0, 0, 0, 0)
        controls_layout.setSpacing(12)
        btn_refresh = QPushButton("Refresh")
        btn_refresh.setProperty("class", "primary")
        btn_refresh.clicked.connect(self._load_logs)
        btn_open = QPushButton("Open logs folder")
        btn_open.setProperty("class", "primary")
        btn_open.clicked.connect(self._open_logs_folder)
        controls_layout.addWidget(btn_refresh)
        controls_layout.addStretch(1)
        controls_layout.addWidget(btn_open)
        card_layout.addWidget(controls)

        self.logs_view = QTextEdit()
        self.logs_view.setReadOnly(True)
        card_layout.addWidget(self.logs_view, 1)

        layout.addWidget(nice_card(card_content), 1)
        self._load_logs()
        return page

    # ---------- HELP ----------
    def _build_help_page(self) -> QWidget:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(24)

        card_content = QWidget()
        card_layout = QVBoxLayout(card_content)
        card_layout.setContentsMargins(0, 0, 0, 0)
        card_layout.setSpacing(16)

        card_layout.addWidget(
            self._section_header(
                "Help & about",
                "Quick links, build information, and diagnostic context for support.",
            )
        )

        about_box = QWidget()
        about_layout = QVBoxLayout(about_box)
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
        card_layout.addWidget(about_box)

        buttons = QWidget()
        buttons_layout = QHBoxLayout(buttons)
        buttons_layout.setContentsMargins(0, 0, 0, 0)
        buttons_layout.setSpacing(12)
        btn_env = QPushButton("Open .env")
        btn_env.setProperty("class", "primary")
        btn_env.clicked.connect(self._open_env_file)
        btn_docs = QPushButton("Open docs")
        btn_docs.setProperty("class", "primary")
        btn_docs.clicked.connect(self._open_docs)
        buttons_layout.addWidget(btn_env)
        buttons_layout.addWidget(btn_docs)
        buttons_layout.addStretch(1)
        card_layout.addWidget(buttons)

        self.diag_view = QTextEdit()
        self.diag_view.setReadOnly(True)
        card_layout.addWidget(self.diag_view, 1)

        btn_refresh = QPushButton("Refresh diagnostics")
        btn_refresh.setProperty("class", "primary")
        btn_refresh.clicked.connect(self._refresh_diagnostics)
        card_layout.addWidget(btn_refresh, alignment=Qt.AlignmentFlag.AlignRight)

        layout.addWidget(nice_card(card_content), 1)

        self._refresh_diagnostics()
        return page

    # ---------- Helpers ----------
    @staticmethod
    def _card_divider() -> QFrame:
        divider = QFrame()
        divider.setObjectName("CardDivider")
        divider.setFrameShape(QFrame.Shape.HLine)
        divider.setFrameShadow(QFrame.Shadow.Plain)
        divider.setLineWidth(1)
        return divider

    @staticmethod
    def _input_row(field: QLineEdit, button: QPushButton) -> QWidget:
        row = QWidget()
        layout = QHBoxLayout(row)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(8)
        layout.addWidget(field)
        layout.addWidget(button)
        return row

    @staticmethod
    def _sync_line_edits(a: QLineEdit, b: QLineEdit) -> None:
        def link(source: QLineEdit, target: QLineEdit) -> Callable[[str], None]:
            def _update(text: str) -> None:
                if target.text() == text:
                    return
                target.blockSignals(True)
                target.setText(text)
                target.blockSignals(False)

            return _update

        a.textChanged.connect(link(a, b))
        b.textChanged.connect(link(b, a))

    @staticmethod
    def _section_header(title: str, subtitle: str = "") -> QWidget:
        container = QWidget()
        layout = QVBoxLayout(container)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(4)
        heading = QLabel(title)
        heading.setObjectName("CardTitle")
        layout.addWidget(heading)
        if subtitle:
            sub = QLabel(subtitle)
            sub.setObjectName("CardSubtitle")
            sub.setWordWrap(True)
            layout.addWidget(sub)
        return container

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

    def _set_job_badge(self, count: int) -> None:
        self.job_badge.setText(f"{count} job{'s' if count != 1 else ''}")

    def _update_status_badge(self, active: bool) -> None:
        state = "active" if active else "idle"
        self.status_badge.setText("running" if active else "idle")
        self.status_badge.setProperty("state", state)
        self.status_badge.style().unpolish(self.status_badge)
        self.status_badge.style().polish(self.status_badge)
        self.status_badge.update()

    def on_heartbeat(self) -> None:
        count = self.queue.count_jobs()
        self.job_count_label.setText(f"Jobs: {count}")
        self._set_job_badge(count)
        active = self.runner is not None or count > 0
        self.heartbeat_label.setText("❤ running" if active else "⏺ idle")
        self._update_status_badge(active)

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
        self.heartbeat_label.setText("❤ running")
        self._update_status_badge(True)
        self._set_job_badge(self.queue.count_jobs())
        self._append_log("[INFO] watcher started")

    def on_stop(self) -> None:
        if self.runner:
            self.runner.stop()
            self.runner = None
            self._append_log("[INFO] watcher stopped")
        self.heartbeat_timer.stop()
        self.heartbeat_label.setText("⏺ idle")
        count = self.queue.count_jobs()
        self._set_job_badge(count)
        self._update_status_badge(count > 0)


def run_gui() -> None:
    app = QApplication([])
    win = MainWindow()
    win.show()
    app.exec()
