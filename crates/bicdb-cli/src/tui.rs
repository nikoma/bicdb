use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Result};
use bicdb_core::{
    create_backup, restore_backup, verify_backup, BackupCreateOptions, BackupRestoreOptions, BicDb,
    BrokerStats, CollectionStats, DbConfig, DbStats, DrainOptions, EncryptionConfig, GroupStats,
    OperationalMetrics, PeekedMessage, PublishOptions, QueueConfig, QueueStats, StoredEvent,
    DEFAULT_COLLECTION_CATALOG, DEFAULT_SEGMENTS_DIR,
};
use bicdb_sql::{SqlEngine, SqlResult, SqlValue};
use crossterm::{
    event::{self, Event, KeyCode, KeyEvent, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Layout, Rect},
    style::{Color, Modifier, Style},
    symbols,
    text::{Line, Span},
    widgets::{
        Axis, Block, BorderType, Borders, Cell, Chart, Dataset, Gauge, GraphType, Paragraph, Row,
        Sparkline, Table, Tabs, Wrap,
    },
    Frame, Terminal,
};

const MAX_RESULT_ROWS: usize = 500;
const MAX_PREVIEW_ROWS: usize = 50;
const MAX_HISTORY: usize = 200;
const BLUE: Color = Color::Rgb(32, 118, 190);
const CYAN: Color = Color::Rgb(0, 190, 210);
const GREEN: Color = Color::Rgb(40, 190, 110);
const AMBER: Color = Color::Rgb(226, 170, 45);
const RED: Color = Color::Rgb(235, 40, 52);
const VIOLET: Color = Color::Rgb(150, 110, 220);
const DIM: Color = Color::Rgb(115, 128, 140);
const PANEL_BG: Color = Color::Rgb(7, 12, 18);

#[derive(Clone, Debug, PartialEq, Eq)]
enum AdminStatement {
    CreateDatabase { name: String, if_not_exists: bool },
}

type TuiTerminal = Terminal<CrosstermBackend<io::Stdout>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Tab {
    Overview,
    Collections,
    Data,
    Broker,
    Sql,
    Activity,
    Operations,
    Help,
}

impl Tab {
    fn all() -> &'static [Self] {
        &[
            Self::Overview,
            Self::Collections,
            Self::Data,
            Self::Broker,
            Self::Sql,
            Self::Activity,
            Self::Operations,
            Self::Help,
        ]
    }

    fn title(self) -> &'static str {
        match self {
            Self::Overview => "Overview",
            Self::Collections => "Collections",
            Self::Data => "Data",
            Self::Broker => "Broker",
            Self::Sql => "SQL",
            Self::Activity => "Activity",
            Self::Operations => "Ops",
            Self::Help => "Help",
        }
    }
}

#[derive(Clone, Debug)]
struct QueryHistoryEntry {
    index: usize,
    sql: String,
    elapsed_ms: u64,
    rows: usize,
    status: String,
    timestamp: i64,
}

#[derive(Clone, Debug)]
struct StatusMessage {
    text: String,
    error: bool,
}

pub(crate) fn run_tui(
    path: PathBuf,
    db: BicDb,
    encryption: Option<EncryptionConfig>,
) -> Result<()> {
    let mut terminal = setup_terminal()?;
    let result = TuiApp::new(path, db, encryption).and_then(|mut app| app.run(&mut terminal));
    restore_terminal(&mut terminal)?;
    result
}

fn setup_terminal() -> Result<TuiTerminal> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    terminal.clear()?;
    Ok(terminal)
}

fn restore_terminal(terminal: &mut TuiTerminal) -> Result<()> {
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    Ok(())
}

struct TuiApp {
    path: PathBuf,
    db: BicDb,
    encryption: Option<EncryptionConfig>,
    tab: Tab,
    selected_collection: usize,
    input: String,
    editing: bool,
    result: Option<SqlResult>,
    result_scroll: usize,
    history: Vec<QueryHistoryEntry>,
    status: StatusMessage,
    stats: DbStats,
    metrics: OperationalMetrics,
    broker: BrokerStats,
    selected_queue: usize,
    selected_group: usize,
    queue_peek: Vec<PeekedMessage>,
    queue_config: Option<QueueConfig>,
    group_dead_letters: Vec<StoredEvent>,
    last_refresh: Instant,
    active_query: Option<String>,
}

impl TuiApp {
    fn new(path: PathBuf, db: BicDb, encryption: Option<EncryptionConfig>) -> Result<Self> {
        let path = normalize_path(path)?;
        let stats = db.stats()?;
        let metrics = OperationalMetrics::from_db(&db)?;
        Ok(Self {
            path,
            db,
            encryption,
            tab: Tab::Overview,
            selected_collection: 0,
            input: String::new(),
            editing: false,
            result: None,
            result_scroll: 0,
            history: Vec::new(),
            status: StatusMessage {
                text: "Ready. Press ? for help, / for command input, q to quit.".to_string(),
                error: false,
            },
            stats,
            metrics,
            broker: BrokerStats::default(),
            selected_queue: 0,
            selected_group: 0,
            queue_peek: Vec::new(),
            queue_config: None,
            group_dead_letters: Vec::new(),
            last_refresh: Instant::now(),
            active_query: None,
        })
    }

    fn run(&mut self, terminal: &mut TuiTerminal) -> Result<()> {
        loop {
            if self.last_refresh.elapsed() >= Duration::from_secs(2) {
                self.refresh();
            }
            terminal.draw(|frame| self.draw(frame))?;
            if event::poll(Duration::from_millis(200))? {
                if let Event::Key(key) = event::read()? {
                    if self.handle_key(key)? {
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    fn refresh(&mut self) {
        match (self.db.stats(), OperationalMetrics::from_db(&self.db)) {
            (Ok(stats), Ok(metrics)) => {
                self.stats = stats;
                self.metrics = metrics;
                self.last_refresh = Instant::now();
            }
            (Err(error), _) | (_, Err(error)) => {
                self.set_error(format!("refresh failed: {error}"));
                self.last_refresh = Instant::now();
            }
        }
        if self.selected_collection >= self.stats.collections.len() {
            self.selected_collection = self.stats.collections.len().saturating_sub(1);
        }
        self.refresh_broker();
    }

    fn refresh_broker(&mut self) {
        self.broker = self.db.with_broker(|broker| broker.stats());
        if self.selected_queue >= self.broker.queues.len() {
            self.selected_queue = self.broker.queues.len().saturating_sub(1);
        }
        let Some(queue) = self.broker.queues.get(self.selected_queue) else {
            self.queue_peek = Vec::new();
            self.queue_config = None;
            self.group_dead_letters = Vec::new();
            return;
        };
        if self.selected_group >= queue.groups.len() {
            self.selected_group = queue.groups.len().saturating_sub(1);
        }
        let queue_name = queue.queue.clone();
        // Tail of the retained log: the newest MAX_PREVIEW_ROWS messages.
        let from = queue.next_sequence.saturating_sub(MAX_PREVIEW_ROWS as u64);
        let group_name = queue
            .groups
            .get(self.selected_group)
            .map(|group| group.group.clone());
        let (peek, config, dead) = self.db.with_broker(|broker| {
            (
                broker.peek(&queue_name, from, MAX_PREVIEW_ROWS),
                broker.queue_config(&queue_name),
                group_name
                    .map(|group| broker.dead_letters(&queue_name, &group))
                    .unwrap_or_default(),
            )
        });
        self.queue_peek = peek;
        self.queue_config = config;
        self.group_dead_letters = dead;
    }

    fn draw(&self, frame: &mut Frame<'_>) {
        let area = frame.area();
        frame.render_widget(Block::default().style(Style::default().bg(PANEL_BG)), area);
        let chunks = Layout::vertical([
            Constraint::Length(3),
            Constraint::Min(8),
            Constraint::Length(3),
        ])
        .split(area);

        self.draw_header(frame, chunks[0]);
        match self.tab {
            Tab::Overview => self.draw_overview(frame, chunks[1]),
            Tab::Collections => self.draw_collections(frame, chunks[1]),
            Tab::Data => self.draw_data(frame, chunks[1]),
            Tab::Broker => self.draw_broker(frame, chunks[1]),
            Tab::Sql => self.draw_sql(frame, chunks[1]),
            Tab::Activity => self.draw_activity(frame, chunks[1]),
            Tab::Operations => self.draw_operations(frame, chunks[1]),
            Tab::Help => self.draw_help(frame, chunks[1]),
        }
        self.draw_footer(frame, chunks[2]);
    }

    fn draw_header(&self, frame: &mut Frame<'_>, area: Rect) {
        let titles = Tab::all()
            .iter()
            .map(|tab| Line::from(tab.title()))
            .collect::<Vec<_>>();
        let selected = Tab::all()
            .iter()
            .position(|tab| *tab == self.tab)
            .unwrap_or_default();
        let tabs = Tabs::new(titles)
            .select(selected)
            .block(panel(Line::from(vec![
                Span::styled(
                    " BicDB ",
                    Style::default()
                        .fg(Color::Black)
                        .bg(CYAN)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled(
                    " COMMAND CENTER ",
                    Style::default()
                        .fg(Color::White)
                        .bg(BLUE)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::raw(" "),
                Span::styled("LOCAL ADMIN", Style::default().fg(AMBER)),
            ])))
            .style(dim_style())
            .highlight_style(
                Style::default()
                    .fg(Color::Black)
                    .bg(CYAN)
                    .add_modifier(Modifier::BOLD),
            )
            .divider(" ");
        frame.render_widget(tabs, area);
    }

    fn draw_footer(&self, frame: &mut Frame<'_>, area: Rect) {
        let style = if self.status.error {
            danger_style()
        } else {
            success_style()
        };
        let input_label = if self.editing { "command" } else { "status" };
        let text = if self.editing {
            format!("> {}", self.input)
        } else {
            self.status.text.clone()
        };
        let footer = Paragraph::new(text)
            .block(panel(Line::from(vec![
                Span::styled(
                    format!(" {} ", input_label.to_uppercase()),
                    Style::default().fg(Color::Black).bg(CYAN),
                ),
                Span::raw(" "),
                Span::styled("Tab panels | / command | F5 refresh | q quit", dim_style()),
            ])))
            .style(style);
        frame.render_widget(footer, area);
    }

    fn draw_overview(&self, frame: &mut Frame<'_>, area: Rect) {
        let columns = Layout::horizontal([
            Constraint::Percentage(34),
            Constraint::Percentage(33),
            Constraint::Percentage(33),
        ])
        .split(area);
        self.draw_summary(frame, columns[0]);
        self.draw_storage_chart(frame, columns[1]);
        self.draw_latency_chart(frame, columns[2]);
    }

    fn draw_summary(&self, frame: &mut Frame<'_>, area: Rect) {
        let overhead = self
            .stats
            .collections
            .iter()
            .map(|collection| collection.storage_overhead_bytes)
            .sum::<u64>();
        let logical = self
            .stats
            .collections
            .iter()
            .map(|collection| collection.logical_record_bytes)
            .sum::<u64>();
        let ratio = if self.stats.size_bytes == 0 {
            0.0
        } else {
            overhead as f64 / self.stats.size_bytes as f64
        };
        let selected = self.selected_collection().map(|collection| {
            format!(
                "{} ({} rows, {})",
                collection.name,
                collection.record_count,
                format_bytes(collection.segment_bytes)
            )
        });
        let lines = vec![
            Line::from(vec![
                Span::styled("Path: ", label_style()),
                Span::raw(self.path.display().to_string()),
            ]),
            Line::from(vec![
                Span::styled("Size: ", label_style()),
                Span::raw(format_bytes(self.stats.size_bytes)),
            ]),
            Line::from(vec![
                Span::styled("Collections: ", label_style()),
                Span::raw(self.stats.collection_count.to_string()),
            ]),
            Line::from(vec![
                Span::styled("Records: ", label_style()),
                Span::raw(self.stats.record_count.to_string()),
            ]),
            Line::from(vec![
                Span::styled("Logical bytes: ", label_style()),
                Span::raw(format_bytes(logical)),
            ]),
            Line::from(vec![
                Span::styled("Overhead bytes: ", label_style()),
                Span::raw(format_bytes(overhead)),
            ]),
            Line::from(vec![
                Span::styled("Pending sync ops: ", label_style()),
                Span::raw(self.stats.pending_sync_ops.to_string()),
            ]),
            {
                let (retained, in_flight, pending, dead) = self.broker.queues.iter().fold(
                    (0usize, 0usize, 0usize, 0usize),
                    |(retained, in_flight, pending, dead), queue| {
                        let (fl, pe, dl) = queue.groups.iter().fold(
                            (0usize, 0usize, 0usize),
                            |(fl, pe, dl), group| {
                                (
                                    fl + group.in_flight,
                                    pe + group.pending_redelivery,
                                    dl + group.dead_letters,
                                )
                            },
                        );
                        (
                            retained + queue.retained_messages,
                            in_flight + fl,
                            pending + pe,
                            dead + dl,
                        )
                    },
                );
                Line::from(vec![
                    Span::styled("Broker: ", label_style()),
                    Span::raw(format!(
                        "{} queues, {retained} msgs, {in_flight} in-flight, {pending} pending, ",
                        self.broker.queues.len()
                    )),
                    if dead > 0 {
                        Span::styled(format!("{dead} dead"), danger_style())
                    } else {
                        Span::raw("0 dead")
                    },
                ])
            },
            Line::from(vec![
                Span::styled("Selected: ", label_style()),
                Span::raw(selected.unwrap_or_else(|| "(none)".to_string())),
            ]),
            Line::from(""),
            Line::from("F5 refresh | / command | ? help | q quit"),
        ];
        let gauge = Gauge::default()
            .block(panel(" STORAGE OVERHEAD ").border_style(Style::default().fg(AMBER)))
            .gauge_style(Style::default().fg(AMBER))
            .ratio(ratio.clamp(0.0, 1.0))
            .label(format!("{:.1}%", ratio * 100.0));

        let chunks = Layout::vertical([Constraint::Min(8), Constraint::Length(3)]).split(area);
        frame.render_widget(
            Paragraph::new(lines)
                .block(panel(" DATABASE "))
                .wrap(Wrap { trim: true }),
            chunks[0],
        );
        frame.render_widget(gauge, chunks[1]);
    }

    fn draw_storage_chart(&self, frame: &mut Frame<'_>, area: Rect) {
        let points = chart_points(
            self.stats
                .collections
                .iter()
                .map(|collection| collection.segment_bytes as f64),
        );
        let max_y = points.iter().map(|(_, y)| *y).fold(1.0, f64::max).max(1.0);
        let datasets = vec![Dataset::default()
            .name("segment bytes")
            .marker(symbols::Marker::Braille)
            .graph_type(GraphType::Bar)
            .style(Style::default().fg(CYAN))
            .data(&points)];
        let chart = Chart::new(datasets)
            .block(panel(" COLLECTION STORAGE ").border_style(Style::default().fg(CYAN)))
            .x_axis(Axis::default().bounds([0.0, points.len().saturating_sub(1) as f64]))
            .y_axis(Axis::default().bounds([0.0, max_y]));
        frame.render_widget(chart, area);
    }

    fn draw_latency_chart(&self, frame: &mut Frame<'_>, area: Rect) {
        let points = chart_points(self.history.iter().map(|entry| entry.elapsed_ms as f64));
        let max_y = points.iter().map(|(_, y)| *y).fold(1.0, f64::max).max(1.0);
        let datasets = vec![Dataset::default()
            .name("query ms")
            .marker(symbols::Marker::Braille)
            .graph_type(GraphType::Line)
            .style(Style::default().fg(GREEN))
            .data(&points)];
        let chart = Chart::new(datasets)
            .block(panel(" QUERY LATENCY ").border_style(Style::default().fg(GREEN)))
            .x_axis(Axis::default().bounds([0.0, points.len().saturating_sub(1) as f64]))
            .y_axis(Axis::default().bounds([0.0, max_y]));
        frame.render_widget(chart, area);
    }

    fn draw_collections(&self, frame: &mut Frame<'_>, area: Rect) {
        let chunks =
            Layout::vertical([Constraint::Percentage(60), Constraint::Percentage(40)]).split(area);
        let rows = self
            .stats
            .collections
            .iter()
            .enumerate()
            .map(|(idx, collection)| {
                let style = if idx == self.selected_collection {
                    selected_style()
                } else {
                    Style::default()
                };
                Row::new(vec![
                    Cell::from(collection.name.clone()),
                    Cell::from(format!("{:?}", collection.mode)),
                    Cell::from(collection.record_count.to_string()),
                    Cell::from(
                        collection
                            .vector_dim
                            .map(|value| value.to_string())
                            .unwrap_or_default(),
                    ),
                    Cell::from(format_bytes(collection.segment_bytes)),
                    Cell::from(format_bytes(collection.storage_overhead_bytes)),
                ])
                .style(style)
            });
        let table = Table::new(
            rows,
            [
                Constraint::Percentage(25),
                Constraint::Length(12),
                Constraint::Length(10),
                Constraint::Length(8),
                Constraint::Length(12),
                Constraint::Length(12),
            ],
        )
        .header(
            Row::new([
                "collection",
                "mode",
                "records",
                "vector",
                "segment",
                "overhead",
            ])
            .style(header_style()),
        )
        .block(panel(" COLLECTIONS "));
        frame.render_widget(table, chunks[0]);
        self.draw_collection_detail(frame, chunks[1]);
    }

    fn draw_collection_detail(&self, frame: &mut Frame<'_>, area: Rect) {
        let Some(collection) = self.selected_collection() else {
            frame.render_widget(
                Paragraph::new("No collections yet").block(panel(" DETAIL ")),
                area,
            );
            return;
        };
        let records = self
            .db
            .scan_collection(&collection.name)
            .unwrap_or_default()
            .into_iter()
            .take(MAX_PREVIEW_ROWS)
            .collect::<Vec<_>>();
        let field_lines = metadata_fields(&records)
            .into_iter()
            .map(Line::from)
            .collect::<Vec<_>>();
        let chunks = Layout::horizontal([Constraint::Percentage(45), Constraint::Percentage(55)])
            .split(area);
        let detail = vec![
            Line::from(format!("name: {}", collection.name)),
            Line::from(format!("records: {}", collection.record_count)),
            Line::from(format!(
                "segment: {}",
                format_bytes(collection.segment_bytes)
            )),
            Line::from(format!(
                "logical: {}",
                format_bytes(collection.logical_record_bytes)
            )),
            Line::from(format!(
                "overhead: {}",
                format_bytes(collection.storage_overhead_bytes)
            )),
            Line::from(format!("last offset: {:?}", collection.last_segment_offset)),
        ];
        frame.render_widget(Paragraph::new(detail).block(panel(" DETAIL ")), chunks[0]);
        frame.render_widget(
            Paragraph::new(field_lines)
                .block(panel(" FIELDS "))
                .wrap(Wrap { trim: true }),
            chunks[1],
        );
    }

    fn draw_data(&self, frame: &mut Frame<'_>, area: Rect) {
        let Some(collection) = self.selected_collection() else {
            frame.render_widget(
                Paragraph::new("No collection selected").block(panel(" DATA PREVIEW ")),
                area,
            );
            return;
        };
        let records = self
            .db
            .scan_collection(&collection.name)
            .unwrap_or_default()
            .into_iter()
            .take(MAX_PREVIEW_ROWS)
            .collect::<Vec<_>>();
        let rows = records.into_iter().map(|record| {
            Row::new(vec![
                Cell::from(record.id),
                Cell::from(
                    record
                        .timestamp
                        .map(|ts| ts.to_string())
                        .unwrap_or_default(),
                ),
                Cell::from(
                    record
                        .vector
                        .as_ref()
                        .map(|v| v.len().to_string())
                        .unwrap_or_default(),
                ),
                Cell::from(
                    record
                        .payload
                        .as_ref()
                        .map(|p| format_bytes(p.len() as u64))
                        .unwrap_or_default(),
                ),
                Cell::from(truncate_cell(&record.metadata.to_string(), 90)),
            ])
        });
        let table = Table::new(
            rows,
            [
                Constraint::Percentage(22),
                Constraint::Length(14),
                Constraint::Length(8),
                Constraint::Length(10),
                Constraint::Percentage(50),
            ],
        )
        .header(
            Row::new(["id", "timestamp", "vector", "payload", "metadata"]).style(header_style()),
        )
        .block(panel(format!(
            " DATA PREVIEW: {} (FIRST {MAX_PREVIEW_ROWS}) ",
            collection.name
        )));
        frame.render_widget(table, area);
    }

    fn draw_broker(&self, frame: &mut Frame<'_>, area: Rect) {
        if self.broker.queues.is_empty() {
            frame.render_widget(
                Paragraph::new(vec![
                    Line::from("No broker queues yet."),
                    Line::from(""),
                    Line::from("Publish from the command line:"),
                    Line::from("  :publish commerce.orders {\"order\": 1}"),
                    Line::from(
                        "  :sql SELECT broker_publish('commerce.orders', '{\"order\": 1}', NULL)",
                    ),
                ])
                .block(panel(" STREAM BROKER ")),
                area,
            );
            return;
        }
        let columns = Layout::horizontal([Constraint::Percentage(42), Constraint::Percentage(58)])
            .split(area);
        let left = Layout::vertical([Constraint::Min(8), Constraint::Length(9)]).split(columns[0]);
        self.draw_broker_queues(frame, left[0]);
        self.draw_broker_config(frame, left[1]);
        let right = Layout::vertical([
            Constraint::Percentage(30),
            Constraint::Percentage(40),
            Constraint::Percentage(30),
        ])
        .split(columns[1]);
        self.draw_broker_groups(frame, right[0]);
        self.draw_broker_messages(frame, right[1]);
        self.draw_broker_dead_letters(frame, right[2]);
    }

    fn selected_queue_stats(&self) -> Option<&QueueStats> {
        self.broker.queues.get(self.selected_queue)
    }

    fn draw_broker_queues(&self, frame: &mut Frame<'_>, area: Rect) {
        let rows = self.broker.queues.iter().enumerate().map(|(idx, queue)| {
            let (in_flight, pending, dead): (usize, usize, usize) =
                queue
                    .groups
                    .iter()
                    .fold((0, 0, 0), |(in_flight, pending, dead), group| {
                        (
                            in_flight + group.in_flight,
                            pending + group.pending_redelivery,
                            dead + group.dead_letters,
                        )
                    });
            let style = if idx == self.selected_queue {
                selected_style()
            } else {
                Style::default()
            };
            Row::new(vec![
                Cell::from(queue.queue.clone()),
                Cell::from(queue.retained_messages.to_string()),
                Cell::from(queue.next_sequence.to_string()),
                Cell::from(queue.groups.len().to_string()),
                Cell::from(in_flight.to_string()),
                Cell::from(pending.to_string()),
                Cell::from(if dead > 0 {
                    Span::styled(dead.to_string(), danger_style())
                } else {
                    Span::raw("0")
                }),
                Cell::from(if queue.configured { "yes" } else { "" }),
            ])
            .style(style)
        });
        let table = Table::new(
            rows,
            [
                Constraint::Min(14),
                Constraint::Length(8),
                Constraint::Length(9),
                Constraint::Length(7),
                Constraint::Length(9),
                Constraint::Length(8),
                Constraint::Length(6),
                Constraint::Length(5),
            ],
        )
        .header(
            Row::new([
                "queue",
                "retained",
                "next seq",
                "groups",
                "in-flight",
                "pending",
                "dlq",
                "cfg",
            ])
            .style(header_style()),
        )
        .block(panel(" QUEUES (Up/Down select) ").border_style(Style::default().fg(CYAN)));
        frame.render_widget(table, area);
    }

    fn draw_broker_config(&self, frame: &mut Frame<'_>, area: Rect) {
        let name = self
            .selected_queue_stats()
            .map(|queue| queue.queue.clone())
            .unwrap_or_default();
        let lines = match &self.queue_config {
            Some(config) => {
                let roles = |set: &Option<std::collections::BTreeSet<String>>| match set {
                    Some(roles) => roles.iter().cloned().collect::<Vec<_>>().join(", "),
                    None => "(unrestricted)".to_string(),
                };
                vec![
                    Line::from(format!(
                        "default max attempts: {}",
                        config
                            .default_max_attempts
                            .map(|value| value.to_string())
                            .unwrap_or_else(|| "(library default)".to_string())
                    )),
                    Line::from(format!(
                        "retention max messages: {}",
                        config
                            .retention_max_messages
                            .map(|value| value.to_string())
                            .unwrap_or_else(|| "(keep all)".to_string())
                    )),
                    Line::from(format!(
                        "retention max age: {}",
                        config
                            .retention_max_age_ms
                            .map(|value| format!("{value} ms"))
                            .unwrap_or_else(|| "(keep all)".to_string())
                    )),
                    Line::from(format!("publish roles: {}", roles(&config.publish_roles))),
                    Line::from(format!("consume roles: {}", roles(&config.consume_roles))),
                    Line::from(format!("admin roles: {}", roles(&config.admin_roles))),
                ]
            }
            None => vec![
                Line::from("(no durable configuration)"),
                Line::from("retention: keep all | auto-trim: off | ACLs: unrestricted"),
                Line::from(":queue-config <queue> <json> to configure"),
            ],
        };
        frame.render_widget(
            Paragraph::new(lines)
                .block(panel(format!(" CONFIG: {name} ")).border_style(Style::default().fg(AMBER)))
                .wrap(Wrap { trim: true }),
            area,
        );
    }

    fn draw_broker_groups(&self, frame: &mut Frame<'_>, area: Rect) {
        let groups: &[GroupStats] = self
            .selected_queue_stats()
            .map(|queue| queue.groups.as_slice())
            .unwrap_or_default();
        if groups.is_empty() {
            frame.render_widget(
                Paragraph::new("No consumer groups yet. :drain <queue> <group> creates one.")
                    .block(panel(" CONSUMER GROUPS ")),
                area,
            );
            return;
        }
        let rows = groups.iter().enumerate().map(|(idx, group)| {
            let style = if idx == self.selected_group {
                selected_style()
            } else {
                Style::default()
            };
            Row::new(vec![
                Cell::from(group.group.clone()),
                Cell::from(
                    group
                        .last_delivered_sequence
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "-".to_string()),
                ),
                Cell::from(if group.lag > 0 {
                    Span::styled(group.lag.to_string(), Style::default().fg(AMBER))
                } else {
                    Span::raw("0")
                }),
                Cell::from(group.in_flight.to_string()),
                Cell::from(group.pending_redelivery.to_string()),
                Cell::from(group.settled_floor.to_string()),
                Cell::from(if group.dead_letters > 0 {
                    Span::styled(group.dead_letters.to_string(), danger_style())
                } else {
                    Span::raw("0")
                }),
            ])
            .style(style)
        });
        let table = Table::new(
            rows,
            [
                Constraint::Min(12),
                Constraint::Length(10),
                Constraint::Length(7),
                Constraint::Length(9),
                Constraint::Length(8),
                Constraint::Length(9),
                Constraint::Length(6),
            ],
        )
        .header(
            Row::new([
                "group",
                "last deliv",
                "lag",
                "in-flight",
                "pending",
                "settled",
                "dlq",
            ])
            .style(header_style()),
        )
        .block(
            panel(" CONSUMER GROUPS (Left/Right select) ").border_style(Style::default().fg(GREEN)),
        );
        frame.render_widget(table, area);
    }

    fn draw_broker_messages(&self, frame: &mut Frame<'_>, area: Rect) {
        let now = now_millis();
        let rows = self.queue_peek.iter().rev().map(|message| {
            let delayed = message.available_at > now;
            Row::new(vec![
                Cell::from(message.sequence.to_string()),
                Cell::from(short_id(&message.message_id.to_string())),
                Cell::from(format_age(now.saturating_sub(message.created_at))),
                Cell::from(if delayed {
                    Span::styled(
                        format!("+{}", format_age(message.available_at.saturating_sub(now))),
                        Style::default().fg(AMBER),
                    )
                } else {
                    Span::raw("now")
                }),
                Cell::from(message.max_attempts.to_string()),
                Cell::from(truncate_cell(&message.payload.to_string(), 70)),
            ])
        });
        let queue = self
            .selected_queue_stats()
            .map(|queue| queue.queue.clone())
            .unwrap_or_default();
        let table = Table::new(
            rows,
            [
                Constraint::Length(8),
                Constraint::Length(10),
                Constraint::Length(8),
                Constraint::Length(8),
                Constraint::Length(7),
                Constraint::Percentage(55),
            ],
        )
        .header(Row::new(["seq", "id", "age", "avail", "maxatt", "payload"]).style(header_style()))
        .block(panel(format!(
            " MESSAGES: {queue} (NEWEST {}) ",
            self.queue_peek.len()
        )));
        frame.render_widget(table, area);
    }

    fn draw_broker_dead_letters(&self, frame: &mut Frame<'_>, area: Rect) {
        let group = self
            .selected_queue_stats()
            .and_then(|queue| queue.groups.get(self.selected_group))
            .map(|group| group.group.clone())
            .unwrap_or_default();
        if self.group_dead_letters.is_empty() {
            frame.render_widget(
                Paragraph::new("No dead letters for this group.")
                    .block(panel(format!(" DEAD LETTERS: {group} "))),
                area,
            );
            return;
        }
        let now = now_millis();
        let rows = self
            .group_dead_letters
            .iter()
            .rev()
            .take(MAX_PREVIEW_ROWS)
            .map(|stored| {
                let meta = stored.event.metadata.get("broker");
                let get = |key: &str| {
                    meta.and_then(|meta| meta.get(key))
                        .map(|value| match value.as_str() {
                            Some(text) => text.to_string(),
                            None => value.to_string(),
                        })
                        .unwrap_or_default()
                };
                let at = meta
                    .and_then(|meta| meta.get("dead_lettered_at_ms"))
                    .and_then(|value| value.as_i64())
                    .unwrap_or_default();
                Row::new(vec![
                    Cell::from(format_age(now.saturating_sub(at))),
                    Cell::from(get("attempts")),
                    Cell::from(Span::styled(
                        truncate_cell(&get("reason"), 40),
                        danger_style(),
                    )),
                    Cell::from(truncate_cell(&get("last_error"), 25)),
                    Cell::from(truncate_cell(&stored.event.payload.to_string(), 50)),
                ])
            });
        let table = Table::new(
            rows,
            [
                Constraint::Length(8),
                Constraint::Length(8),
                Constraint::Percentage(30),
                Constraint::Percentage(20),
                Constraint::Percentage(38),
            ],
        )
        .header(
            Row::new(["age", "attempts", "reason", "last error", "payload"]).style(header_style()),
        )
        .block(
            panel(format!(
                " DEAD LETTERS: {group} ({}) | :redrive | :purge-dlq ",
                self.group_dead_letters.len()
            ))
            .border_style(Style::default().fg(RED)),
        );
        frame.render_widget(table, area);
    }

    fn draw_sql(&self, frame: &mut Frame<'_>, area: Rect) {
        let chunks = Layout::vertical([Constraint::Length(5), Constraint::Min(8)]).split(area);
        let help = vec![
            Line::from(
                "Type SQL directly, or commands like :explain SELECT * FROM patients LIMIT 5",
            ),
            Line::from("Useful: :refresh | :backup /tmp/db.bicbackup passphrase | :check | :kill"),
        ];
        frame.render_widget(
            Paragraph::new(help)
                .block(panel(" SQL CONSOLE "))
                .wrap(Wrap { trim: true }),
            chunks[0],
        );
        self.draw_result_table(frame, chunks[1]);
    }

    fn draw_result_table(&self, frame: &mut Frame<'_>, area: Rect) {
        let Some(result) = &self.result else {
            frame.render_widget(
                Paragraph::new("No result yet. Press / and run a SELECT or :explain query.")
                    .block(panel(" RESULT ")),
                area,
            );
            return;
        };
        let widths = result
            .columns
            .iter()
            .map(|_| Constraint::Min(12))
            .collect::<Vec<_>>();
        let rows = result
            .rows
            .iter()
            .skip(self.result_scroll)
            .take(MAX_RESULT_ROWS)
            .map(|row| {
                Row::new(
                    row.iter()
                        .map(|value| Cell::from(truncate_cell(&value.to_cell(), 80)))
                        .collect::<Vec<_>>(),
                )
            });
        let table = Table::new(rows, widths)
            .header(Row::new(result.columns.clone()).style(header_style()))
            .block(panel(format!(" RESULT: {} ROWS ", result.rows.len())));
        frame.render_widget(table, area);
    }

    fn draw_activity(&self, frame: &mut Frame<'_>, area: Rect) {
        let chunks = Layout::vertical([
            Constraint::Percentage(35),
            Constraint::Percentage(25),
            Constraint::Percentage(40),
        ])
        .split(area);
        self.draw_latency_chart(frame, chunks[0]);
        let spark_data = self
            .history
            .iter()
            .rev()
            .take(80)
            .map(|entry| entry.elapsed_ms.max(1))
            .collect::<Vec<_>>();
        let sparkline = Sparkline::default()
            .block(panel(" RECENT QUERY LATENCY "))
            .style(Style::default().fg(VIOLET))
            .data(&spark_data);
        frame.render_widget(sparkline, chunks[1]);
        let rows = self.history.iter().rev().take(20).map(|entry| {
            Row::new(vec![
                Cell::from(entry.index.to_string()),
                Cell::from(entry.timestamp.to_string()),
                Cell::from(entry.elapsed_ms.to_string()),
                Cell::from(entry.rows.to_string()),
                Cell::from(entry.status.clone()),
                Cell::from(truncate_cell(&entry.sql, 100)),
            ])
        });
        let table = Table::new(
            rows,
            [
                Constraint::Length(5),
                Constraint::Length(12),
                Constraint::Length(8),
                Constraint::Length(8),
                Constraint::Length(10),
                Constraint::Percentage(62),
            ],
        )
        .header(Row::new(["#", "time", "ms", "rows", "status", "query"]).style(header_style()))
        .block(panel(" QUERY HISTORY "));
        frame.render_widget(table, chunks[2]);
    }

    fn draw_operations(&self, frame: &mut Frame<'_>, area: Rect) {
        let chunks = Layout::horizontal([Constraint::Percentage(50), Constraint::Percentage(50)])
            .split(area);
        let commands = vec![
            Line::from(vec![Span::styled(
                "Database administration",
                header_style(),
            )]),
            Line::from("CREATE DATABASE acme"),
            Line::from(":createdb <name-or-path>"),
            Line::from(":opendb <path>"),
            Line::from(":databases"),
            Line::from(""),
            Line::from(vec![Span::styled(
                "Collection administration",
                header_style(),
            )]),
            Line::from(":create-collection <name> [standard|timeseries]"),
            Line::from(":drop-collection <name> force"),
            Line::from(":use <collection>"),
            Line::from(""),
            Line::from(vec![Span::styled("Stream broker", header_style())]),
            Line::from(":publish <queue> <json-payload>"),
            Line::from(":drain <queue> <group> [max]  (consume + ack)"),
            Line::from(":queue-config <queue> <json>"),
            Line::from(":trim <queue>"),
            Line::from(":redrive <queue> <group> [max]"),
            Line::from(":purge-dlq <queue> <group>"),
            Line::from(":sql SELECT broker_stats() / broker_peek('q', 0, 50)"),
            Line::from(""),
            Line::from(vec![Span::styled("Maintenance", header_style())]),
            Line::from(":refresh"),
            Line::from(":check"),
            Line::from(":backup <file.bicbackup> <passphrase>"),
            Line::from(":verify-backup <file.bicbackup> <passphrase>"),
            Line::from(":restore <file.bicbackup> <target-dir> <passphrase> [force]"),
            Line::from(":explain <select-query>"),
            Line::from(":sql <query>"),
            Line::from(":kill"),
        ];
        frame.render_widget(
            Paragraph::new(commands)
                .block(panel(" ADMIN OPERATIONS "))
                .wrap(Wrap { trim: true }),
            chunks[0],
        );
        let metric_lines = metrics_lines(&self.metrics);
        frame.render_widget(
            Paragraph::new(metric_lines)
                .block(panel(" OPERATIONAL METRICS "))
                .wrap(Wrap { trim: true }),
            chunks[1],
        );
    }

    fn draw_help(&self, frame: &mut Frame<'_>, area: Rect) {
        let help = vec![
            Line::from(vec![Span::styled("Navigation", header_style())]),
            Line::from("Tab / Shift-Tab: switch panels"),
            Line::from("1-8: jump to panel"),
            Line::from("Up/Down: select collections/queues or scroll result rows"),
            Line::from("Left/Right: select consumer group on the Broker panel"),
            Line::from("F5: refresh metrics"),
            Line::from("/ or :: command input"),
            Line::from("Esc: leave input"),
            Line::from("q: quit"),
            Line::from(""),
            Line::from(vec![Span::styled("Commands", header_style())]),
            Line::from("SQL can be typed directly from input mode."),
            Line::from("CREATE DATABASE acme creates and opens a sibling BicDB directory."),
            Line::from(":createdb acme | :opendb ./otherdb | :databases"),
            Line::from(":create-collection patients [standard|timeseries]"),
            Line::from(":drop-collection patients force"),
            Line::from(":explain SELECT * FROM collection LIMIT 10"),
            Line::from(":backup ./backup.bicbackup passphrase"),
            Line::from(":restore ./backup.bicbackup ./restored passphrase force"),
            Line::from(":check"),
            Line::from(
                ":kill cancels a TUI-tracked active query when cancellation is wired through the running path.",
            ),
            Line::from(""),
            Line::from(vec![Span::styled("Stream broker", header_style())]),
            Line::from(
                "Broker panel: queues (Up/Down), consumer groups (Left/Right), live message tail, per-group dead letters, durable config/ACLs.",
            ),
            Line::from(":publish orders {\"n\": 1} | :drain orders workers 10 | :trim orders"),
            Line::from(
                ":redrive orders workers | :purge-dlq orders workers | :queue-config orders {\"retention_max_messages\": 100000}",
            ),
            Line::from(
                "Full SQL surface: broker_publish, broker_consume, broker_ack/nack, broker_peek, broker_stats, ...",
            ),
            Line::from(""),
            Line::from(
                "Charts show collection segment sizes, query latency history, storage overhead, and recent latency sparkline.",
            ),
        ];
        frame.render_widget(
            Paragraph::new(help)
                .block(panel(" HELP "))
                .wrap(Wrap { trim: true }),
            area,
        );
    }

    fn handle_key(&mut self, key: KeyEvent) -> Result<bool> {
        if self.editing {
            return self.handle_input_key(key);
        }

        match key.code {
            KeyCode::Char('q') => return Ok(true),
            KeyCode::Char('?') => self.tab = Tab::Help,
            KeyCode::Char('/') => {
                self.editing = true;
                self.input.clear();
            }
            KeyCode::Char(':') => {
                self.editing = true;
                self.input.clear();
                self.input.push(':');
            }
            KeyCode::Tab => self.next_tab(),
            KeyCode::BackTab => self.previous_tab(),
            KeyCode::F(1) => self.tab = Tab::Help,
            KeyCode::F(2) => self.tab = Tab::Sql,
            KeyCode::F(3) => self.tab = Tab::Data,
            KeyCode::F(4) => self.tab = Tab::Operations,
            KeyCode::F(5) => {
                self.refresh();
                self.set_status("refreshed".to_string());
            }
            KeyCode::Down => self.move_down(),
            KeyCode::Up => self.move_up(),
            KeyCode::Left => self.move_left(),
            KeyCode::Right => self.move_right(),
            KeyCode::Char(value @ '1'..='8') => {
                let idx = value as usize - '1' as usize;
                if let Some(tab) = Tab::all().get(idx) {
                    self.tab = *tab;
                }
            }
            _ => {}
        }
        Ok(false)
    }

    fn handle_input_key(&mut self, key: KeyEvent) -> Result<bool> {
        match key.code {
            KeyCode::Esc => {
                self.editing = false;
                self.input.clear();
            }
            KeyCode::Enter => {
                let command = self.input.trim().to_string();
                self.editing = false;
                self.input.clear();
                if !command.is_empty() {
                    self.execute_command(&command);
                }
            }
            KeyCode::Backspace => {
                self.input.pop();
            }
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => return Ok(true),
            KeyCode::Char(ch) => self.input.push(ch),
            _ => {}
        }
        Ok(false)
    }

    fn execute_command(&mut self, command: &str) {
        let outcome = if let Some(rest) = command.strip_prefix(':') {
            self.execute_colon_command(rest.trim())
        } else {
            match parse_admin_statement(command) {
                Some(Ok(statement)) => self.execute_admin_statement(statement),
                Some(Err(error)) => Err(error),
                None => self.execute_sql(command),
            }
        };
        match outcome {
            Ok(message) => self.set_status(message),
            Err(error) => self.set_error(error.to_string()),
        }
        self.refresh();
    }

    fn execute_colon_command(&mut self, command: &str) -> Result<String> {
        let mut parts = command.split_whitespace();
        let Some(name) = parts.next() else {
            return Ok("empty command".to_string());
        };
        match name {
            "help" | "?" => {
                self.tab = Tab::Help;
                Ok("help opened".to_string())
            }
            "refresh" => {
                self.refresh();
                Ok("refreshed".to_string())
            }
            "createdb" => {
                let name = parts
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("usage: :createdb <name-or-path>"))?;
                self.create_database(name, false)
            }
            "opendb" => {
                let path = parts
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("usage: :opendb <path>"))?;
                let target = self.resolve_database_path(path)?;
                self.open_database(target)?;
                Ok(format!("opened database {}", self.path.display()))
            }
            "databases" => self.list_databases(),
            "create-collection" => {
                let name = parts.next().ok_or_else(|| {
                    anyhow::anyhow!("usage: :create-collection <name> [standard|timeseries]")
                })?;
                match parts.next().unwrap_or("standard") {
                    "standard" => self.db.create_collection(name)?,
                    "timeseries" | "time-series" => self.db.create_timeseries_collection(name)?,
                    other => bail!("unknown collection mode `{other}`"),
                }
                self.refresh();
                self.result = Some(SqlResult::new(
                    vec!["operation".to_string(), "collection".to_string()],
                    vec![vec![
                        SqlValue::String("create_collection".to_string()),
                        SqlValue::String(name.to_string()),
                    ]],
                ));
                Ok(format!("created collection {name}"))
            }
            "drop-collection" => {
                let name = parts
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("usage: :drop-collection <name> force"))?;
                let force =
                    parts.any(|part| part.eq_ignore_ascii_case("force") || part == "--force");
                if !force {
                    bail!("refusing to drop collection `{name}` without `force`");
                }
                let dropped = self.db.drop_collection(name)?;
                self.refresh();
                self.result = Some(SqlResult::new(
                    vec![
                        "operation".to_string(),
                        "collection".to_string(),
                        "dropped".to_string(),
                    ],
                    vec![vec![
                        SqlValue::String("drop_collection".to_string()),
                        SqlValue::String(name.to_string()),
                        SqlValue::Bool(dropped),
                    ]],
                ));
                Ok(if dropped {
                    format!("dropped collection {name}")
                } else {
                    format!("collection {name} did not exist")
                })
            }
            "queues" | "broker" => {
                self.tab = Tab::Broker;
                self.refresh_broker();
                Ok("broker panel opened".to_string())
            }
            "publish" => {
                let queue = parts
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("usage: :publish <queue> <json-payload>"))?
                    .to_string();
                let payload_text = command[name.len()..].trim();
                let payload_text = payload_text[queue.len()..].trim();
                if payload_text.is_empty() {
                    bail!("usage: :publish <queue> <json-payload>");
                }
                let payload: serde_json::Value = serde_json::from_str(payload_text)
                    .map_err(|error| anyhow::anyhow!("payload is not valid JSON: {error}"))?;
                let receipt = self.db.with_broker(|broker| {
                    broker.publish_with(&queue, payload, PublishOptions::default())
                })?;
                self.tab = Tab::Broker;
                self.refresh_broker();
                Ok(format!(
                    "published to {queue}: seq {} id {}",
                    receipt.sequence, receipt.message_id
                ))
            }
            "drain" => {
                let queue = parts
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("usage: :drain <queue> <group> [max]"))?
                    .to_string();
                let group = parts
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("usage: :drain <queue> <group> [max]"))?
                    .to_string();
                let max = parts
                    .next()
                    .map(|raw| raw.parse::<usize>())
                    .transpose()
                    .map_err(|_| anyhow::anyhow!("max must be a number"))?
                    .unwrap_or(MAX_PREVIEW_ROWS);
                let mut drained: Vec<Vec<SqlValue>> = Vec::new();
                let report = self.db.with_broker(|broker| {
                    broker.drain(
                        &queue,
                        &group,
                        "tui",
                        DrainOptions {
                            max_messages: max,
                            ..DrainOptions::default()
                        },
                        |message| {
                            drained.push(vec![
                                SqlValue::Int(message.sequence as i64),
                                SqlValue::String(message.message_id.to_string()),
                                SqlValue::Int(message.attempts as i64),
                                SqlValue::String(message.payload.to_string()),
                            ]);
                            Ok(())
                        },
                    )
                })?;
                self.result = Some(SqlResult::new(
                    vec![
                        "sequence".to_string(),
                        "message_id".to_string(),
                        "attempts".to_string(),
                        "payload".to_string(),
                    ],
                    drained,
                ));
                self.tab = Tab::Broker;
                self.refresh_broker();
                Ok(format!(
                    "drained {queue}/{group}: {} delivered, {} acked (result on SQL panel)",
                    report.delivered, report.acked
                ))
            }
            "trim" => {
                let queue = parts
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("usage: :trim <queue>"))?
                    .to_string();
                let report = self.db.with_broker(|broker| broker.trim(&queue))?;
                self.refresh_broker();
                Ok(format!(
                    "trimmed {queue}: {} messages removed through seq {:?}, {} -> {}",
                    report.removed_messages,
                    report.through_sequence,
                    format_bytes(report.bytes_before),
                    format_bytes(report.bytes_after)
                ))
            }
            "redrive" => {
                let queue = parts
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("usage: :redrive <queue> <group> [max]"))?
                    .to_string();
                let group = parts
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("usage: :redrive <queue> <group> [max]"))?
                    .to_string();
                let max = parts
                    .next()
                    .map(|raw| raw.parse::<usize>())
                    .transpose()
                    .map_err(|_| anyhow::anyhow!("max must be a number"))?
                    .unwrap_or(100);
                let receipts = self
                    .db
                    .with_broker(|broker| broker.redrive_dead_letters(&queue, &group, max))?;
                self.refresh_broker();
                Ok(format!(
                    "redrove {} dead letters back onto {queue}",
                    receipts.len()
                ))
            }
            "purge-dlq" => {
                let queue = parts
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("usage: :purge-dlq <queue> <group>"))?
                    .to_string();
                let group = parts
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("usage: :purge-dlq <queue> <group>"))?
                    .to_string();
                let purged = self
                    .db
                    .with_broker(|broker| broker.purge_dead_letters(&queue, &group))?;
                self.refresh_broker();
                Ok(format!("purged {purged} dead letters from {queue}/{group}"))
            }
            "queue-config" => {
                let queue = parts
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("usage: :queue-config <queue> <json>"))?
                    .to_string();
                let config_text = command[name.len()..].trim();
                let config_text = config_text[queue.len()..].trim();
                if config_text.is_empty() {
                    bail!("usage: :queue-config <queue> <json>");
                }
                let value: serde_json::Value = serde_json::from_str(config_text)
                    .map_err(|error| anyhow::anyhow!("config is not valid JSON: {error}"))?;
                let roles = |key: &str| {
                    value.get(key).and_then(|raw| raw.as_array()).map(|list| {
                        list.iter()
                            .filter_map(|role| role.as_str())
                            .map(str::to_string)
                            .collect::<std::collections::BTreeSet<String>>()
                    })
                };
                let config = QueueConfig {
                    default_max_attempts: value
                        .get("default_max_attempts")
                        .and_then(|raw| raw.as_u64())
                        .map(|raw| raw as u32),
                    retention_max_messages: value
                        .get("retention_max_messages")
                        .and_then(|raw| raw.as_u64()),
                    retention_max_age_ms: value
                        .get("retention_max_age_ms")
                        .and_then(|raw| raw.as_i64()),
                    publish_roles: roles("publish_roles"),
                    consume_roles: roles("consume_roles"),
                    admin_roles: roles("admin_roles"),
                };
                self.db
                    .with_broker(|broker| broker.configure_queue(&queue, config))?;
                self.tab = Tab::Broker;
                self.refresh_broker();
                Ok(format!("configured queue {queue}"))
            }
            "check" => {
                let report =
                    BicDb::verify_path(&self.path, DbConfig::default(), self.encryption.clone())?;
                Ok(format!(
                    "integrity checked: {} collections, {} records",
                    report.collections_checked, report.record_frames
                ))
            }
            "sql" => {
                let sql = command[name.len()..].trim();
                if sql.is_empty() {
                    bail!("usage: :sql <query>");
                }
                self.execute_sql(sql)
            }
            "explain" => {
                let sql = command[name.len()..].trim();
                if sql.is_empty() {
                    bail!("usage: :explain <select-query>");
                }
                self.execute_sql(&format!("EXPLAIN {sql}"))
            }
            "backup" => {
                let out = parts
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("usage: :backup <file> <passphrase>"))?;
                let passphrase = parts
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("usage: :backup <file> <passphrase>"))?;
                self.db.flush()?;
                let report = create_backup(
                    &self.path,
                    out,
                    BackupCreateOptions {
                        passphrase: passphrase.to_string(),
                        base_backup: None,
                    },
                )?;
                Ok(format!(
                    "backup created: {} files, {}",
                    report.files_included,
                    format_bytes(report.encrypted_bytes)
                ))
            }
            "verify-backup" => {
                let backup = parts
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("usage: :verify-backup <file> <passphrase>"))?;
                let passphrase = parts
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("usage: :verify-backup <file> <passphrase>"))?;
                let report = verify_backup(backup, passphrase)?;
                Ok(format!(
                    "backup verified: {} files, manifest {}",
                    report.files_included, report.manifest_hash
                ))
            }
            "restore" => {
                let backup = parts.next().ok_or_else(|| {
                    anyhow::anyhow!("usage: :restore <backup> <target-dir> <passphrase> [force]")
                })?;
                let target = parts.next().ok_or_else(|| {
                    anyhow::anyhow!("usage: :restore <backup> <target-dir> <passphrase> [force]")
                })?;
                let passphrase = parts.next().ok_or_else(|| {
                    anyhow::anyhow!("usage: :restore <backup> <target-dir> <passphrase> [force]")
                })?;
                let force =
                    parts.any(|part| part.eq_ignore_ascii_case("force") || part == "--force");
                let report = restore_backup(
                    backup,
                    target,
                    BackupRestoreOptions {
                        passphrase: passphrase.to_string(),
                        force,
                    },
                )?;
                Ok(format!(
                    "restored {} files to {}",
                    report.files_restored,
                    report.target_path.display()
                ))
            }
            "kill" => {
                if let Some(query) = self.active_query.take() {
                    Ok(format!("cancel requested for {query}"))
                } else {
                    Ok("no TUI-launched query is currently active".to_string())
                }
            }
            "use" => {
                let collection = parts
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("usage: :use <collection>"))?;
                let Some(index) = self
                    .stats
                    .collections
                    .iter()
                    .position(|stats| stats.name == collection)
                else {
                    bail!("collection `{collection}` not found");
                };
                self.selected_collection = index;
                self.tab = Tab::Data;
                Ok(format!("selected collection {collection}"))
            }
            other => Err(anyhow::anyhow!("unknown command `{other}`")),
        }
    }

    fn execute_admin_statement(&mut self, statement: AdminStatement) -> Result<String> {
        match statement {
            AdminStatement::CreateDatabase {
                name,
                if_not_exists,
            } => self.create_database(&name, if_not_exists),
        }
    }

    fn create_database(&mut self, name: &str, if_not_exists: bool) -> Result<String> {
        let path = self.resolve_database_path(name)?;
        let existed = path.exists();
        if existed && !if_not_exists {
            bail!(
                "database path already exists: {}. Use CREATE DATABASE IF NOT EXISTS or :opendb.",
                path.display()
            );
        }
        self.open_database(path.clone())?;
        self.result = Some(SqlResult::new(
            vec![
                "operation".to_string(),
                "database".to_string(),
                "path".to_string(),
                "created".to_string(),
            ],
            vec![vec![
                SqlValue::String("create_database".to_string()),
                SqlValue::String(name.to_string()),
                SqlValue::String(path.display().to_string()),
                SqlValue::Bool(!existed),
            ]],
        ));
        Ok(format!(
            "{} database {} at {}",
            if existed {
                "opened existing"
            } else {
                "created"
            },
            name,
            path.display()
        ))
    }

    fn open_database(&mut self, path: PathBuf) -> Result<()> {
        let path = normalize_path(path)?;
        self.db.flush()?;
        let db = match self.encryption.clone() {
            Some(config) => BicDb::open_with_encryption(&path, DbConfig::default(), config)?,
            None => BicDb::open(&path)?,
        };
        self.path = path;
        self.db = db;
        self.selected_collection = 0;
        self.result_scroll = 0;
        self.refresh();
        Ok(())
    }

    fn list_databases(&mut self) -> Result<String> {
        let root = database_root(&self.path);
        let mut rows = Vec::new();
        if root.exists() {
            for entry in fs::read_dir(&root)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_dir() && looks_like_bicdb_database(&path) {
                    rows.push(vec![
                        SqlValue::String(entry.file_name().to_string_lossy().to_string()),
                        SqlValue::String(path.display().to_string()),
                        SqlValue::Bool(path == self.path),
                    ]);
                }
            }
        }
        rows.sort_by(|left, right| left[0].to_cell().cmp(&right[0].to_cell()));
        let count = rows.len();
        self.result = Some(SqlResult::new(
            vec![
                "database".to_string(),
                "path".to_string(),
                "current".to_string(),
            ],
            rows,
        ));
        self.tab = Tab::Sql;
        Ok(format!(
            "listed {count} database paths under {}",
            root.display()
        ))
    }

    fn resolve_database_path(&self, name: &str) -> Result<PathBuf> {
        let clean = name.trim().trim_end_matches(';').trim();
        if clean.is_empty() {
            bail!("database name cannot be empty");
        }
        let raw = PathBuf::from(clean);
        if raw.is_absolute() || clean.contains('/') || clean.starts_with('.') {
            return normalize_path(raw);
        }
        normalize_path(database_root(&self.path).join(clean))
    }

    fn execute_sql(&mut self, sql: &str) -> Result<String> {
        self.tab = Tab::Sql;
        self.active_query = Some(sql.to_string());
        let started = Instant::now();
        let result = SqlEngine::new(&self.db).execute(sql);
        let elapsed_ms = started.elapsed().as_millis() as u64;
        self.active_query = None;
        match result {
            Ok(result) => {
                let rows = result.rows.len();
                self.result = Some(result);
                self.result_scroll = 0;
                self.push_history(sql, elapsed_ms, rows, "ok");
                Ok(format!("query ok: {rows} rows in {elapsed_ms}ms"))
            }
            Err(error) => {
                self.push_history(sql, elapsed_ms, 0, "error");
                Err(error.into())
            }
        }
    }

    fn push_history(&mut self, sql: &str, elapsed_ms: u64, rows: usize, status: &str) {
        let index = self
            .history
            .last()
            .map(|entry| entry.index + 1)
            .unwrap_or(1);
        self.history.push(QueryHistoryEntry {
            index,
            sql: sql.to_string(),
            elapsed_ms,
            rows,
            status: status.to_string(),
            timestamp: unix_timestamp(),
        });
        if self.history.len() > MAX_HISTORY {
            let overflow = self.history.len() - MAX_HISTORY;
            self.history.drain(0..overflow);
        }
    }

    fn next_tab(&mut self) {
        let tabs = Tab::all();
        let current = tabs
            .iter()
            .position(|tab| *tab == self.tab)
            .unwrap_or_default();
        self.tab = tabs[(current + 1) % tabs.len()];
    }

    fn previous_tab(&mut self) {
        let tabs = Tab::all();
        let current = tabs
            .iter()
            .position(|tab| *tab == self.tab)
            .unwrap_or_default();
        self.tab = tabs[(current + tabs.len() - 1) % tabs.len()];
    }

    fn move_down(&mut self) {
        match self.tab {
            Tab::Collections | Tab::Data => {
                if self.selected_collection + 1 < self.stats.collections.len() {
                    self.selected_collection += 1;
                }
            }
            Tab::Broker => {
                if self.selected_queue + 1 < self.broker.queues.len() {
                    self.selected_queue += 1;
                    self.selected_group = 0;
                    self.refresh_broker();
                }
            }
            Tab::Sql => {
                if let Some(result) = &self.result {
                    self.result_scroll =
                        (self.result_scroll + 1).min(result.rows.len().saturating_sub(1));
                }
            }
            _ => {}
        }
    }

    fn move_up(&mut self) {
        match self.tab {
            Tab::Collections | Tab::Data => {
                self.selected_collection = self.selected_collection.saturating_sub(1);
            }
            Tab::Broker => {
                if self.selected_queue > 0 {
                    self.selected_queue -= 1;
                    self.selected_group = 0;
                    self.refresh_broker();
                }
            }
            Tab::Sql => {
                self.result_scroll = self.result_scroll.saturating_sub(1);
            }
            _ => {}
        }
    }

    fn move_left(&mut self) {
        if self.tab == Tab::Broker && self.selected_group > 0 {
            self.selected_group -= 1;
            self.refresh_broker();
        }
    }

    fn move_right(&mut self) {
        if self.tab == Tab::Broker {
            let groups = self
                .selected_queue_stats()
                .map(|queue| queue.groups.len())
                .unwrap_or(0);
            if self.selected_group + 1 < groups {
                self.selected_group += 1;
                self.refresh_broker();
            }
        }
    }

    fn selected_collection(&self) -> Option<&CollectionStats> {
        self.stats.collections.get(self.selected_collection)
    }

    fn set_status(&mut self, text: String) {
        self.status = StatusMessage { text, error: false };
    }

    fn set_error(&mut self, text: String) {
        self.status = StatusMessage { text, error: true };
    }
}

fn parse_admin_statement(sql: &str) -> Option<Result<AdminStatement>> {
    let tokens = match tokenize_admin_sql(sql) {
        Ok(tokens) => tokens,
        Err(error) => return Some(Err(error)),
    };
    if tokens.len() < 2 {
        return None;
    }
    if !tokens[0].eq_ignore_ascii_case("create") || !tokens[1].eq_ignore_ascii_case("database") {
        return None;
    }

    let mut index = 2;
    let mut if_not_exists = false;
    if tokens
        .get(index)
        .is_some_and(|token| token.eq_ignore_ascii_case("if"))
    {
        let has_not_exists = tokens
            .get(index + 1)
            .is_some_and(|token| token.eq_ignore_ascii_case("not"))
            && tokens
                .get(index + 2)
                .is_some_and(|token| token.eq_ignore_ascii_case("exists"));
        if !has_not_exists {
            return Some(Err(anyhow::anyhow!(
                "usage: CREATE DATABASE [IF NOT EXISTS] <name>"
            )));
        }
        if_not_exists = true;
        index += 3;
    }

    let Some(name) = tokens.get(index) else {
        return Some(Err(anyhow::anyhow!(
            "usage: CREATE DATABASE [IF NOT EXISTS] <name>"
        )));
    };
    if tokens.len() > index + 1 {
        return Some(Err(anyhow::anyhow!(
            "unsupported CREATE DATABASE options in TUI admin mode"
        )));
    }

    Some(Ok(AdminStatement::CreateDatabase {
        name: name.clone(),
        if_not_exists,
    }))
}

fn tokenize_admin_sql(sql: &str) -> Result<Vec<String>> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut quote = None;

    for ch in sql.trim().chars() {
        if let Some(quote_char) = quote {
            if ch == quote_char {
                quote = None;
            } else {
                current.push(ch);
            }
            continue;
        }

        match ch {
            '"' | '\'' => quote = Some(ch),
            ';' => {
                push_token(&mut tokens, &mut current);
            }
            ch if ch.is_whitespace() => push_token(&mut tokens, &mut current),
            ch => current.push(ch),
        }
    }
    if let Some(quote_char) = quote {
        bail!("unterminated quoted identifier starting with {quote_char}");
    }
    push_token(&mut tokens, &mut current);
    Ok(tokens)
}

fn push_token(tokens: &mut Vec<String>, current: &mut String) {
    if !current.is_empty() {
        tokens.push(std::mem::take(current));
    }
}

fn normalize_path(path: PathBuf) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path)
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}

fn database_root(path: &Path) -> PathBuf {
    path.parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."))
}

fn looks_like_bicdb_database(path: &Path) -> bool {
    path.join(DEFAULT_SEGMENTS_DIR).is_dir()
        || path.join(DEFAULT_COLLECTION_CATALOG).is_file()
        || path.join(bicdb_core::ENCRYPTION_METADATA_FILE).is_file()
}

fn panel<'a>(title: impl Into<Line<'a>>) -> Block<'a> {
    Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(BLUE))
        .style(Style::default().fg(Color::White).bg(PANEL_BG))
        .title(title)
}

fn selected_style() -> Style {
    Style::default()
        .fg(Color::Black)
        .bg(CYAN)
        .add_modifier(Modifier::BOLD)
}

fn success_style() -> Style {
    Style::default().fg(GREEN).add_modifier(Modifier::BOLD)
}

fn danger_style() -> Style {
    Style::default().fg(RED).add_modifier(Modifier::BOLD)
}

fn dim_style() -> Style {
    Style::default().fg(DIM)
}

fn label_style() -> Style {
    Style::default().fg(CYAN).add_modifier(Modifier::BOLD)
}

fn header_style() -> Style {
    Style::default()
        .fg(Color::Black)
        .bg(AMBER)
        .add_modifier(Modifier::BOLD)
}

fn chart_points(values: impl IntoIterator<Item = f64>) -> Vec<(f64, f64)> {
    let mut points = values
        .into_iter()
        .enumerate()
        .map(|(idx, value)| (idx as f64, value.max(0.0)))
        .collect::<Vec<_>>();
    if points.is_empty() {
        points.push((0.0, 0.0));
    }
    points
}

fn metadata_fields(records: &[bicdb_core::Record]) -> Vec<String> {
    let mut fields = records
        .iter()
        .filter_map(|record| record.metadata.as_object())
        .flat_map(|object| object.keys().cloned())
        .collect::<Vec<_>>();
    fields.sort();
    fields.dedup();
    if fields.is_empty() {
        vec!["(no metadata fields in preview)".to_string()]
    } else {
        fields
    }
}

fn metrics_lines(metrics: &OperationalMetrics) -> Vec<Line<'static>> {
    let sections = [
        ("server", &metrics.server),
        ("storage", &metrics.storage),
        ("planner", &metrics.planner),
        ("transaction", &metrics.transaction),
        ("backup", &metrics.backup),
        ("compaction", &metrics.compaction),
        ("security", &metrics.security),
        ("replication", &metrics.replication),
        ("memory", &metrics.memory),
    ];
    let mut lines = vec![Line::from(format!(
        "generated_at: {}",
        metrics.generated_at
    ))];
    for (section, values) in sections {
        lines.push(Line::from(vec![Span::styled(
            section.to_string(),
            header_style(),
        )]));
        for (name, value) in values {
            lines.push(Line::from(format!("  {name}: {value}")));
        }
    }
    lines
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} {}", UNITS[unit])
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

fn truncate_cell(value: &str, max: usize) -> String {
    let char_count = value.chars().count();
    if char_count <= max {
        return value.to_string();
    }
    if max <= 3 {
        return ".".repeat(max);
    }
    let mut truncated = value.chars().take(max - 3).collect::<String>();
    truncated.push_str("...");
    truncated
}

fn unix_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use bicdb_core::Record;
    use bicdb_sql::SqlValue;
    use serde_json::json;

    #[test]
    fn tui_app_executes_sql_and_records_latency_history() {
        let temp = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(temp.path()).unwrap();
        db.create_collection("patients").unwrap();
        db.insert(
            "patients",
            Record::new("p1").with_metadata(json!({"name": "Ada", "clinic": "rural-7"})),
        )
        .unwrap();

        let mut app = TuiApp::new(temp.path().to_path_buf(), db, None).unwrap();
        app.execute_command(":sql SELECT COUNT(*) FROM patients");

        assert_eq!(app.history.len(), 1);
        assert_eq!(app.history[0].status, "ok");
        let result = app.result.as_ref().unwrap();
        assert_eq!(result.rows[0][0], SqlValue::Int(1));
    }

    #[test]
    fn metadata_fields_are_sorted_and_unique() {
        let records = vec![
            Record::new("a").with_metadata(json!({"z": 1, "a": 2})),
            Record::new("b").with_metadata(json!({"a": 3, "m": 4})),
        ];
        assert_eq!(metadata_fields(&records), vec!["a", "m", "z"]);
    }

    #[test]
    fn tui_intercepts_create_database_and_opens_new_path() {
        let temp = tempfile::tempdir().unwrap();
        let current = temp.path().join("current");
        let db = BicDb::open(&current).unwrap();
        let mut app = TuiApp::new(current, db, None).unwrap();

        app.execute_command("CREATE DATABASE acme;");

        let created = temp.path().join("acme");
        assert!(!app.status.error, "{}", app.status.text);
        assert_eq!(app.path, created);
        assert!(created.join(DEFAULT_SEGMENTS_DIR).is_dir());
        assert_eq!(
            app.result.as_ref().unwrap().rows[0][0],
            SqlValue::String("create_database".to_string())
        );
    }

    #[test]
    fn tui_collection_admin_commands_create_and_drop_collection() {
        let temp = tempfile::tempdir().unwrap();
        let db = BicDb::open(temp.path()).unwrap();
        let mut app = TuiApp::new(temp.path().to_path_buf(), db, None).unwrap();

        app.execute_command(":create-collection patients");
        assert!(!app.status.error, "{}", app.status.text);
        assert!(app
            .stats
            .collections
            .iter()
            .any(|stats| stats.name == "patients"));

        app.execute_command(":drop-collection patients force");
        assert!(!app.status.error, "{}", app.status.text);
        assert!(!app
            .stats
            .collections
            .iter()
            .any(|stats| stats.name == "patients"));
    }

    #[test]
    fn parse_create_database_if_not_exists() {
        assert_eq!(
            parse_admin_statement("CREATE DATABASE IF NOT EXISTS \"acme\";")
                .unwrap()
                .unwrap(),
            AdminStatement::CreateDatabase {
                name: "acme".to_string(),
                if_not_exists: true
            }
        );
    }
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default()
}

fn format_age(millis: i64) -> String {
    let seconds = millis / 1000;
    if seconds < 60 {
        format!("{seconds}s")
    } else if seconds < 3600 {
        format!("{}m", seconds / 60)
    } else if seconds < 86_400 {
        format!("{}h", seconds / 3600)
    } else {
        format!("{}d", seconds / 86_400)
    }
}

fn short_id(id: &str) -> String {
    id.chars().take(8).collect()
}

#[cfg(test)]
mod broker_panel_tests {
    use super::*;
    use bicdb_core::{ConsumeOptions, NackOptions};
    use ratatui::backend::TestBackend;
    use serde_json::json;

    fn buffer_text(terminal: &Terminal<TestBackend>) -> String {
        terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect()
    }

    #[test]
    fn broker_panel_renders_queues_groups_messages_and_dead_letters() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(dir.path()).unwrap();
        {
            let mut broker = db.broker();
            broker
                .configure_queue(
                    "commerce.orders",
                    QueueConfig {
                        retention_max_messages: Some(1000),
                        ..QueueConfig::default()
                    },
                )
                .unwrap();
            for n in 0..3 {
                broker
                    .publish(
                        "commerce.orders",
                        json!({"order": n}),
                        serde_json::Value::Null,
                    )
                    .unwrap();
            }
            let batch = broker
                .consume(
                    "commerce.orders",
                    "workers",
                    "w1",
                    ConsumeOptions {
                        max_messages: 2,
                        visibility_timeout_ms: 30_000,
                    },
                )
                .unwrap();
            broker
                .ack("commerce.orders", "workers", "w1", batch[0].message_id)
                .unwrap();
            broker
                .nack(
                    "commerce.orders",
                    "workers",
                    "w1",
                    batch[1].message_id,
                    NackOptions {
                        requeue: false,
                        delay_ms: None,
                        error: Some("poison".into()),
                    },
                )
                .unwrap();
        }

        let mut app = TuiApp::new(dir.path().to_path_buf(), db, None).unwrap();
        app.refresh();
        app.tab = Tab::Broker;
        let backend = TestBackend::new(160, 48);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|frame| app.draw(frame)).unwrap();
        let text = buffer_text(&terminal);

        assert!(text.contains("QUEUES"), "queues table rendered");
        assert!(text.contains("commerce.orders"), "queue listed");
        assert!(text.contains("CONSUMER GROUPS"), "groups table rendered");
        assert!(text.contains("workers"), "group listed");
        assert!(text.contains("MESSAGES"), "message tail rendered");
        assert!(text.contains("order"), "payload preview visible");
        assert!(text.contains("DEAD LETTERS"), "dlq panel rendered");
        assert!(text.contains("poison"), "dead-letter error visible");
        assert!(text.contains("CONFIG"), "config panel rendered");
        assert!(text.contains("1000"), "retention shown");

        // Overview carries the broker summary line.
        app.tab = Tab::Overview;
        terminal.draw(|frame| app.draw(frame)).unwrap();
        let text = buffer_text(&terminal);
        assert!(text.contains("Broker:"), "overview broker summary");
        // The summary line may wrap mid-phrase in the narrow panel; the
        // dead-letter word itself must be visible.
        assert!(text.contains("dead"), "dead-letter count surfaced");
    }
}
