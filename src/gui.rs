use iced::widget::{button, column, slider, text};
use iced::widget::canvas::{self, Canvas, Frame, Geometry, Path, Stroke};
use iced::{Application, Command, Element, Length, Settings, Subscription, Theme};
use iced::{executor, time, Color, Point, Rectangle, Size};
use std::sync::mpsc::{self, Receiver, Sender};
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct GuiState {
    pub bankroll: f64,
    pub last_market: String,
    pub last_status: String,
    pub epsilon: f64,
    pub threshold: f64,
}

impl GuiState {
    pub fn new(bankroll: f64) -> Self {
        Self {
            bankroll,
            last_market: "-".to_string(),
            last_status: "Starting".to_string(),
            epsilon: 0.0,
            threshold: 0.08,
        }
    }

    pub fn with_status(bankroll: f64, last_market: String, last_status: String) -> Self {
        Self {
            bankroll,
            last_market,
            last_status,
            epsilon: 0.0,
            threshold: 0.08,
        }
    }

    pub fn with_params(
        bankroll: f64,
        last_market: String,
        last_status: String,
        epsilon: f64,
        threshold: f64,
    ) -> Self {
        Self {
            bankroll,
            last_market,
            last_status,
            epsilon,
            threshold,
        }
    }
}

pub struct GuiHandle {
    sender: Sender<GuiState>,
    command_receiver: Receiver<GuiCommand>,
}

impl GuiHandle {
    pub fn update(&self, state: GuiState) {
        let _ = self.sender.send(state);
    }

    pub fn try_recv_command(&self) -> Option<GuiCommand> {
        self.command_receiver.try_recv().ok()
    }
}

pub fn create_gui(initial: GuiState) -> (GuiHandle, GuiFlags) {
    let (tx, rx) = mpsc::channel();
    let (cmd_tx, cmd_rx) = mpsc::channel();
    (
        GuiHandle {
            sender: tx,
            command_receiver: cmd_rx,
        },
        GuiFlags {
            initial,
            receiver: rx,
            command_sender: cmd_tx,
        },
    )
}

pub fn run_gui(flags: GuiFlags) {
    #[cfg(target_os = "windows")]
    {
        if std::env::var("WGPU_BACKEND").is_err() {
            // Avoid noisy Vulkan present-mode warnings on some Windows drivers.
            std::env::set_var("WGPU_BACKEND", "dx12");
        }
    }
    let mut settings = Settings::with_flags(flags);
    settings.window.size = Size::new(420.0, 240.0);
    let _ = AgentGui::run(settings);
}

pub struct GuiFlags {
    initial: GuiState,
    receiver: Receiver<GuiState>,
    command_sender: Sender<GuiCommand>,
}

#[derive(Debug, Clone)]
enum Message {
    Tick,
    TogglePause,
    SetEpsilon(f32),
    SetThreshold(f32),
    ClearThreshold,
    SetBankroll(f32),
    ResetBankroll,
}

#[derive(Debug, Clone)]
pub enum GuiCommand {
    TogglePause,
    SetEpsilon(f64),
    SetThreshold(f64),
    ClearThreshold,
    ResetBankroll(f64),
}

struct AgentGui {
    state: GuiState,
    receiver: Receiver<GuiState>,
    command_sender: Sender<GuiCommand>,
    paused: bool,
    bankroll_target: f64,
    bankroll_history: Vec<f64>,
}

impl Application for AgentGui {
    type Executor = executor::Default;
    type Message = Message;
    type Theme = Theme;
    type Flags = GuiFlags;

    fn new(flags: Self::Flags) -> (Self, Command<Self::Message>) {
        let initial = flags.initial.clone();
        (
            Self {
                state: initial.clone(),
                receiver: flags.receiver,
                command_sender: flags.command_sender,
                paused: false,
                bankroll_target: initial.bankroll,
                bankroll_history: vec![initial.bankroll],
            },
            Command::none(),
        )
    }

    fn title(&self) -> String {
        "Polymarket Agent".to_string()
    }

    fn update(&mut self, message: Self::Message) -> Command<Self::Message> {
        match message {
            Message::Tick => {
                while let Ok(state) = self.receiver.try_recv() {
                    self.state = state.clone();
                    self.bankroll_history.push(state.bankroll);
                    if self.bankroll_history.len() > 120 {
                        let drain = self.bankroll_history.len() - 120;
                        self.bankroll_history.drain(0..drain);
                    }
                }
            }
            Message::TogglePause => {
                self.paused = !self.paused;
                let _ = self.command_sender.send(GuiCommand::TogglePause);
            }
            Message::SetEpsilon(value) => {
                let epsilon = value as f64;
                self.state.epsilon = epsilon;
                let _ = self.command_sender.send(GuiCommand::SetEpsilon(epsilon));
            }
            Message::SetThreshold(value) => {
                let threshold = value as f64;
                self.state.threshold = threshold;
                let _ = self.command_sender.send(GuiCommand::SetThreshold(threshold));
            }
            Message::ClearThreshold => {
                let _ = self.command_sender.send(GuiCommand::ClearThreshold);
            }
            Message::SetBankroll(value) => {
                self.bankroll_target = value as f64;
            }
            Message::ResetBankroll => {
                let _ = self
                    .command_sender
                    .send(GuiCommand::ResetBankroll(self.bankroll_target));
            }
        }
        Command::none()
    }

    fn view(&self) -> Element<Self::Message> {
        let pause_label = if self.paused { "Resume" } else { "Pause" };
        let epsilon_slider = slider(0.0..=0.5, self.state.epsilon as f32, Message::SetEpsilon);
        let threshold_slider =
            slider(0.03..=0.12, self.state.threshold as f32, Message::SetThreshold);
        let bankroll_slider =
            slider(10.0..=500.0, self.bankroll_target as f32, Message::SetBankroll);

        column![
            text(format!("Bankroll: ${:.2}", self.state.bankroll)),
            text(format!("Last Market: {}", self.state.last_market)),
            text(format!("Status: {}", self.state.last_status)),
            button(pause_label).on_press(Message::TogglePause),
            text(format!("Epsilon: {:.2}", self.state.epsilon)),
            epsilon_slider,
            text(format!("Edge Threshold: {:.2}", self.state.threshold)),
            threshold_slider,
            button("Clear Threshold Override").on_press(Message::ClearThreshold),
            text(format!("Sim Bankroll Target: ${:.0}", self.bankroll_target)),
            bankroll_slider,
            button("Reset Bankroll (Shadow)").on_press(Message::ResetBankroll),
            chart_widget(&self.bankroll_history),
        ]
        .spacing(8)
        .width(Length::Fill)
        .into()
    }

    fn subscription(&self) -> Subscription<Self::Message> {
        time::every(Duration::from_millis(500)).map(|_| Message::Tick)
    }
}

fn chart_widget<'a>(history: &[f64]) -> Element<'a, Message> {
    Canvas::new(BankrollChart {
        history: history.to_vec(),
    })
    .width(Length::Fill)
    .height(Length::Fixed(120.0))
    .into()
}

struct BankrollChart {
    history: Vec<f64>,
}

impl canvas::Program<Message> for BankrollChart {
    type State = ();

    fn draw(
        &self,
        _state: &Self::State,
        _renderer: &iced::Renderer,
        _theme: &Theme,
        bounds: Rectangle,
        _cursor: iced::mouse::Cursor,
    ) -> Vec<Geometry> {
        let mut frame = Frame::new(_renderer, bounds.size());
        let bg = Path::rectangle(Point::ORIGIN, frame.size());
        frame.fill(&bg, Color::from_rgb(0.08, 0.08, 0.09));

        if self.history.len() < 2 {
            return vec![frame.into_geometry()];
        }

        let min = self
            .history
            .iter()
            .copied()
            .fold(f64::INFINITY, f64::min);
        let max = self
            .history
            .iter()
            .copied()
            .fold(f64::NEG_INFINITY, f64::max);
        let range = (max - min).max(1e-6);

        let w = bounds.width;
        let h = bounds.height;
        let left = 10.0_f32;
        let right = (w - 10.0).max(left + 1.0);
        let top = 8.0_f32;
        let bottom = (h - 10.0).max(top + 1.0);
        let span_x = right - left;
        let span_y = bottom - top;

        let path = Path::new(|builder| {
            for (idx, value) in self.history.iter().enumerate() {
                let t = if self.history.len() <= 1 {
                    0.0
                } else {
                    idx as f32 / (self.history.len() - 1) as f32
                };
                let x = left + t * span_x;
                let normalized = ((*value - min) / range) as f32;
                let y = bottom - normalized * span_y;
                let point = Point::new(x, y);
                if idx == 0 {
                    builder.move_to(point);
                } else {
                    builder.line_to(point);
                }
            }
        });

        frame.stroke(
            &path,
            Stroke::default()
                .with_width(2.0)
                .with_color(Color::from_rgb(0.18, 0.72, 0.96)),
        );

        vec![frame.into_geometry()]
    }
}
