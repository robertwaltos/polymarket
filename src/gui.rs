use iced::widget::{button, column, slider, text};
use iced::{Application, Command, Element, Length, Settings, Subscription, Theme};
use iced::{executor, time};
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

pub fn spawn_gui(initial: GuiState) -> GuiHandle {
    let (tx, rx) = mpsc::channel();
    let (cmd_tx, cmd_rx) = mpsc::channel();
    std::thread::spawn(move || {
        let mut settings = Settings::default();
        settings.window.size = (420.0, 240.0);
        settings.flags = GuiFlags {
            initial,
            receiver: rx,
            command_sender: cmd_tx,
        };
        let _ = AgentGui::run(settings);
    });

    GuiHandle {
        sender: tx,
        command_receiver: cmd_rx,
    }
}

struct GuiFlags {
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
}

#[derive(Debug, Clone)]
pub enum GuiCommand {
    TogglePause,
    SetEpsilon(f64),
    SetThreshold(f64),
    ClearThreshold,
}

struct AgentGui {
    state: GuiState,
    receiver: Receiver<GuiState>,
    command_sender: Sender<GuiCommand>,
    paused: bool,
}

impl Application for AgentGui {
    type Executor = executor::Default;
    type Message = Message;
    type Theme = Theme;
    type Flags = GuiFlags;

    fn new(flags: Self::Flags) -> (Self, Command<Self::Message>) {
        (
            Self {
                state: flags.initial,
                receiver: flags.receiver,
                command_sender: flags.command_sender,
                paused: false,
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
                    self.state = state;
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
        }
        Command::none()
    }

    fn view(&self) -> Element<Self::Message> {
        let pause_label = if self.paused { "Resume" } else { "Pause" };
        let epsilon_slider = slider(0.0..=0.5, self.state.epsilon as f32, Message::SetEpsilon);
        let threshold_slider =
            slider(0.03..=0.12, self.state.threshold as f32, Message::SetThreshold);

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
            chart_widget(),
        ]
        .spacing(8)
        .width(Length::Fill)
        .into()
    }

    fn subscription(&self) -> Subscription<Self::Message> {
        time::every(Duration::from_millis(500)).map(|_| Message::Tick)
    }
}

#[cfg(feature = "gui-charts")]
fn chart_widget<'a>() -> Element<'a, Message> {
    use plotters::prelude::*;
    use plotters_iced::{Chart, ChartWidget};

    struct MarketChart;

    impl Chart<Message> for MarketChart {
        fn build_chart<DB: DrawingBackend>(&self, builder: ChartBuilder<DB>) {
            let _ = builder
                .margin(10)
                .set_label_area_size(LabelAreaPosition::Left, 40)
                .set_label_area_size(LabelAreaPosition::Bottom, 30)
                .build_cartesian_2d(0f32..10f32, 0f32..1f32)
                .map(|mut chart| {
                    let _ = chart.configure_mesh().draw();
                    let series = (0..10).map(|i| (i as f32, i as f32 * 0.1));
                    let _ = chart.draw_series(LineSeries::new(series, &RED));
                });
        }
    }

    ChartWidget::new(MarketChart).into()
}

#[cfg(not(feature = "gui-charts"))]
fn chart_widget<'a>() -> Element<'a, Message> {
    text("Charts disabled (enable feature gui-charts)").into()
}
