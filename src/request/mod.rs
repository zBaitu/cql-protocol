use crate::message::Message;

pub use auth_response::AuthResponse;
pub use batch::Batch;
pub use execute::Execute;
pub use options::Options;
pub use prepare::Prepare;
pub use query::Query;
pub use register::Register;
pub use startup::Startup;

pub mod auth_response;
pub mod batch;
pub mod execute;
pub mod options;
pub mod prepare;
pub mod query;
pub mod register;
pub mod startup;

pub trait Request: Message {}
