use clap::Parser;

/// The siren command sounds the camera's siren once
#[derive(Parser, Debug)]
pub struct Opt {
    /// The name of the camera to sound the siren on. Must be a name in the config
    pub camera: String,
}
