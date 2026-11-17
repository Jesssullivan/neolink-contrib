///
/// # Neolink Siren
///
/// This module handles the siren subcommand
///
/// The subcommand sounds the camera's siren once, with message 263.
///
/// # Usage
///
/// ```bash
/// neolink siren --config=config.toml CameraName
/// ```
///
use anyhow::{Context, Result};

mod cmdline;

use crate::common::NeoReactor;
pub(crate) use cmdline::Opt;

/// Entry point for the siren subcommand
///
/// Opt is the command line options
pub(crate) async fn main(opt: Opt, reactor: NeoReactor) -> Result<()> {
    let camera = reactor.get(&opt.camera).await?;

    camera
        .run_task(|camera| {
            Box::pin(async move {
                camera
                    .siren()
                    .await
                    .context("Could not send the siren command to the camera")
            })
        })
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::cmdline::{Command, Opt};
    use clap::Parser;

    #[test]
    fn siren_subcommand_parses() {
        let opt = Opt::try_parse_from(["neolink", "siren", "--config", "neolink.toml", "Garden"])
            .expect("siren parses");
        assert_eq!(
            opt.config.as_deref(),
            Some(std::path::Path::new("neolink.toml"))
        );
        match opt.cmd {
            Some(Command::Siren(siren)) => assert_eq!(siren.camera, "Garden"),
            other => panic!("expected the siren subcommand, got {:?}", other),
        }
    }

    #[test]
    fn siren_subcommand_needs_a_camera() {
        assert!(Opt::try_parse_from(["neolink", "siren", "--config", "neolink.toml"]).is_err());
    }
}
