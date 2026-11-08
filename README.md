# Neolink

![CI](https://github.com/alexbahder/neolink/workflows/CI/badge.svg)
[![dependency status](https://deps.rs/repo/github/alexbahder/neolink/status.svg)](https://deps.rs/repo/github/alexbahder/neolink)

Neolink is a small program that acts as a proxy between Reolink IP cameras and
normal RTSP clients.
Certain cameras, such as the Reolink B800, do not implement ONVIF or RTSP, but
instead use a proprietary "Baichuan" protocol only compatible with their apps
and NVRs (any camera that uses "port 9000" will likely be using this protocol).
Neolink allows you to use NVR software such as Shinobi or Blue Iris to receive
video from these cameras instead.
The Reolink NVR is not required, and the cameras are unmodified.
Your NVR software connects to Neolink, which forwards the video stream from the
camera.

The Neolink project is not affiliated with Reolink in any way; everything it
does has been reverse engineered.

## This Fork

This fork is a patch / fix of [QuantumEntangledAndy's](https://github.com/QuantumEntangledAndy/neolink) fork, which is an extension of
[thirtythreeforty's](https://github.com/thirtythreeforty/neolink) with additional
features not yet in upstream master.

**Major Features**:
Memoryleak that resulted in crashes after extended runtime of 24/7 streaming should be gone. 

**Minor Features**:

- Improved AV Sync, should now not have weird drift of sync of audio and video.

## Installation

Look at [QuantumEntangledAndy's](https://github.com/QuantumEntangledAndy/neolink) page for installation and configuration instructions. I will try and add them here later, adjusted for that this is now a different fork. 

## License

Neolink is free software, released under the GNU Affero General Public License
v3.

This means that if you incorporate it into a piece of software available over
the network, you must offer that software's source code to your users.

## Donations

If you find this code helpful please consider supporting development via contributions to the codebase / feedback, and/or donations to [QuantumEntangledAndy](https://github.com/QuantumEntangledAndy/) for their work on the original (current) mainstream. 

## Disclaimers

- I am not a programmer. 
- This branch was written with the help of AI tools.
- This software is provided with **ABSOLUTELY NO WARRANTY**. 
