# OpenClaw VPS Setup & Fixes Log

This document serves as a record of all the configurations, network adjustments, and security settings applied to the remote OpenClaw instance hosted on the VPS (72.61.235.245). 

This file is necessary to give context to Claude, Codex, or any other agent reviewing the architecture so they understand how OpenClaw is deployed, why certain errors occurred, and how they were resolved.

## 1. Network Architecture
- **Host System**: Ubuntu VPS
- **Deployment**: Docker Compose (`openclaw-gateway`, `openclaw-cli`, and `ollama`)
- **Network Mode**: Changed to `network_mode: "host"` so all containers securely communicate on `127.0.0.1` locally. This bypassed previous internal DNS and Docker Bridge separation issues.
- **Access Protocol**: The system is NOT exposed publicly. It relies on an SSH Tunnel (`ssh -L 18789:127.0.0.1:18789 root@72.61.235.245`).

## 2. Issues Diagnosed & Resolved

### Issue A: "Pairing Required" and "Untrusted Proxy"
- **Cause**: OpenClaw strictly enforces Device Pairing for connections originating from external IPs or proxies. When accessed via Cloudflare, OpenClaw rejected the connection.
- **Resolution**: 
  - Purged the Cloudflare tunnel entirely to keep architecture clean and secure. 
  - Restored reliance on the SSH Tunnel (`127.0.0.1`), meaning the connection appears 100% local to OpenClaw. 
  - By using `localhost:18789` via the SSH SSH tunnel, OpenClaw skips the DM pairing entirely and relies strictly on the `OPENCLAW_GATEWAY_TOKEN`.

### Issue B: "API limits Reached" and Gemini Fallbacks
- **Cause**: OpenClaw lost its internal token mapping connecting it to the local Ollama container. When this happened, OpenClaw panicked and fell back to the Google Gemini API key. Because the Gemini API key used was highly experimental/preview, it suffered from severe quota limitations (API rate limit).
- **Resolution**: 
  - Re-mapped Ollama explicitly within OpenClaw. 
  - Locked `OLLAMA_API_KEY=ollama-local` in the persistent environment and generated a proper `auth-profiles.json`.
  - OpenClaw is now firmly pinned to use the local, entirely free, limit-less Ollama (`llama3:latest`). Gemini will no longer intervene unless everything completely fails.

### Issue C: Sandbox / Docker Issues
- **Cause**: The original base image for OpenClaw does not have the Docker CLI installed inside it. When OpenClaw is tasked with a dangerous code block, it spawns a sandbox, which requires the Docker CLI to communicate with `/var/run/docker.sock`. Recreating the container continually wiped out manual `apt-get` installations.
- **Resolution Initiated**: Passed `/var/run/docker.sock` explicitly into the mount points and initiated a fresh terminal installation of `docker.io` within the OpenClaw container. *(Note: Work on this specific task was halted per the user's latest request to avoid further active modifications on the server).*

## 3. Final Configurations

**`openclaw.json` (Security Enforcement)**
- `tools.exec.security` is set to `deny`, and `ask` is set to `always`.
- `tools.fs.workspaceOnly` is set to `true`.
- OpenClaw is strictly invisible to the internet.

**`models.json` (Brain Integrity)**
- Overrode the context window of `llama3:latest` from `8,192` to `32,768` to ensure no "Context Length Exceeded" errors happen during heavy workflows.

## 4. Current State
As is, OpenClaw acts entirely as a remote API and Gateway locked behind an SSH tunnel. It expects `Gateway Token` authentication via `ws://127.0.0.1:18789`.
