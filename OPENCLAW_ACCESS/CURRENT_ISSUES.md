# 🚨 RESOLVED OPENCLAW ISSUES & ERROR LOGS

> **ATTENTION CLAUDE, CODEX, AND AI ASSISTANTS:**
> This document contains the exact error outputs and issues the user *previously* faced with the remote OpenClaw instance hosted on their VPS. Access is strictly via SSH Tunnel `18789:127.0.0.1:18789`.
> **These issues have now been resolved as described below.**

---

## ✅ Issue 1: The "Sandbox mode requires Docker" Error (RESOLVED)

**Exact Error Log from OpenClaw UI:**
```text
⚠️ Agent failed before reply: Sandbox mode requires Docker, but the "docker" command was not found in PATH.
```

**Resolution:**
A custom Dockerfile was built on the VPS (`openclaw:local-with-docker`) pulling the base OpenClaw image and natively installing `docker.io` via apt-get. The internal Sandbox module now successfully accesses `/var/run/docker.sock` and survives any gateway container restarts.

---

## ✅ Issue 2: API Rate Limits & Model Fallback Loop (RESOLVED)

**Exact Error Log from OpenClaw UI:**
```text
⚠️ API rate limit reached. Please try again later.
All models failed (2): ollama/llama3: Unknown model: ollama/llama3 [...] google/gemini-3-flash-preview: ⚠️ API rate limit reached.
```

**Resolution:**
The missing provider authentication issues with Ollama were resolved by hardcoding `OLLAMA_API_KEY=ollama-local` and injecting the persistent base URL into the config. All Google Gemini fallbacks were **completely removed** to prevent rate-limit loops. OpenClaw routes exclusively through the local unlimited Llama 3 model.

---

## ✅ Issue 3: The "Pairing Required" Loop (Cloudflare Web Proxy vs Localhost) (RESOLVED)

**Exact Error Log (Backend VPS logs):**
```text
[ws] Proxy headers detected from untrusted address...
code=1008 reason=pairing required
```

**Resolution:**
The Cloudflare Reverse Proxy/Tunnel was entirely uninstalled. OpenClaw relies solely on the SSH port forward (`localhost:18789`). This bypasses the pairing protocol enforcement entirely because all requests originate purely from `127.0.0.1`. Do not attempt to use reverse proxies without strict whitelist configuration.
