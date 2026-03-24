# OpenClaw Architecture and Fixes (Updated 2026-03-24)

## Current Setup

OpenClaw is hosted on a remote Ubuntu VPS (root@72.61.235.245) and is accessed via an SSH tunnel for security.
Local access: http://localhost:18789
SSH Tunnel command: ssh -L 18789:127.0.0.1:18789 root@72.61.235.245

### Resolved Issues

- Sandbox: OpenClaw now uses a custom image (openclaw:local-with-docker) that includes the Docker CLI natively for sandboxing.
- API Rate Limits: Anthropic and Gemini fallbacks have been removed. The system now exclusively uses a local Ollama instance (Llama 3).
- Context Window: The context window for Llama 3 has been increased to 32,768 tokens (up from 8,192) to meet the agent's 16,000 token minimum requirement.
- Authentication: Ollama authentication is managed via OLLAMA_API_KEY="ollama-local" and a manually configured auth-profiles.json.

### Persistent Files (on VPS)
/root/.openclaw/openclaw.json: Main gateway configuration.
/root/.openclaw/agents/main/agent/models.json: Agent-specific model metadata (pinned to 32k).
/root/.openclaw/agents/main/agent/auth-profiles.json: Authentication profiles.

## Maintenance Notes
If the context window error reappears after an image update, verify that:
1. /home/node/.openclaw/agents/main/agent/models.json contains 32768 for contextWindow.
2. The Ollama provider baseUrl is correctly pointing to 127.0.0.1:11434.
