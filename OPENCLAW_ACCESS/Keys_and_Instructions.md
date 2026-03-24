# 🛡️ OpenClaw Secure Access Dashboard

| Asset | Value |
| :--- | :--- |
| **Gateway Token** | `39b5a60f1e32595ecc88298660ff3986dcd3acfeabf41a03796712200b8811b7` |
| **Local Port** | `18789` |
| **Model** | `ollama/llama3` (Infinite Free Brain) |
| **VPS IP** | `72.61.235.245` |

---

### 🛡️ **Endorsed Access Method (SSH Tunnel ONLY):**
OpenClaw is restricted to `127.0.0.1` locally on the VPS for security (avoiding device pairing limits). Cloudflare tunnels or Reverse Proxies are **NOT supported** as they trigger OpenClaw's strict proxy header rules and break UI connection logic.

**Always run this command in your Mac Terminal before accessing the UI:**
`ssh -L 18789:127.0.0.1:18789 root@72.61.235.245`

### 🚀 **To Access:**
Once the tunnel is up, go to:
[http://localhost:18789](http://localhost:18789)
