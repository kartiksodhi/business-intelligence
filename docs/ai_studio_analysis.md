# AI Studio Business Intelligence App Analysis

**URL**: [AI Studio App (23fdf69a-3598-4a92-802b-c58d1531b236)](https://aistudio.google.com/apps/23fdf69a-3598-4a92-802b-c58d1531b236?showAssistant=true)

## Core Mission
The app is designed as a professional corporate intelligence platform for Indian banks, lenders, and Asset Reconstruction Companies (ARCs). It identifies corporate distress, predicts financial signals, and maps contagion risks using real-time public data.

## Configuration
- **Model**: `gemini-3-flash-preview`
- **Capabilities**: Google Search (Search tool) enabled for live data retrieval.
- **Safety**: Standard AI Studio safety settings.
- **Output Mode**: Strict JSON Enforcement (`application/json`).

## System Logic (Prompt Engineering)
The app operates under a "Senior Business Intelligence Analyst" persona.

### Personality & Role
> "You are a Senior Business Intelligence Analyst specializing in Indian corporate distress and asset reconstruction intelligence. Your objective is to provide high-fidelity intelligence for banks, lenders, and ARCs to identify risks or opportunities."

### Analytical Pipeline
1.  **Verification**: Confirm legal identity via CIN.
2.  **Signal Detection**: Scrape and interpret data from MCA (Ministry of Corporate Affairs) and NCLT (National Company Law Tribunal).
3.  **Contagion Mapping**: Identify related entities (subsidiaries, partners, shared directors).
4.  **Impact Analysis**: Objectively state facts, financial implications, and risk spread without emotional adjectives.

## Data Structure (Output Schema)
The results are structured to facilitate database ingestion (React/Next.js UI compatible):
- `name`: String (Company Name)
- `cin`: String (21-character CIN)
- `healthScore`: Integer (0-100)
- `healthStatus`: Enum (CRITICAL | ALERT | WATCH | INFO)
- `signals`: Array of objects (Type, Description, Date)
- `contagionRisk`: Detailed assessment of network risk.
- `analysis`: 3-5 clinical bullet points.

## Integration Potential
This app's logic perfectly mirrors the "Distressed Asset" focus of the BI Signal Network project. The prompts used here can be directly implemented into the backend `bi_engine` or used to refine the "Alert Analyst Synthesizer" in `INTELLIGENCE.md`.

---
> [!TIP]
> This app uses `gemini-3-flash-preview` which provides extremely fast results with real-time search capabilities. This is a significant upgrade for identifying current events not yet in the OGD historical datasets.
