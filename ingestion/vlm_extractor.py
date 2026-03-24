from __future__ import annotations

import base64
import json
import logging
import mimetypes
import os
from pathlib import Path
from typing import Optional

import anthropic
from pydantic import BaseModel, ValidationError, field_validator


logger = logging.getLogger(__name__)


VLM_SYSTEM_PROMPT = """You are a financial data extraction engine processing an Indian legal/bank notice.
Task: Extract the Borrower Name, Lender (Bank), Demand Amount, and Date.
Constraint 1: Mathematically convert "Lakhs" or "Crores" into a plain integer (e.g., "1.5 Crore" = 15000000).
Constraint 2: Ignore handwritten "Received" stamps; prioritize the printed letterhead.
Output strictly as JSON: {"demand_amount_inr": <INT_OR_NULL>, "date_of_notice": "<YYYY-MM-DD_OR_NULL>", "lender_name": "<STRING_OR_NULL>", "borrower_cin": "<STRING_OR_NULL>"}"""


class LegalDocumentExtractionResponse(BaseModel):
    demand_amount_inr: Optional[int]
    date_of_notice: Optional[str]
    lender_name: Optional[str]
    borrower_cin: Optional[str]

    @field_validator("demand_amount_inr", mode="before")
    @classmethod
    def _normalize_amount(cls, value):
        if value in (None, "", "null"):
            return None
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        if isinstance(value, str):
            raw = value.replace(",", "").replace("₹", "").strip()
            if not raw:
                return None
            lower = raw.lower()
            if "crore" in lower or lower.endswith("cr"):
                number = "".join(ch for ch in raw if ch.isdigit() or ch == ".")
                return int(float(number) * 10_000_000) if number else None
            if "lakh" in lower:
                number = "".join(ch for ch in raw if ch.isdigit() or ch == ".")
                return int(float(number) * 100_000) if number else None
            if raw.replace(".", "", 1).isdigit():
                return int(float(raw))
        raise ValueError("invalid demand_amount_inr")

    @field_validator("date_of_notice")
    @classmethod
    def _validate_date(cls, value):
        if value in (None, "", "null"):
            return None
        from datetime import datetime

        return datetime.strptime(value, "%Y-%m-%d").date().isoformat()

    @field_validator("lender_name", "borrower_cin", mode="before")
    @classmethod
    def _normalize_strings(cls, value):
        if value in (None, "", "null"):
            return None
        if isinstance(value, str):
            return value.strip() or None
        raise ValueError("invalid string field")


def route_document(source, file):
    if source in ["sarfaesi", "nclt", "drt"]:
        return vlm_extract(file)
    elif source == "captcha":
        return pytesseract_solve(file)
    else:
        return playwright_scrape(file)


def vlm_extract(file):
    raw = _claude_api_call(file, model="claude-sonnet-4-6")
    try:
        return LegalDocumentExtractionResponse.model_validate_json(raw).model_dump()
    except ValidationError:
        raw = _claude_api_call(file, model="claude-sonnet-4-6", temperature=0)
        try:
            return LegalDocumentExtractionResponse.model_validate_json(raw).model_dump()
        except ValidationError:
            logger.error("VLM validation failed twice; raw response: %s", raw)
            return None


def pytesseract_solve(file):
    import io

    import pytesseract
    from PIL import Image

    image_bytes = _read_file_bytes(file)
    image = Image.open(io.BytesIO(image_bytes))
    return pytesseract.image_to_string(image).strip()


def playwright_scrape(file):
    return file


def _claude_api_call(file, model: str, temperature: Optional[float] = None) -> str:
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    payload = _build_file_block(file)
    kwargs = {
        "model": model,
        "max_tokens": 300,
        "system": VLM_SYSTEM_PROMPT,
        "messages": [
            {
                "role": "user",
                "content": [
                    payload,
                    {"type": "text", "text": "Return only the JSON object described in the system prompt."},
                ],
            }
        ],
    }
    if temperature is not None:
        kwargs["temperature"] = temperature

    response = client.messages.create(**kwargs)
    text_blocks = [block.text for block in response.content if getattr(block, "type", None) == "text"]
    return "".join(text_blocks).strip()


def _build_file_block(file):
    data = _read_file_bytes(file)
    media_type = _media_type_for(file)
    block_type = "document" if media_type == "application/pdf" else "image"
    return {
        "type": block_type,
        "source": {
            "type": "base64",
            "media_type": media_type,
            "data": base64.b64encode(data).decode("utf-8"),
        },
    }


def _read_file_bytes(file) -> bytes:
    if isinstance(file, bytes):
        return file
    if isinstance(file, Path):
        return file.read_bytes()
    if isinstance(file, str):
        return Path(file).read_bytes()
    raise TypeError("file must be bytes, str path, or Path")


def _media_type_for(file) -> str:
    if isinstance(file, (str, Path)):
        guessed, _ = mimetypes.guess_type(str(file))
        if guessed:
            return guessed
    return "image/png"

