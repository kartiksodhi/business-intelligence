"""OpenCV CAPTCHA solver for Indian government portals (securimage style).

The e-Courts / NCLT portals use securimage — light gray text on white background,
clean sans-serif font, no noise arcs.

Pipeline (tested with ground truth, 5/5 correct):
  For each scale in [3x, 4x, 5x]:
    1. Grayscale + white padding (prevents edge character loss)
    2. Scale up with cubic interpolation
    3. Otsu threshold → clean binary
    4. Tight crop via connected-component bounding box (fixes leading-char segmentation bug)
    5. Tesseract PSM 13 (raw line) + PSM 7 (single line) — both run, candidates collected
  Vote on 6-char candidates → return majority winner.

Why tight crop + PSM 13:
  Tesseract's word-level segmentation (PSM 6/7/8) sometimes treats the first character
  as a separate "word" and drops it. PSM 13 (raw line, no segmentation) on a tight-cropped
  image forces Tesseract to read all characters left-to-right without layout analysis.

Usage:
    from ingestion.scrapers.captcha_solver import solve, WHITELIST_LOWER, WHITELIST_UPPER

    text = solve(image_bytes)                              # e-Courts (lowercase)
    text = solve(image_bytes, whitelist=WHITELIST_UPPER)  # NCLT (uppercase)
"""

import logging
from collections import Counter
from typing import Optional

import cv2
import numpy as np
import pytesseract
from PIL import Image

logger = logging.getLogger(__name__)

WHITELIST_LOWER = "abcdefghijklmnopqrstuvwxyz0123456789"
WHITELIST_UPPER = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
WHITELIST_DIGITS = "0123456789"

_SCALES = [3, 4, 5]


def solve(
    image_bytes: bytes,
    whitelist: str = WHITELIST_LOWER,
    min_length: int = 4,
    expected_length: int = 6,
) -> Optional[str]:
    """Solve a CAPTCHA image. Returns text or None.

    Args:
        image_bytes:     Raw image bytes (PNG/JPEG/GIF).
        whitelist:       Tesseract character whitelist.
        min_length:      Minimum chars to accept a candidate.
        expected_length: Preferred candidate length for voting (6 for securimage).
    """
    nparr = np.frombuffer(image_bytes, np.uint8)
    img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    if img is None:
        logger.warning("captcha_solver: cv2.imdecode returned None")
        return None

    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    h, w = gray.shape

    # White padding prevents Tesseract from dropping edge characters
    pad = max(4, h // 4)
    gray = cv2.copyMakeBorder(gray, pad, pad, pad, pad, cv2.BORDER_CONSTANT, value=255)

    allowed = set(whitelist)
    candidates: list[str] = []

    for scale in _SCALES:
        ph, pw = gray.shape
        scaled = cv2.resize(gray, (pw * scale, ph * scale), interpolation=cv2.INTER_CUBIC)
        _, binary = cv2.threshold(scaled, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
        cropped = _tight_crop(binary)

        for psm in (13, 7):  # PSM 13 = raw line (no segmentation); PSM 7 = single line
            cfg = f"--psm {psm} --oem 1 -c tessedit_char_whitelist={whitelist}"
            try:
                raw = pytesseract.image_to_string(Image.fromarray(cropped), config=cfg).strip()
            except Exception as e:
                logger.warning("captcha_solver: Tesseract error scale=%dx psm=%d: %s", scale, psm, e)
                continue

            text = "".join(c for c in raw.lower() if c in allowed)
            if len(text) >= min_length:
                candidates.append(text)
                logger.debug("captcha_solver scale=%dx psm=%d: %r", scale, psm, text)

    if not candidates:
        logger.warning("captcha_solver: no candidates produced")
        return None

    # Prefer expected-length candidates, then vote
    preferred = [c for c in candidates if len(c) == expected_length]
    pool = preferred if preferred else candidates
    winner, count = Counter(pool).most_common(1)[0]

    logger.debug("captcha_solver: winner=%r (%d/%d votes)", winner, count, len(pool))
    return winner


# ------------------------------------------------------------------ #
#  Internal helpers                                                    #
# ------------------------------------------------------------------ #

def _tight_crop(binary: np.ndarray, margin: int = 20) -> np.ndarray:
    """Crop binary image to the tight bounding box of all character-sized blobs.

    Fixes Tesseract's tendency to drop leading characters that it segments as
    separate words. By cropping tightly, all characters land in one tight strip
    and PSM 13 reads them correctly left-to-right.
    """
    inv = cv2.bitwise_not(binary)
    _, _, stats, _ = cv2.connectedComponentsWithStats(inv, connectivity=8)

    boxes = []
    for i in range(1, len(stats)):
        x, y, cw, ch, area = stats[i]
        aspect = cw / (ch + 1)
        # Filter to character-sized blobs: not too tiny (noise) or huge (full-image artifact)
        if 50 <= area <= 200_000 and 0.05 <= aspect <= 5.0:
            boxes.append((x, y, x + cw, y + ch))

    if not boxes:
        return binary  # fallback: return as-is

    x1 = min(b[0] for b in boxes)
    y1 = min(b[1] for b in boxes)
    x2 = max(b[2] for b in boxes)
    y2 = max(b[3] for b in boxes)

    img_h, img_w = binary.shape
    return binary[
        max(0, y1 - margin) : min(img_h, y2 + margin),
        max(0, x1 - margin) : min(img_w, x2 + margin),
    ]
