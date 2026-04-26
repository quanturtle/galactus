"""Thin MLX-server client (OpenAI-compatible /v1/chat/completions)."""

import json
import logging
import os
from typing import Any

import httpx

logger = logging.getLogger(__name__)


def _repair_json(s: str) -> str:
    """Auto-close unclosed ``[`` arrays before a ``}`` close-curly.

    gemma-3-4b consistently emits JSON like ``"members": ["x"}`` (missing
    ``]``); a structural walk fixes the common case without breaking valid
    input.
    """
    out: list[str] = []
    stack: list[str] = []
    in_string = False
    escape = False
    for c in s:
        if escape:
            escape = False
            out.append(c)
            continue
        if in_string:
            if c == "\\":
                escape = True
            elif c == '"':
                in_string = False
            out.append(c)
            continue
        if c == '"':
            in_string = True
            out.append(c)
        elif c in "{[":
            stack.append(c)
            out.append(c)
        elif c == "]":
            if stack and stack[-1] == "[":
                stack.pop()
            out.append(c)
        elif c == "}":
            while stack and stack[-1] == "[":
                out.append("]")
                stack.pop()
            if stack and stack[-1] == "{":
                stack.pop()
            out.append(c)
        else:
            out.append(c)
    # Trailing unclosed brackets at end-of-stream
    while stack:
        out.append("]" if stack.pop() == "[" else "}")
    return "".join(out)


def _setting(name: str, default: str) -> str:
    return os.environ.get(name, default)


def mlx_url() -> str:
    return _setting("GALACTUS_MLX_URL", "http://localhost:8081")


def mlx_model() -> str:
    return _setting("GALACTUS_MLX_MODEL", "mlx-community/gemma-3-4b-it-qat-4bit")


def llm_timeout() -> int:
    return int(_setting("GALACTUS_LLM_TIMEOUT", "60"))


async def chat_json(
    system: str,
    user: str,
    *,
    client: httpx.AsyncClient | None = None,
    max_tokens: int = 512,
) -> dict[str, Any] | list[Any] | None:
    """POST to MLX /v1/chat/completions, parse JSON content.

    Strips ``` code fences. Returns None on parse failure or HTTP error
    (caller decides whether to keep going).
    """
    payload = {
        "model": mlx_model(),
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": 0,
        "max_tokens": max_tokens,
    }

    async def _do(c: httpx.AsyncClient) -> dict[str, Any] | list[Any] | None:
        resp = await c.post(f"{mlx_url()}/v1/chat/completions", json=payload)
        resp.raise_for_status()
        data = resp.json()
        content = data["choices"][0]["message"]["content"].strip()
        usage = data.get("usage", {})
        logger.debug(
            "MLX: %d prompt + %d completion tokens",
            usage.get("prompt_tokens", 0),
            usage.get("completion_tokens", 0),
        )
        if content.startswith("```"):
            content = content.split("\n", 1)[-1]
            if content.endswith("```"):
                content = content[:-3].strip()
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            try:
                return json.loads(_repair_json(content))
            except json.JSONDecodeError:
                logger.warning("Malformed JSON from MLX: %s", content[:500])
                return None

    if client is not None:
        return await _do(client)
    async with httpx.AsyncClient(timeout=llm_timeout()) as c:
        return await _do(c)
