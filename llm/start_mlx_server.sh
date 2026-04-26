#!/usr/bin/env bash
# Start MLX inference server for the canonical llm_refine step.
set -euo pipefail

MODEL="${GALACTUS_MLX_MODEL:-mlx-community/gemma-3-4b-it-qat-4bit}"
PORT="${GALACTUS_MLX_PORT:-8081}"
DECODE_CONCURRENCY="${GALACTUS_MLX_DECODE_CONCURRENCY:-4}"
PROMPT_CONCURRENCY="${GALACTUS_MLX_PROMPT_CONCURRENCY:-4}"

exec .venv/bin/mlx_lm.server \
    --model "$MODEL" \
    --port "$PORT" \
    --temp 0.0 \
    --decode-concurrency "$DECODE_CONCURRENCY" \
    --prompt-concurrency "$PROMPT_CONCURRENCY" \
    --log-level INFO
