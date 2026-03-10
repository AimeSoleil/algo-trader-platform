"""Trading analysis skill — Claude SKILL.md standard.

The ``trading-analysis/`` directory contains the skill bundle
(SKILL.md + references/ + assets/) that is mounted into the LLM
runtime by each provider's native skill mechanism:

* **OpenAI** — Responses API ``shell`` tool with inline skill bundle.
* **Copilot SDK** — ``skill_directories`` session configuration.

No Python loader is required; the model reads SKILL.md and
navigates references autonomously.
"""
