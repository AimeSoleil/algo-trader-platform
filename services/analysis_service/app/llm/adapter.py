"""LLM 适配器 — 统一接口 + 回退逻辑 + 并行分片"""
from __future__ import annotations

import asyncio
from time import perf_counter

from shared.config import get_settings
from shared.models.blueprint import LLMTradingBlueprint
from shared.models.signal import SignalFeatures
from shared.utils import get_logger

from services.analysis_service.app.llm.base import LLMProviderBase
from services.analysis_service.app.llm.chunker import merge_blueprints, split_signal_features
from services.analysis_service.app.llm.openai_provider import OpenAIProvider

logger = get_logger("llm_adapter")


class LLMAdapter:
    """
    LLM 统一适配器：
    - 根据配置选择 primary provider
    - primary 失败时回退到 secondary
    - 大 watchlist 自动分片并行调用 LLM，减少单次请求大小
    """

    def __init__(self):
        settings = get_settings()
        self.primary_name = settings.llm.provider
        self._primary: LLMProviderBase | None = None
        self._secondary: LLMProviderBase | None = None

        # Chunking config
        self._chunk_size = settings.llm.chunk_size
        self._max_concurrent = settings.llm.max_concurrent_chunks
        self._benchmark_symbols = settings.llm.benchmark_symbols

    def _create_provider(self, name: str) -> LLMProviderBase:
        if name == "openai":
            return OpenAIProvider()
        elif name == "copilot":
            from services.analysis_service.app.llm.copilot_provider import CopilotProvider
            return CopilotProvider()
        else:
            raise ValueError(f"Unknown LLM provider: {name}")

    def _create_secondary(self, primary_name: str) -> LLMProviderBase | None:
        """创建回退 provider"""
        if primary_name == "openai":
            try:
                from services.analysis_service.app.llm.copilot_provider import CopilotProvider
                return CopilotProvider()
            except ImportError:
                return None
        elif primary_name == "copilot":
            return OpenAIProvider()
        return None

    def _get_primary(self) -> LLMProviderBase:
        if self._primary is None:
            self._primary = self._create_provider(self.primary_name)
            logger.info("llm_adapter.primary_initialized", provider=self.primary_name)
        return self._primary

    def _get_secondary(self) -> LLMProviderBase | None:
        if self._secondary is None:
            self._secondary = self._create_secondary(self.primary_name)
            if self._secondary is not None:
                logger.info("llm_adapter.secondary_initialized")
        return self._secondary

    # ------------------------------------------------------------------
    # Single-chunk generation (primary → fallback)
    # ------------------------------------------------------------------

    async def _generate_single(
        self,
        signal_features: list[SignalFeatures],
        current_positions: dict | None,
        previous_execution: dict | None,
        *,
        chunk_mode: bool = False,
    ) -> LLMTradingBlueprint:
        """Generate a blueprint for one chunk with primary→secondary fallback."""
        primary = self._get_primary()
        try:
            return await primary.generate_blueprint(
                signal_features, current_positions, previous_execution,
                chunk_mode=chunk_mode,
            )
        except Exception as e:
            logger.warning(
                "llm_adapter.primary_failed",
                provider=self.primary_name,
                error=str(e),
            )
            secondary = self._get_secondary()
            if secondary:
                logger.info("llm_adapter.fallback_to_secondary")
                return await secondary.generate_blueprint(
                    signal_features, current_positions, previous_execution,
                    chunk_mode=chunk_mode,
                )
            raise

    # ------------------------------------------------------------------
    # Public API — auto-chunking orchestrator
    # ------------------------------------------------------------------

    async def generate_blueprint(
        self,
        signal_features: list[SignalFeatures],
        current_positions: dict | None = None,
        previous_execution: dict | None = None,
    ) -> LLMTradingBlueprint:
        """生成蓝图：自动分片 → 并行 LLM 调用 → 合并结果。

        When the number of non-benchmark symbols exceeds ``chunk_size``,
        the features are split into smaller chunks (each containing
        benchmark symbols for market context).  Chunks are processed in
        parallel with a concurrency limiter and then merged into one
        coherent ``LLMTradingBlueprint``.

        Small watchlists (≤ chunk_size) take the fast single-call path
        with zero overhead.
        """
        chunks = split_signal_features(
            signal_features, self._chunk_size, self._benchmark_symbols,
        )

        # ── Fast path: single chunk → no splitting overhead ──
        if len(chunks) == 1:
            blueprint = await self._generate_single(
                chunks[0], current_positions, previous_execution,
                chunk_mode=False,
            )
            logger.info("llm_adapter.success", provider=self.primary_name, chunks=1)
            return blueprint

        # ── Multi-chunk parallel path ──
        logger.info(
            "llm_adapter.chunked_generation_start",
            total_symbols=len(signal_features),
            num_chunks=len(chunks),
            chunk_size=self._chunk_size,
            max_concurrent=self._max_concurrent,
        )
        started = perf_counter()
        semaphore = asyncio.Semaphore(self._max_concurrent)

        async def _process_chunk(idx: int, chunk: list[SignalFeatures]) -> LLMTradingBlueprint:
            async with semaphore:
                symbols = [sf.symbol for sf in chunk]
                logger.debug(
                    "llm_adapter.chunk_start",
                    chunk_index=idx,
                    symbols=symbols,
                )
                bp = await self._generate_single(
                    chunk, current_positions, previous_execution,
                    chunk_mode=True,
                )
                logger.debug(
                    "llm_adapter.chunk_done",
                    chunk_index=idx,
                    plans=len(bp.symbol_plans),
                )
                return bp

        tasks = [
            _process_chunk(i, chunk) for i, chunk in enumerate(chunks)
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Separate successes from failures
        blueprints: list[LLMTradingBlueprint] = []
        errors: list[tuple[int, Exception]] = []
        for idx, result in enumerate(results):
            if isinstance(result, BaseException):
                errors.append((idx, result))  # type: ignore[arg-type]
            else:
                blueprints.append(result)

        if errors:
            failed_indices = [i for i, _ in errors]
            logger.warning(
                "llm_adapter.chunks_failed",
                failed_chunks=failed_indices,
                errors=[str(e) for _, e in errors],
            )

        if not blueprints:
            # All chunks failed — re-raise the first error
            raise errors[0][1]

        merged = merge_blueprints(blueprints)

        elapsed_ms = round((perf_counter() - started) * 1000, 2)
        logger.info(
            "llm_adapter.chunked_generation_done",
            provider=self.primary_name,
            num_chunks=len(chunks),
            succeeded=len(blueprints),
            failed=len(errors),
            total_plans=len(merged.symbol_plans),
            elapsed_ms=elapsed_ms,
        )
        return merged
