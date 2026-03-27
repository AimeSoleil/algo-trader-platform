"""LLM 适配器 — 统一接口 + 回退逻辑 + 并行分片 + multi-agent 模式"""
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
from shared.metrics import llm_fallback_total, llm_circuit_open_total

logger = get_logger("llm_adapter")


class _CircuitBreaker:
    """Simple circuit breaker for LLM providers."""

    def __init__(self, threshold: int = 5, cooldown: float = 60.0):
        self._threshold = threshold
        self._cooldown = cooldown
        self._failure_count = 0
        self._opened_at: float | None = None

    @property
    def is_open(self) -> bool:
        if self._opened_at is None:
            return False
        # Check if cooldown has elapsed (half-open)
        from time import monotonic
        if monotonic() - self._opened_at >= self._cooldown:
            return False
        return True

    def record_success(self) -> None:
        self._failure_count = 0
        self._opened_at = None

    def record_failure(self) -> None:
        self._failure_count += 1
        if self._failure_count >= self._threshold:
            from time import monotonic
            self._opened_at = monotonic()


class LLMAdapter:
    """
    LLM 统一适配器：
    - 根据配置选择 primary provider
    - primary 失败时回退到 secondary
    - 大 watchlist 自动分片并行调用 LLM，减少单次请求大小
    """

    def __init__(self):
        settings = get_settings()
        self.primary_name = settings.analysis_service.llm.provider
        self._primary: LLMProviderBase | None = None
        self._secondary: LLMProviderBase | None = None

        # Chunking config
        self._chunk_size = settings.analysis_service.llm.chunk_size
        self._max_concurrent = settings.analysis_service.llm.max_concurrent_chunks
        self._benchmark_symbols = settings.analysis_service.llm.benchmark_symbols

        # Global concurrency limiter (shared across all invocations)
        self._semaphore = asyncio.Semaphore(self._max_concurrent)

        # Per-provider circuit breakers
        self._circuit: dict[str, _CircuitBreaker] = {}

        # Agentic mode
        self._agentic_mode = settings.analysis_service.llm.agentic_mode
        self._orchestrator = None  # lazy-init

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

    def _get_circuit(self, provider_name: str) -> _CircuitBreaker:
        """Get or create circuit breaker for a provider."""
        if provider_name not in self._circuit:
            settings = get_settings()
            self._circuit[provider_name] = _CircuitBreaker(
                threshold=settings.analysis_service.llm.circuit_breaker_threshold,
                cooldown=settings.analysis_service.llm.circuit_breaker_cooldown_seconds,
            )
        return self._circuit[provider_name]

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
        """Generate a blueprint for one chunk with primary→secondary fallback + circuit breaker."""
        primary = self._get_primary()
        primary_cb = self._get_circuit(self.primary_name)

        if not primary_cb.is_open:
            try:
                bp = await primary.generate_blueprint(
                    signal_features, current_positions, previous_execution,
                    chunk_mode=chunk_mode,
                )
                primary_cb.record_success()
                return bp
            except Exception as e:
                primary_cb.record_failure()
                if primary_cb.is_open:
                    llm_circuit_open_total.labels(provider=self.primary_name).inc()
                    logger.warning(
                        "llm_adapter.circuit_opened",
                        provider=self.primary_name,
                    )
                logger.warning(
                    "llm_adapter.primary_failed",
                    provider=self.primary_name,
                    error=str(e),
                )
        else:
            logger.info(
                "llm_adapter.primary_circuit_open",
                provider=self.primary_name,
            )

        secondary = self._get_secondary()
        if secondary:
            llm_fallback_total.inc()
            logger.info("llm_adapter.fallback_to_secondary")
            secondary_name = "copilot" if self.primary_name == "openai" else "openai"
            secondary_cb = self._get_circuit(secondary_name)
            try:
                bp = await secondary.generate_blueprint(
                    signal_features, current_positions, previous_execution,
                    chunk_mode=chunk_mode,
                )
                secondary_cb.record_success()
                return bp
            except Exception as e:
                secondary_cb.record_failure()
                raise
        raise RuntimeError(f"Primary provider {self.primary_name} failed and no secondary available")

    # ------------------------------------------------------------------
    # Public API — auto-chunking orchestrator
    # ------------------------------------------------------------------

    async def generate_blueprint(
        self,
        signal_features: list[SignalFeatures],
        current_positions: dict | None = None,
        previous_execution: dict | None = None,
    ) -> LLMTradingBlueprint:
        """生成蓝图：agentic 模式或 legacy 分片模式。

        When ``agentic_mode`` is enabled, delegates to the multi-agent
        orchestrator (6 specialist agents → Synthesizer → Critic).
        Falls back to the legacy chunk-split pipeline on failure.

        When ``agentic_mode`` is disabled, uses the classic
        split → parallel LLM calls → merge approach.
        """
        if self._agentic_mode:
            return await self._generate_agentic(
                signal_features, current_positions, previous_execution,
            )
        return await self._generate_legacy(
            signal_features, current_positions, previous_execution,
        )

    # ------------------------------------------------------------------
    # Agentic pipeline (multi-agent orchestrator)
    # ------------------------------------------------------------------

    def _get_orchestrator(self):
        """Lazy-init the AgentOrchestrator."""
        if self._orchestrator is None:
            from services.analysis_service.app.llm.agents.orchestrator import AgentOrchestrator
            self._orchestrator = AgentOrchestrator()
            logger.info("llm_adapter.orchestrator_initialized")
        return self._orchestrator

    async def _generate_agentic(
        self,
        signal_features: list[SignalFeatures],
        current_positions: dict | None,
        previous_execution: dict | None,
    ) -> LLMTradingBlueprint:
        """Multi-agent pipeline with fallback to legacy on failure."""
        started = perf_counter()
        try:
            orchestrator = self._get_orchestrator()
            blueprint = await orchestrator.generate(
                signal_features=signal_features,
                current_positions=current_positions,
                previous_execution=previous_execution,
            )
            elapsed_ms = round((perf_counter() - started) * 1000, 2)
            logger.info(
                "llm_adapter.agentic_success",
                plans=len(blueprint.symbol_plans),
                elapsed_ms=elapsed_ms,
            )
            return blueprint
        except Exception as e:
            elapsed_ms = round((perf_counter() - started) * 1000, 2)
            logger.warning(
                "llm_adapter.agentic_failed_fallback_to_legacy",
                error=str(e),
                elapsed_ms=elapsed_ms,
            )
            # Fallback to legacy chunk pipeline
            return await self._generate_legacy(
                signal_features, current_positions, previous_execution,
            )

    # ------------------------------------------------------------------
    # Legacy chunk-split pipeline
    # ------------------------------------------------------------------

    async def _generate_legacy(
        self,
        signal_features: list[SignalFeatures],
        current_positions: dict | None = None,
        previous_execution: dict | None = None,
    ) -> LLMTradingBlueprint:
        """Legacy 分片模式：自动分片 → 并行 LLM 调用 → 合并结果。

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

        async def _process_chunk(idx: int, chunk: list[SignalFeatures]) -> LLMTradingBlueprint:
            async with self._semaphore:
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

        # Track symbols lost from failed chunks
        missing_symbols: list[str] = []
        if errors:
            all_chunks_symbols = {i: [sf.symbol for sf in chunk] for i, chunk in enumerate(chunks)}
            benchmark_set = {s.upper() for s in self._benchmark_symbols}
            for idx, _ in errors:
                for sym in all_chunks_symbols.get(idx, []):
                    if sym.upper() not in benchmark_set:
                        missing_symbols.append(sym)

        if not blueprints:
            # All chunks failed — re-raise the first error
            raise errors[0][1]

        merged = merge_blueprints(blueprints)
        if missing_symbols:
            merged.missing_symbols = missing_symbols
            logger.warning(
                "llm_adapter.missing_symbols",
                symbols=missing_symbols,
            )

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
