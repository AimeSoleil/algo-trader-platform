from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.data_service.app.tasks import coordination


class _FakeSignature(dict):
    def set(self, **options):
        updated = _FakeSignature(self)
        updated["options"] = {**self.get("options", {}), **options}
        return updated


class _FakeStageBarrier:
    def s(self, stage_name: str, trading_date: str):
        return _FakeSignature(
            kind="signature",
            task="data_service.tasks.stage_barrier",
            args=[stage_name, trading_date],
            options={},
        )


def _fake_signature(name: str, args=None, queue: str | None = None, immutable: bool = False):
    return _FakeSignature(
        kind="signature",
        task=name,
        args=list(args or []),
        options={"queue": queue, "immutable": immutable},
    )


def _fake_group(tasks):
    return {"kind": "group", "tasks": list(tasks)}


def _fake_chord(header, body):
    return {"kind": "chord", "header": header, "body": body}


def _fake_chain(*steps):
    return _FakeSignature(kind="chain", steps=list(steps), options={})


def test_build_downstream_steps_fans_out_analysis_by_daily_chunk_size(monkeypatch):
    all_symbols = [f"SYM{i:02d}" for i in range(12)]
    tradable_symbols = [f"SYM{i:02d}" for i in range(45)]

    settings = SimpleNamespace(
        common=SimpleNamespace(
            watchlist=SimpleNamespace(
                all=all_symbols,
                for_data_signal=tradable_symbols,
            )
        ),
        data_service=SimpleNamespace(
            worker=SimpleNamespace(
                pipeline=SimpleNamespace(chunk_size=5)
            )
        ),
        analysis_service=SimpleNamespace(
            daily_task_chunk_size=20,
        ),
    )

    monkeypatch.setattr(coordination, "get_settings", lambda: settings)
    monkeypatch.setattr(coordination.celery_app, "signature", _fake_signature)
    monkeypatch.setattr(coordination, "group", _fake_group)
    monkeypatch.setattr(coordination, "chord", _fake_chord)
    monkeypatch.setattr(coordination, "celery_chain", _fake_chain)
    monkeypatch.setattr(coordination, "stage_barrier", _FakeStageBarrier())

    steps = coordination._build_downstream_steps("2026-05-08")

    assert [name for name, _ in steps] == ["compute_daily_signals", "generate_daily_blueprint"]

    compute_stage = steps[0][1]
    assert compute_stage["kind"] == "chord"
    assert len(compute_stage["header"]["tasks"]) == 3
    assert compute_stage["header"]["tasks"][0]["task"] == "signal_service.tasks.compute_signals_chunk"
    assert compute_stage["header"]["tasks"][0]["args"] == [all_symbols[:5], "2026-05-08"]
    assert compute_stage["header"]["tasks"][2]["args"] == [all_symbols[10:], "2026-05-08"]

    analysis_stage = steps[1][1]
    assert analysis_stage["kind"] == "chain"
    assert analysis_stage["options"]["immutable"] is True

    analysis_fanout = analysis_stage["steps"][0]
    assert analysis_fanout["kind"] == "chord"
    analysis_chunk_tasks = analysis_fanout["header"]["tasks"]
    assert len(analysis_chunk_tasks) == 3
    assert analysis_chunk_tasks[0]["task"] == "analysis_service.tasks.generate_daily_blueprint_chunk"
    assert analysis_chunk_tasks[0]["args"] == [tradable_symbols[:20], "2026-05-08"]
    assert analysis_chunk_tasks[1]["args"] == [tradable_symbols[20:40], "2026-05-08"]
    assert analysis_chunk_tasks[2]["args"] == [tradable_symbols[40:], "2026-05-08"]

    finalize_task = analysis_fanout["body"]
    assert finalize_task["task"] == "analysis_service.tasks.finalize_daily_blueprint_chunks"
    assert finalize_task["args"] == ["2026-05-08"]
    assert finalize_task["options"]["queue"] == "analysis"

    barrier_task = analysis_stage["steps"][1]
    assert barrier_task["task"] == "data_service.tasks.stage_barrier"
    assert barrier_task["args"] == ["generate_daily_blueprint", "2026-05-08"]
    assert barrier_task["options"]["queue"] == "data"