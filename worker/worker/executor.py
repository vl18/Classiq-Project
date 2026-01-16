from __future__ import annotations

from typing import Dict, Any, Tuple
import time

from qiskit import qasm3
from qiskit_aer import AerSimulator


def run_qasm3_counts(qasm3_str: str, shots: int) -> Tuple[Dict[str, int], Dict[str, Any]]:
    """Run QASM3 circuit locally with AerSimulator.

    Returns (counts, metrics).
    """
    t0 = time.perf_counter()
    circuit = qasm3.loads(qasm3_str)
    build_ms = int((time.perf_counter() - t0) * 1000)

    sim = AerSimulator()
    t1 = time.perf_counter()
    job = sim.run(circuit, shots=shots)
    result = job.result()
    counts = result.get_counts()
    exec_ms = int((time.perf_counter() - t1) * 1000)

    # Ensure JSON-serializable dict with string keys and int values
    counts_out = {str(k): int(v) for k, v in counts.items()}

    metrics = {
        "build_ms": build_ms,
        "exec_ms": exec_ms,
        "shots": shots,
        "num_qubits": getattr(circuit, "num_qubits", None),
        "num_clbits": getattr(circuit, "num_clbits", None),
        "depth": circuit.depth() if hasattr(circuit, "depth") else None,
        "size": circuit.size() if hasattr(circuit, "size") else None,
    }
    metrics = {k: v for k, v in metrics.items() if v is not None}
    return counts_out, metrics
