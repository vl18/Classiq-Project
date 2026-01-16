from worker.worker.executor import run_qasm3_counts


QASM3_BELL = """OPENQASM 3;
include "stdgates.inc";
qubit[2] q;
bit[2] c;
h q[0];
cx q[0], q[1];
c[0] = measure q[0];
c[1] = measure q[1];
"""


def test_run_qasm3_counts_ok():
    counts, metrics = run_qasm3_counts(QASM3_BELL, shots=128)

    assert sum(counts.values()) == 128
    assert metrics["shots"] == 128
    assert "exec_ms" in metrics
