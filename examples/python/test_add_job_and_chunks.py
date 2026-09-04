import tempfile
import unittest
from pathlib import Path

import yaml

from add_job_and_chunks import load_benchmark_jobs


class BenchmarkJobsTest(unittest.TestCase):
    def test_loads_enabled_batch_shard_count(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            jobs = root / "jobs"
            jobs.mkdir()
            (root / "benchmark.yaml").write_text(
                yaml.safe_dump({"benchmark": {"id": "sample", "jobs": "jobs"}})
            )
            (jobs / "batch.yaml").write_text(
                yaml.safe_dump(
                    {
                        "job_id": "batch-job",
                        "schedule": {"enabled": True, "kind": "batch"},
                        "workload": {
                            "dataset": {"partitioning": {"shard_count": 4}}
                        },
                    }
                )
            )

            self.assertEqual(load_benchmark_jobs(root), [("00000000005BVEJSREANFBXD52", 4)])


if __name__ == "__main__":
    unittest.main()
