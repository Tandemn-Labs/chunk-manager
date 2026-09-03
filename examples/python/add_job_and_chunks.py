import argparse

import grpc
from google.protobuf import duration_pb2
from tandemn.chunkmanager.v1 import chunk_manager_pb2
from tandemn.chunkmanager.v1 import chunk_manager_pb2_grpc


RPC_TIMEOUT_SECONDS = 5

# Use:
# python add_job_and_chunks.py \
# --target host:port \
# <job-id-1>:50 \
# <job-id-2>:10

def parse_job_spec(value: str) -> tuple[str, int]:
    job_id, separator, chunk_count_text = value.rpartition(":")
    if not separator or not job_id or not chunk_count_text:
        raise argparse.ArgumentTypeError(
            "job specification must be in the form JOB_ID:CHUNK_COUNT"
        )

    try:
        chunk_count = int(chunk_count_text)
    except ValueError as error:
        raise argparse.ArgumentTypeError("chunk count must be an integer") from error

    if chunk_count <= 0:
        raise argparse.ArgumentTypeError("chunk count must be positive")

    return job_id, chunk_count


def register_job(
    planner: chunk_manager_pb2_grpc.PlannerServiceStub,
    job_id: str,
    chunk_count: int,
) -> None:
    created = planner.CreateJob(
        chunk_manager_pb2.CreateJobRequest(
            job_id=job_id,
            total_chunk_count=chunk_count,
            max_retries=2,
            retry_backoff=duration_pb2.Duration(seconds=1),
            lease_duration=duration_pb2.Duration(seconds=60),
        ),
        timeout=RPC_TIMEOUT_SECONDS,
    )
    assert created.job.state == chunk_manager_pb2.JOB_STATE_PENDING

    registered = planner.RegisterChunks(
        chunk_manager_pb2.RegisterChunksRequest(
            job_id=job_id,
            chunks=[
                chunk_manager_pb2.ChunkRegistration(
                    chunk_id=chunk_id,
                    input_ref=f"s3://batched-chunks/{job_id}/input/{chunk_id}.jsonl",
                )
                for chunk_id in range(chunk_count)
            ],
        ),
        timeout=RPC_TIMEOUT_SECONDS,
    )
    assert registered.registered_count == chunk_count

    finalized = planner.FinalizeJobRegistration(
        chunk_manager_pb2.FinalizeJobRegistrationRequest(job_id=job_id),
        timeout=RPC_TIMEOUT_SECONDS,
    )
    assert finalized.job.state == chunk_manager_pb2.JOB_STATE_RUNNING

    print(f"Registered {chunk_count} chunks for job {job_id}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create jobs and register their chunks without creating chains."
    )
    parser.add_argument(
        "job_specs",
        metavar="JOB_ID:CHUNK_COUNT",
        nargs="+",
        type=parse_job_spec,
        help="one or more job ID and chunk count pairs",
    )
    parser.add_argument("--target", required=True)
    args = parser.parse_args()

    with grpc.insecure_channel(args.target) as channel:
        planner = chunk_manager_pb2_grpc.PlannerServiceStub(channel)
        for job_id, chunk_count in args.job_specs:
            register_job(planner, job_id, chunk_count)


if __name__ == "__main__":
    main()
