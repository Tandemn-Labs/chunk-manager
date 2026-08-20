import argparse

import grpc
import ulid
from google.protobuf import duration_pb2
from tandemn.chunkmanager.v1 import chunk_manager_pb2
from tandemn.chunkmanager.v1 import chunk_manager_pb2_grpc


RPC_TIMEOUT_SECONDS = 5


def run(target: str) -> None:
    job_id = str(ulid.new())
    chain = chunk_manager_pb2.ChainIdentity(
        job_id=job_id,
        rank_id=str(ulid.new()),
        chain_id=0,
    )

    with grpc.insecure_channel(target) as channel:
        planner = chunk_manager_pb2_grpc.PlannerServiceStub(channel)
        worker = chunk_manager_pb2_grpc.WorkerServiceStub(channel)

        created = planner.CreateJob(
            chunk_manager_pb2.CreateJobRequest(
                job_id=job_id,
                total_chunk_count=1,
                max_retries=1,
                retry_backoff=duration_pb2.Duration(seconds=1),
                lease_duration=duration_pb2.Duration(seconds=30),
            ),
            timeout=RPC_TIMEOUT_SECONDS,
        )
        assert created.job.state == chunk_manager_pb2.JOB_STATE_PENDING

        registered = planner.RegisterChunks(
            chunk_manager_pb2.RegisterChunksRequest(
                job_id=job_id,
                chunks=[
                    chunk_manager_pb2.ChunkRegistration(
                        chunk_id=0,
                        input_ref="s3://example-input/chunk-0",
                    )
                ],
            ),
            timeout=RPC_TIMEOUT_SECONDS,
        )
        assert registered.registered_count == 1

        planner.AddChainAssociation(
            chunk_manager_pb2.AddChainAssociationRequest(chain=chain),
            timeout=RPC_TIMEOUT_SECONDS,
        )

        finalized = planner.FinalizeJobRegistration(
            chunk_manager_pb2.FinalizeJobRegistrationRequest(job_id=job_id),
            timeout=RPC_TIMEOUT_SECONDS,
        )
        assert finalized.job.state == chunk_manager_pb2.JOB_STATE_RUNNING

        claimed = worker.ClaimChunks(
            chunk_manager_pb2.ClaimChunksRequest(chain=chain, max_chunks=1),
            timeout=RPC_TIMEOUT_SECONDS,
        )
        assert claimed.job_state == chunk_manager_pb2.JOB_STATE_RUNNING
        assert len(claimed.leases) == 1

        lease = chunk_manager_pb2.LeaseReference(
            chunk_id=claimed.leases[0].chunk_id,
            generation=claimed.leases[0].generation,
        )
        renewed = worker.RenewLeases(
            chunk_manager_pb2.RenewLeasesRequest(chain=chain, leases=[lease]),
            timeout=RPC_TIMEOUT_SECONDS,
        )
        assert len(renewed.renewed) == 1
        assert not renewed.stale

        completed = worker.CompleteChunk(
            chunk_manager_pb2.CompleteChunkRequest(
                chain=chain,
                lease=lease,
                output_uri="s3://example-output/chunk-0",
                checksum="sha256:example",
                output_size_bytes=1,
            ),
            timeout=RPC_TIMEOUT_SECONDS,
        )
        assert completed.job_state == chunk_manager_pb2.JOB_STATE_SUCCEEDED

        terminal = worker.ClaimChunks(
            chunk_manager_pb2.ClaimChunksRequest(chain=chain, max_chunks=1),
            timeout=RPC_TIMEOUT_SECONDS,
        )
        assert terminal.job_state == chunk_manager_pb2.JOB_STATE_SUCCEEDED
        assert not terminal.leases

    print(f"job {job_id} succeeded")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", default="127.0.0.1:9090")
    args = parser.parse_args()
    run(args.target)


if __name__ == "__main__":
    main()
