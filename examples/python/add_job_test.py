import argparse

import grpc
import ulid
from google.protobuf import duration_pb2
from tandemn.chunkmanager.v1 import chunk_manager_pb2
from tandemn.chunkmanager.v1 import chunk_manager_pb2_grpc


RPC_TIMEOUT_SECONDS = 5

CHUNK_MANAGER_ADDR=""

def main() -> None:
    job_id = str(ulid.from_int(1))
    print(f"Job id: {job_id}")

    with grpc.insecure_channel(CHUNK_MANAGER_ADDR) as channel:
        planner = chunk_manager_pb2_grpc.PlannerServiceStub(channel)
        worker = chunk_manager_pb2_grpc.WorkerServiceStub(channel)

        ########### Create / Register job ###########

        created = planner.CreateJob(
            chunk_manager_pb2.CreateJobRequest(
                job_id=job_id,
                total_chunk_count=50,
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
                    for chunk_id in range(50)
                ],
            ),
            timeout=RPC_TIMEOUT_SECONDS,
        )
        assert registered.registered_count == 50

        finalized = planner.FinalizeJobRegistration(
            chunk_manager_pb2.FinalizeJobRegistrationRequest(job_id=job_id),
            timeout=RPC_TIMEOUT_SECONDS,
        )
        assert finalized.job.state == chunk_manager_pb2.JOB_STATE_RUNNING


        ########### Add chain ###########

        chain1 = chunk_manager_pb2.ChainIdentity(
            job_id=job_id,
            rank_id=str(ulid.from_int(1)),
            chain_id=1,
        )
        planner.AddChainAssociation(
            chunk_manager_pb2.AddChainAssociationRequest(chain=chain1),
            timeout=RPC_TIMEOUT_SECONDS,
        )

        chain2 = chunk_manager_pb2.ChainIdentity(
            job_id=job_id,
            rank_id=str(ulid.from_int(1)),
            chain_id=2,
        )
        planner.AddChainAssociation(
            chunk_manager_pb2.AddChainAssociationRequest(chain=chain2),
            timeout=RPC_TIMEOUT_SECONDS,
        )


if __name__ == "__main__":
    main()
