import grpc
import ulid
from tandemn.chunkmanager.v1 import chunk_manager_pb2
from tandemn.chunkmanager.v1 import chunk_manager_pb2_grpc


RPC_TIMEOUT_SECONDS = 5

CHUNK_MANAGER_ADDR = "127.0.0.1:60002"

def main() -> None:
    add_chain(1, 1, 2)


def drain_chain(job_id, rank_id, chain_id):
    job_id = str(ulid.from_int(job_id))
    rank_id = str(ulid.from_int(rank_id))
    print(f"Job id: {job_id}, rank-id: {rank_id}, chain-id: {chain_id}")

    with grpc.insecure_channel(CHUNK_MANAGER_ADDR) as channel:
        planner = chunk_manager_pb2_grpc.PlannerServiceStub(channel)

        chain = chunk_manager_pb2.ChainIdentity(
            job_id=job_id,
            rank_id=rank_id,
            chain_id=chain_id,
        )
        drained = planner.DrainChainAssociation(
            chunk_manager_pb2.DrainChainAssociationRequest(chain=chain),
            timeout=RPC_TIMEOUT_SECONDS,
        )
        print(drained)
        assert drained.association.state == chunk_manager_pb2.CHAIN_STATE_DRAINING


def add_chain(job_id, rank_id, chain_id):
    job_id = str(ulid.from_int(job_id))
    rank_id = str(ulid.from_int(rank_id))
    print(f"job-id: {job_id}, rank-id: {rank_id}, chain-id: {chain_id}")

    with grpc.insecure_channel(CHUNK_MANAGER_ADDR) as channel:
        planner = chunk_manager_pb2_grpc.PlannerServiceStub(channel)

        chain = chunk_manager_pb2.ChainIdentity(
            job_id=job_id,
            rank_id=rank_id,
            chain_id=chain_id,
        )
        added = planner.AddChainAssociation(
            chunk_manager_pb2.AddChainAssociationRequest(chain=chain),
            timeout=RPC_TIMEOUT_SECONDS,
        )
        print(added)
        assert added.association.state == chunk_manager_pb2.CHAIN_STATE_ACTIVE


if __name__ == "__main__":
    main()
