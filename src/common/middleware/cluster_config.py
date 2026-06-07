from dataclasses import dataclass

@dataclass
class ClusterConfig:
    cluster_name: str
    node_id: int
    cluster_size: int
