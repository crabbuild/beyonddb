"""Persisted benchmark topology; the server supports 2..256 initial ranges."""
from dataclasses import asdict, dataclass
import json
from pathlib import Path


@dataclass(frozen=True)
class Topology:
    nodes: int = 3
    cells_per_node: int = 10

    def __post_init__(self):
        if (type(self.nodes) is not int or type(self.cells_per_node) is not int
                or self.nodes < 1 or self.cells_per_node < 1
                or not 2 <= self.cells <= 256):
            raise ValueError("nodes and cells-per-node must be positive integers with 2..256 total cells")

    @property
    def cells(self):
        return self.nodes * self.cells_per_node

    @property
    def indices(self):
        return range(self.nodes)

    @property
    def node_names(self):
        return [f"perf30-node{i+1}" for i in self.indices]

    @property
    def endpoints(self):
        return [f"http://127.0.0.1:{18001+i}" for i in self.indices]

    @property
    def containers(self):
        return [f"beyonddb-{name}" for name in self.node_names]

    def peer_port(self, index):
        # The fixture's object store already owns port 19100.
        return 19001 + index + (index >= 99)

    def owner(self, ordinal):
        if not 0 <= ordinal < self.cells:
            raise ValueError("range ordinal outside topology")
        return ordinal // self.cells_per_node

    def partner(self, ordinal):
        # A full node's range block crosses owners; one-node fixtures cross cells.
        self.owner(ordinal)
        step = self.cells_per_node if self.nodes > 1 else 1
        return (ordinal + step) % self.cells

    def prefix(self, ordinal):
        self.owner(ordinal)
        lower, upper = ordinal * 256 // self.cells, (ordinal + 1) * 256 // self.cells
        return chr(min(lower + 1, upper - 1))

    def to_dict(self):
        return asdict(self)

    @classmethod
    def load(cls, root):
        path = Path(root) / "fixture.json"
        # Retained pre-configurable fixtures were always three nodes / 30 cells.
        return cls(**json.loads(path.read_text())) if path.exists() else cls()

    @classmethod
    def from_result(cls, result):
        return cls(**result.get("topology", {}))


def covers_all(case):
    """Read new results and the original 30-cell result schema."""
    return case.get("covers_all_cells", case.get("covers_all_30_cells", False))
