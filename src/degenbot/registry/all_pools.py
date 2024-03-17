from typing import Dict

from eth_typing import ChecksumAddress
from eth_utils.address import to_checksum_address

from ..baseclasses import BaseLiquidityPool
from ..logging import logger

# Internal state dictionary that maintains a keyed dictionary of all
# pool helper objects. The top level dict is keyed by chain ID, and
# sub-dicts are keyed by the checksummed pool address.
_all_pools: Dict[
    int,
    Dict[ChecksumAddress, BaseLiquidityPool],
] = {}


class AllPools:
    def __init__(self, chain_id: int) -> None:
        try:
            _all_pools[chain_id]
        except KeyError:
            _all_pools[chain_id] = {}
        finally:
            self.pools = _all_pools[chain_id]

    def __delitem__(self, pool: BaseLiquidityPool | str) -> None:
        if isinstance(pool, BaseLiquidityPool):
            _pool_address = pool.address
        else:
            _pool_address = to_checksum_address(pool)
        del self.pools[_pool_address]

    def __getitem__(self, pool_address: str) -> BaseLiquidityPool:
        return self.pools[to_checksum_address(pool_address)]

    def __setitem__(self, pool_address: str, pool_helper: BaseLiquidityPool) -> None:
        _pool_address = to_checksum_address(pool_address)
        if self.pools.get(_pool_address):
            logger.warning(
                f"A pool helper with address {_pool_address} already exists! It has been overwritten."
            )
        self.pools[_pool_address] = pool_helper

    def __len__(self) -> int:
        return len(self.pools)

    def get(self, pool_address: str) -> BaseLiquidityPool | None:
        return self.pools.get(to_checksum_address(pool_address))
