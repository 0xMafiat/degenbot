from abc import ABC, abstractmethod
from typing import Tuple

from brownie import Contract
from brownie.convert import to_address

from .abi import V3_LP_ABI
from .libraries import TickBitmap
from .tick_lens import TickLens


class BaseV3LiquidityPool(ABC):
    @abstractmethod
    def _derived():
        """
        An abstract method designed to ensure that all consumers of this API
        use a derived class instead of this base class. Calling BaseV3LiquidityPool()
        will raise a NotImplementedError exception.

        Consumers should use V3LiquidityPool() instead, or create their own derived
        class and define a `_derived` method within that class.
        """
        raise NotImplementedError

    def __init__(self, address: str, lens: Contract = None):
        self.address = to_address(address)

        try:
            self._brownie_contract = Contract(address=address)
        except:
            try:
                self._brownie_contract = Contract.from_explorer(
                    address=address, silent=True
                )
            except:
                try:
                    self._brownie_contract = Contract.from_abi(
                        name="", address=address, abi=V3_LP_ABI
                    )
                except:
                    raise

        if lens:
            self.lens = lens
        else:
            try:
                self.lens = TickLens()
            except:
                raise

        try:
            self.token0 = self._brownie_contract.token0()
            self.token1 = self._brownie_contract.token1()
            self.fee = self._brownie_contract.fee()
            self.slot0 = self._brownie_contract.slot0()
            self.liquidity = self._brownie_contract.liquidity()
            self.tick_spacing = self._brownie_contract.tickSpacing()
            self.sqrt_price_x96 = self.slot0[0]
            self.tick = self.slot0[1]
            self.factory = self._brownie_contract.factory()
            self.tick_data = {}
            self.tick_word, _ = self.get_tick_bitmap_position(self.tick)
            self.get_tick_data_at_word(self.tick_word)
        except:
            raise

    def get_tick_bitmap_position(self, tick) -> Tuple[int, int]:
        """
        Retrieves the wordPosition and bitPosition for the input tick

        This function corrects internally for tick spacing! e.g. tick=600 is the
        11th initialized tick for an LP with tickSpacing of 60, starting at 0.
        Each "word" in the tickBitmap holds 256 initialized positions, so the 11th
        position of the 1st word will represent tick=600.

        Calling `get_tick_bitmap_position(600)` returns (0,10), where:
            0 = wordPosition (zero-indexed)
            10 = bitPosition (zero-indexed)
        """
        return TickBitmap.position(tick // self.tick_spacing)

    def get_tick_data_at_word(self, word_position: int):
        """
        Gets the initialized tick values at a specific word (a 32 byte number
        representing 256 ticks at the tickSpacing interval), then stores
        the liquidity values in the `self.tick_data` dictionary using the tick
        as the key.
        """
        try:
            tick_data = self.lens._brownie_contract.getPopulatedTicksInWord(
                self.address, word_position
            )
        except:
            raise
        else:
            for (tick, liquidityNet, liquidityGross) in tick_data:
                self.tick_data[tick] = liquidityNet, liquidityGross
            return tick_data

    def auto_update(self):
        """
        Retrieves the current slot0 and liquidity values from the LP,
        stores any that have changed, and returns a tuple with an update status
        boolean and a dictionary holding the current mutable values:
            - liquidity
            - sqrt_price_x96
            - tick
        """
        updated = False
        try:
            if (slot0 := self._brownie_contract.slot0()) != self.slot0:
                updated = True
                self.slot0 = slot0
                self.sqrt_price_x96 = self.slot0[0]
                self.tick = self.slot0[1]
            if (
                liquidity := self._brownie_contract.liquidity()
            ) != self.liquidity:
                updated = True
                self.liquidity = liquidity
        except:
            raise
        else:
            return updated, {
                "liquidity": self.liquidity,
                "sqrt_price_x96": self.sqrt_price_x96,
                "tick": self.tick,
            }


class V3LiquidityPool(BaseV3LiquidityPool):
    def _derived():
        pass