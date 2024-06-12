from typing import Any, Dict

from eth_typing import ChecksumAddress
from eth_utils.address import to_checksum_address


FACTORY_ADDRESSES: Dict[
    int,  # Chain ID
    Dict[
        ChecksumAddress,  # Factory address
        Dict[str, Any],
    ],
] = {
    1: {
        # Uniswap (V2)
        to_checksum_address("0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f"): {
            "init_hash": "0x96e8ac4277198ff8b6f785478aa9a39f403cb768dd02cbee326c3e7da348845f"
        },
        # Uniswap (V3)
        to_checksum_address("0x1F98431c8aD98523631AE4a59f267346ea31F984"): {
            "init_hash": "0xe34f199b19b2b4f47f68442619d555527d244f78a3297ea89325f843f87b8b54"
        },
        # Sushiswap (V2)
        to_checksum_address("0xC0AEe478e3658e2610c5F7A4A2E1777cE9e4f2Ac"): {
            "init_hash": "0xe18a34eb0e04b04f7a0ac29a6e80748dca96319b42c54d679cb821dca90c6303"
        },
        # Sushiswap (V3)
        to_checksum_address("0xbACEB8eC6b9355Dfc0269C18bac9d6E2Bdc29C4F"): {
            "init_hash": "0xe34f199b19b2b4f47f68442619d555527d244f78a3297ea89325f843f87b8b54"
        },
    },
    42161: {
        # Uniswap (V3)
        to_checksum_address("0x1F98431c8aD98523631AE4a59f267346ea31F984"): {
            "init_hash": "0xe34f199b19b2b4f47f68442619d555527d244f78a3297ea89325f843f87b8b54"
        },
        # Sushiswap (V2)
        to_checksum_address("0xc35DADB65012eC5796536bD9864eD8773aBc74C4"): {
            "init_hash": "0xe18a34eb0e04b04f7a0ac29a6e80748dca96319b42c54d679cb821dca90c6303"
        },
        # Sushiswap (V3)
        to_checksum_address("0x1af415a1EbA07a4986a52B6f2e7dE7003D82231e"): {
            "init_hash": "0xe34f199b19b2b4f47f68442619d555527d244f78a3297ea89325f843f87b8b54"
        },
    },
    56: {
        # PancakeSwap: Router (V2)
        to_checksum_address("0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73"): {
            "init_hash": "0x00fb7f630766e6a796048ea87d01acd3068e8ff67d078148a3fa3f4a84f69bd5"
        },
        # PancakeSwap: Router (V3)
        to_checksum_address("0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865"): {
            "init_hash": None
        },

    },
}


TICKLENS_ADDRESSES: Dict[
    int,  # Chain ID
    Dict[
        ChecksumAddress,  # Factory address
        ChecksumAddress,  # TickLens address
    ],
] = {
    # Ethereum Mainnet
    1: {
        # Uniswap V3
        # ref: https://docs.uniswap.org/contracts/v3/reference/deployments
        to_checksum_address("0x1F98431c8aD98523631AE4a59f267346ea31F984"): to_checksum_address(
            "0xbfd8137f7d1516D3ea5cA83523914859ec47F573"
        ),
        # Sushiswap V3
        # ref: https://docs.sushi.com/docs/Products/V3%20AMM/Periphery/Deployment%20Addresses
        to_checksum_address("0xbACEB8eC6b9355Dfc0269C18bac9d6E2Bdc29C4F"): to_checksum_address(
            "0xFB70AD5a200d784E7901230E6875d91d5Fa6B68c"
        ),
    },
    # Arbitrum
    42161: {
        # Uniswap V3
        # ref: https://docs.uniswap.org/contracts/v3/reference/deployments
        to_checksum_address("0x1F98431c8aD98523631AE4a59f267346ea31F984"): to_checksum_address(
            "0xbfd8137f7d1516D3ea5cA83523914859ec47F573"
        ),
        # Sushiswap V3
        # ref: https://docs.sushi.com/docs/Products/V3%20AMM/Periphery/Deployment%20Addresses
        to_checksum_address("0x1af415a1EbA07a4986a52B6f2e7dE7003D82231e"): to_checksum_address(
            "0x8516944E89f296eb6473d79aED1Ba12088016c9e"
        ),
    },
    # BNB Chain
    56: {
        # Pancakeswap V3
        # ref: https://docs.uniswap.org/contracts/v3/reference/deployments
        to_checksum_address("0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865"): to_checksum_address(
            "0x9a489505a00cE272eAa5e07Dba6491314CaE3796"
        ),
        # Uniswap V3
        # ref: https://docs.uniswap.org/contracts/v3/reference/deployments
        to_checksum_address("0xdB1d10011AD0Ff90774D0C6Bb92e5C5c8b4461F7"): to_checksum_address(
            "0xD9270014D396281579760619CCf4c3af0501A47C"
        ),
    },
    # Base Chain
    8453: {
        # Uniswap V3
        # ref: https://docs.uniswap.org/contracts/v3/reference/deployments
        to_checksum_address("0x33128a8fC17869897dcE68Ed026d694621f6FDfD"): to_checksum_address(
            "0x0CdeE061c75D43c82520eD998C23ac2991c9ac6d"
        ),
    },
}
