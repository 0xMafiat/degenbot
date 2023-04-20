import asyncio
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from collections import deque
from typing import List, Tuple
import datetime

import brownie
import eth_abi
import eth_account
# import flashbots
import web3
import websockets


import degenbot as bot

BROWNIE_NETWORK = "ftm-main-local" #TODO: change for node
BROWNIE_ACCOUNT = "degenbot"
#FLASHBOTS_IDENTITY_ACCOUNT = "flashbots_id"

MULTICALL_FLUSH_INTERVAL = 1000

#FLASHBOTS_RELAY_URL = "https://relay.flashbots.net"

WEBSOCKET_URI = "ws://localhost:18546" #TODO: change for node

FTMSCAN_API_KEY = "X4BJNCSHP8DR35XR1NTX6TPQ1ZJ7GPS3M1"

ARB_CONTRACT_ADDRESS = "0xdf373B80733059068aCd5229f92f1513a764ADA2" #TODO
WFTM_ADDRESS = "0x21be370D5312f44cB42ce377BC9b8a0cEF1A4C83"
MULTICALL_ADDRESS = "0xb828C456600857abd4ed6C32FAcc607bD0464F4F"

MIN_PROFIT_FTM = int(0.0 * 10**18)

MINER_TIP = 0.8  # % of profit to bribe the miner (via relay) #TODO change

DRY_RUN = False

RELAY_RETRIES = 3

VERBOSE_ACTIVATION = False
VERBOSE_BLOCKS = False
VERBOSE_EVENTS = True
VERBOSE_PROCESSING = False
VERBOSE_RELAY_SIMULATION = True
VERBOSE_TIMING = False
VERBOSE_UPDATES = False
VERBOSE_WATCHDOG = True
VERBOSE_UPDATE_POOL = True

ARB_ONCHAIN_ENABLE = True

# require min. number of simulations before evaluating the cutoff threshold
SIMULATION_CUTOFF_MIN_ATTEMPTS = 10
# arbs that fail simulations greater than this percentage will be added to a blacklist
SIMULATION_CUTOFF_FAIL_THRESHOLD = 0.8



async def _main_async():

    with ThreadPoolExecutor() as executor:
        asyncio.get_running_loop().set_default_executor(executor)
        tasks = [
            asyncio.create_task(coro)
            for coro in [
                load_arbs(),
                activate_arbs(),
                refresh_pools(),
                watch_events(),
                watch_new_blocks(),
                status_watchdog(),
                track_balance(),
                average_block_time()
            ]
        ]
        await asyncio.sleep(0.2)
        try:
            await asyncio.gather(*tasks)
        except Exception as e:
            print(f"main: {e}")
            print(type(e))


# scan through the json files, build arb_helpers (load arb_paths, tokens, lp's, blacklisted tokens)
async def load_arbs():

    print("Starting arb loading function")

    global degenbot_lp_helpers
    global degenbot_cycle_arb_helpers
    global arb_simulations

    # liquidity_pool_and_token_addresses will filter out any blacklisted addresses, so helpers should draw from this as the "official" source of truth
    liquidity_pool_data = {}
    for filename in [
        "spookyswap_lps.json",
        # "equalizer_lps.json",
        "wigoswap_lps.json",
        "knightswap_lps.json"
    ]:
        with open(filename) as file:
            for pool in json.load(file):
                pool_address = pool.get("pool_address")
                token0_address = pool.get("token0")
                token1_address = pool.get("token1")
                if (
                    token0_address in BLACKLISTED_TOKENS
                    or token1_address in BLACKLISTED_TOKENS
                ):
                    continue
                else:
                    liquidity_pool_data[pool_address] = pool
    print(f"Found {len(liquidity_pool_data)} pools")


    arb_paths = []
    for filename in [
        "ftm_arbs_2pool_wo_equalizer.json",
        "ftm_arbs_triangle_wo_equalizer.json"
        #"new_ftm_arbs_top_liq_triangle_wo_eq.json",
        #"ftm_arbs_four_path_top_liq_wo_eq.json"
    ]:
        with open(filename) as file:
            for arb_id, arb in json.load(file).items():
                passed_checks = True
                if arb_id in BLACKLISTED_ARBS:
                    passed_checks = False
                for pool_address in arb.get("path"):
                    if not liquidity_pool_data.get(pool_address):
                        passed_checks = False
                if passed_checks:
                    arb_paths.append(arb)
    print(f"Found {len(arb_paths)} arb paths")

    # Identify all unique pool addresses in arb paths
    unique_pool_addresses = {
        pool_address
        for arb in arb_paths
        for pool_address in arb.get("path")
        if liquidity_pool_data.get(pool_address)
    }
    print(f"Found {len(unique_pool_addresses)} unique pools")

    # Identify all unique token addresses, checking if the pool is present in pools_and_tokens (pre-checked against the blacklist)
    unique_tokens = (
        # Token0 addresses
        {
            token_address
            for arb in arb_paths
            for pool_address in arb.get("path")
            for pool_dict in arb.get("pools").values()
            if (token_address := pool_dict.get("token0"))
            if liquidity_pool_data.get(pool_address)
        }
        |
        # token1 addresses
        {
            token_address
            for arb in arb_paths
            for pool_address in arb.get("path")
            for pool_dict in arb.get("pools").values()
            if (token_address := pool_dict.get("token1"))
            if pool_address in liquidity_pool_data.get(pool_address)
        }
    )
    print(f"Found {len(unique_tokens)} unique tokens")

    # build a dict of Erc20Token helper objects, keyed by address
    degenbot_token_helpers = {}

    event_loop = asyncio.get_running_loop()

    start = time.time()

    for token_address in unique_tokens:
        await asyncio.sleep(0)
        try:
            token_helper = await event_loop.run_in_executor(
                None,
                partial(
                    bot.Erc20Token,
                    address=token_address,
                    silent=True,
                    unload_brownie_contract_after_init=True
                )
            )
        except ValueError:
            BLACKLISTED_TOKENS.append(token_address)
        except Exception as e:
            print(e)
            print(type(e))
        else:
            if VERBOSE_PROCESSING:
                print(f"Created token helper: {token_helper}")
            degenbot_token_helpers[token_helper.address] = token_helper
    print(
        f"Build {len(degenbot_token_helpers)} tokens in {time.time() - start:.2f}s"
    )

    with open("fantom_blacklisted_tokens.json","w") as file:
        json.dump(BLACKLISTED_TOKENS, file, indent=2)

    start = time.time()
    event_loop = asyncio.get_running_loop()

    #lens = (
    #    bot.uniswap.v3.TickLens()
    #)
    for pool_address in unique_pool_addresses:

        if not (
            token0_obj := degenbot_token_helpers.get(
                liquidity_pool_data.get(pool_address).get("token0")
            )
        ) or not (
            token1_obj := degenbot_token_helpers.get(
                liquidity_pool_data.get(pool_address).get("token1")
            )
        ):
            continue

        pool_type = liquidity_pool_data[pool_address]["type"]

        try:
            if pool_type == "UniswapV2":
                pool_helper = await  event_loop.run_in_executor(
                    None,
                    partial(
                        bot.LiquidityPool,
                        address=pool_address,
                        tokens=[token0_obj,token1_obj],
                        update_method="external",
                        silent=True,
                        abi=bot.UNISWAPV2_LP_ABI
                    )
                )

            elif pool_type == "UniswapV3":
                pool_helper = await event_loop.run_in_executor(
                    None,
                    partial(
                        bot.V3LiquidityPool(
                            address=pool_address,
                            tokens=[token0_obj, token1_obj],
                            update_method="external",
                            abi=bot.uniswap.v3.abi.UNISWAP_V3_POOL_ABI,
                            #lens=lens
                        )
                    )
                )
            else:
                raise Exception("Could not identify pool type")
        except Exception as e:
            print(e)
            print(type(e))
        else:
            if VERBOSE_PROCESSING:
                print(f"Created pool helper: {pool_helper}")
            degenbot_lp_helpers[pool_helper.address] = pool_helper

    print(
        f"Built {len(degenbot_lp_helpers)} liquidity pool helpers in {time.time() - start:.2f}s"
    )

    # TODO: change for other networks
    _wftm_balance = wftm.balanceOf(arb_contract.address)

    def create_uniswap_lp_cycle(arb_id, arb):
        try:
            swap_pools = [pool_obj for pool_address in arb.get("path") if (pool_obj := degenbot_lp_helpers.get(pool_address))]
            if len(swap_pools) == len(arb.get("path")):
                return bot.UniswapLpCycle(
                    input_token=degenbot_token_helpers.get(WFTM_ADDRESS),
                    swap_pools=swap_pools,
                    max_input=_wftm_balance,
                    id=arb_id
                )
        except bot.exceptions.ArbitrageError:
            BLACKLISTED_ARBS.append(arb_id)
            return None

    try:
        degenbot_cycle_arb_helpers = {
            arb_id : uniswap_lp_cycle
            for arb in arb_paths
            if (arb_id := arb.get("id")) not in BLACKLISTED_ARBS
            if (uniswap_lp_cycle := create_uniswap_lp_cycle(arb_id, arb))
        }
        print(f"Built {len(degenbot_cycle_arb_helpers)} cycle arb helpers")

        arb_simulations = {
            id: {
                "simulations": 0,
                "failures": 0,
            }
            for id in degenbot_cycle_arb_helpers.keys()
        }
    except Exception as e:
        degenbot_cycle_arb_helpers.pop(arb_id)
        BLACKLISTED_ARBS.append(arb_id)
        with open("fantom_blacklisted_arbs.json", "w") as file:
            json.dump(BLACKLISTED_ARBS, file, indent=2)


# Calculates the gas use for the specified arb against a particular block (has test_gas() function in it)
def test_onchain_arb_gas(
        arb_id,
        block_number,
):
    def test_gas(
            arb: bot.arbitrage.base.Arbitrage,
            payloads: dict,
            tx_params: dict,
            block_number,
            arb_id=None
    ) -> Tuple[bool, int]:

        if VERBOSE_TIMING:
            start = time.monotonic()
            print("starting test_gas")

        global arb_simulations

        try:
            arb_simulations[arb_id]["simulations"] += 1
            gas_estimate = (
                w3.eth.contract(
                    address=arb_contract.address,
                    abi=arb_contract.abi
                ).functions.execute_payloads(payloads)
                .estimate_gas(
                    tx_params,
                    block_identifier=block_number,
                )
            )
        except web3.exceptions.ContractLogicError as e:
            arb_simulations[arb_id]["failures"] += 1
            success = False
        except Exception as e:
            print(f"Error test_gas: {e}")
            print(f"Error type test_gas: {type(e)}")
            success = False
        else:
            success = True
            arb.gas_estimate = gas_estimate

        if VERBOSE_TIMING:
            print(f"test_gas completed in {time.monotonic() - start:0.4f}s")

        return (
            success,
            arb.gas_estimate if success else 0
        )

    if not (arb_helper := degenbot_cycle_arb_helpers.get(arb_id)):
        return

    if VERBOSE_TIMING:
        start = time.monotonic()
        print("starting test_onchain_arb")

    tx_params = {
        "from": bot_account.address,
        "chainId": brownie.chain.id,
        "nonce": bot_account.nonce,
    }

    try: 
        arb_payloads = arb_helper.generate_payloads(
            from_address=arb_contract.address,
        )


        success, gas_estimate = test_gas(
            arb=arb_helper,
            payloads=arb_payloads,
            tx_params=tx_params,
            block_number=block_number,
            arb_id=arb_helper.id
        )

        if success and VERBOSE_ACTIVATION:
            print(f"Gas estimate for arb {arb_helper.id}: {gas_estimate}")

        if VERBOSE_TIMING:
            print(f"test_onchain_arb completed in {time.monotonic() - start:0.4f}s")

        if not success:
            return
        else:
            arb_helper.gas_estimate = gas_estimate

    except Exception as e:
        print(f"BLACKLISTED_ARB: {arb_helper.id}")
        degenbot_cycle_arb_helpers.pop(arb_id)
        BLACKLISTED_ARBS.append(arb_helper.id)
        with open("fantom_blacklisted_arbs.json", "w") as file:
            json.dump(BLACKLISTED_TOKENS, file, indent=2)




# identify all possible arbs (regardless of profit) that have not been simulated for gas and simulate gas for them (uses test_onchain_arb_gas())
async def activate_arbs():

    print("Activating arbs")

    while True:
        await asyncio.sleep(AVERAGE_BLOCK_TIME)
        if status_paused:
            continue

        arbs_to_process = (
            arb_helper
            for arb_helper in degenbot_cycle_arb_helpers.copy().values()
            if not arb_helper.gas_estimate
        )

        while True:

            await asyncio.sleep(0)

            try:
                arb_helper = next(arbs_to_process)
                #print(f"process arb: {arb_helper}")
            except StopIteration:
                break
            except Exception as e:
                print(e)
                print(type(e))

            try:
                arb_helper.auto_update(block_number=newest_block)
                arb_helper.calculate_arbitrage()
            except bot.exceptions.ArbitrageError as e:
                continue
            except Exception as e:
                if VERBOSE_ACTIVATION:
                    print(f"estimate_arbs: {e}")
                    print(type(e))
                continue
            else:
                #print(f"testing onchain gas for {arb_helper} ({arb_helper.id})")
                test_onchain_arb_gas(
                    arb_id=arb_helper.id, block_number=newest_block,
                )


# finding all pools with the most recent update marked before the block where event-based updates began
async def refresh_pools():

    global status_pool_sync_in_progress
    (print("starting refresh_pools"))

    while True:
        # run once per block
        await asyncio.sleep(AVERAGE_BLOCK_TIME)

        if first_new_block and first_event_block:
            this_block = newest_block
        else:
            continue

        outdated_v2_pools = (
            pool_obj
            for pool_obj in degenbot_lp_helpers.values()
            if pool_obj.uniswap_version == 2
            if pool_obj.update_block < first_event_block
        )
        outdated_v3_pools = (
            pool_obj
            for pool_obj in degenbot_lp_helpers.values()
            if pool_obj.uniswap_version == 3
            if pool_obj.update_block < first_event_block
        )
        #if VERBOSE_UPDATE_POOL:
        #    count_v2 = 0
        #    for _ in outdated_v2_pools:
        #        count_v2 += 1
        #    count_v3 = 0
        #    for _ in outdated_v3_pools:
        #        count_v3 += 1
        #    if count_v2:
        #        print(f"found {count_v2} outdated v2 pools")
        #    if count_v3:
        #        print(f"found {count_v3} outdated v3 pools")

        #count_v2 = 0
        #count_v3 = 0

        for lp_helper in outdated_v2_pools:
            status_pool_sync_in_progress = True
            print(f"Refreshing outdated V2 pool: {lp_helper}")
            try:
                lp_helper.update_reserves(
                    override_update_method="polling",
                    update_block=this_block - 1,
                    silent=not VERBOSE_UPDATES
                )
            except Exception as e:
                print(f"(refresh_pools)-V2 {e}")

        for lp_helper in outdated_v3_pools:
            status_pool_sync_in_progress = True
            print(f"Refreshing outdated V3 pool: {lp_helper}")
            try:
                lp_helper.auto_update(block_number=this_block - 1)
            except Exception as e:
                print(f"(refresh_pools)-V3 {e}")

        status_pool_sync_in_progress = False


# watches the mint, burn, swap, sync events (mint, burn and swap for v3, sync for v2)
async def watch_events():
    global status_events
    global first_event_block

    status_events = False

    arbs_to_check = deque()

    received_events = 0
    processed_syncs = 0
    processed_mints = 0
    processed_burns = 0
    processed_swaps = 0

    _TIMEOUT = 0.5  # how many seconds to wait before assuming the last event was received

    print("Starting event watcher loop")

    def process_sync_event(message: dict):

        event_address = w3.toChecksumAddress(
            message.get("params").get("result").get("address")
        )

        event_block = int(
            message.get("params").get("result").get("blockNumber"),
            16
        )

        event_data = message.get("params").get("result").get("data")

        event_reserves = eth_abi.decode(
            ["uint112", "uint112"],
            bytes.fromhex(event_data[2:]),
        )

        try:
            v2_pool_helper = degenbot_lp_helpers[event_address]
        except KeyError:
            pass
        except Exception as e:
            print(e)
            print(type(e))
        else:
            reserves0, reserves1 = event_reserves
            v2_pool_helper.update_reserves(
                external_token0_reserves=reserves0,
                external_token1_reserves=reserves1,
                silent=not VERBOSE_UPDATES,
                print_ratios=False,
                update_block=event_block
            )
            # find all arbs that care about this pool
            if arbs_affected := [
                arb
                for arb in degenbot_cycle_arb_helpers.values()
                for lp_obj in arb.swap_pools
                if v2_pool_helper is lp_obj
            ]:
                arbs_to_check.extend(arbs_affected)
        finally:
            nonlocal processed_syncs
            processed_syncs += 1
            if VERBOSE_EVENTS and processed_syncs % 500 == 0:
                print(f"[EVENT] Processed {processed_syncs} syncs")

    def process_mint_event(message: dict):

        event_address = w3.toChecksumAddress(
            message.get("params").get("result").get("address")
        )
        event_block = int(
            message.get("params").get("result").get("blockNumber"),
            16
        )
        event_data = message.get("params").get("result").get("data")

        try:
            v3_pool_helper = degenbot_lp_helpers[event_address]
            event_tick_lower = eth_abi.decode(
                ["int24"],
                bytes.fromhex(
                    message.get("params").get("result").get("topics")[2][2:]
                ),
            )[0]
            event_tick_upper = eth_abi.decode(
                ["int24"],
                message.get("params").get("result").get("topics")[3][2:]
            )[0]
            _, event_liquidity, _, _ = eth_abi.decode(
                ["address", "uint128", "uint256", "uint256"],
                bytes.fromhex(event_data[2:])
            )
        except KeyError:
            pass
        except Exception as e:
            print(e)
            print(type(e))
        else:
            if event_liquidity != 0:
                v3_pool_helper.external_update(
                    updates={
                        "liquidity_change": (
                            event_liquidity,
                            event_tick_lower,
                            event_tick_upper,
                        )
                    },
                    block_number=event_block,
                )
                # find all arbs that care about this pool
                arbs_affected = [
                    arb
                    for arb in degenbot_cycle_arb_helpers.values()
                    for pool_obj in arb.swap_pools
                    if v3_pool_helper is pool_obj
                ]
                arbs_to_check.extend(arbs_affected)
        finally:
            nonlocal processed_mints
            processed_mints += 1
            if VERBOSE_EVENTS and processed_mints % 500 == 0:
                print(f"[EVENT] Processed {processed_mints} mints")

    def process_burn_event(message: dict):

        event_address = w3.toChecksumAddress(
            message.get("params").get("result").get("address")
        )
        event_block = int(
            message.get("params").get("result").get("blockNumber"),
            16,
        )
        event_data = message.get("params").get("result").get("data")

        # ignore events for pools we are not tracking
        if not (v3_pool_helper := degenbot_lp_helpers.get(event_address)):
            return

        try:
            event_tick_lower = eth_abi.decode(
                ["int24"],
                bytes.fromhex(
                    message.get("params").get("result").get("topics")[2][2:]
                ),
            )[0]
            event_tick_upper = eth_abi.decode(
                ["int24"],
                bytes.fromhex(
                    message.get("params").get("result").get("topics")[3][2:]
                )
            )[0]
            event_liquidity, _, _ = eth_abi.decode(
                ["uint128","uint256","uint256"],
                bytes.fromhex(event_data[2:]),
            )
            event_liquidity *= -1
        except Exception as e:
            print(e)
        else:
            if event_liquidity != 0:
                v3_pool_helper.external_update(
                    updates={
                        "liquidity_change": (
                            event_liquidity,
                            event_tick_lower,
                            event_tick_upper,
                        )
                    },
                    block_number=event_block,
                )

            # find all arbs that care about this pool
            arbs_affected = [
                arb
                for arb in degenbot_cycle_arb_helpers.values()
                for pool_obj in arb.swap_pools
                if v3_pool_helper is pool_obj
            ]
            arbs_to_check.extend(arbs_affected)
        finally:
            nonlocal processed_burns
            processed_burns += 1
            if VERBOSE_EVENTS and processed_burns % 500 == 0:
                print(f"[EVENT] Processed {processed_burns} burns")

    def process_swap_event(message: dict):

        event_address = w3.toChecksumAddress(
            message.get("params").get("result").get("address")
        )
        event_block = int(
            message.get("params").get("result").get("blockNumber"),
            16,
        )
        event_data = message.get("params").get("result").get("data")

        (
            _,
            _,
            event_sqrt_price_x96,
            event_liquidity,
            event_tick
        ) = eth_abi.decode(
            [
                "int256",
                "int256",
                "uint160",
                "uint128",
                "int24"
            ],
            bytes.fromhex(event_data[2:]),
        )

        try:
            v3_pool_helper = degenbot_lp_helpers[event_address]
            v3_pool_helper.external_update(
                updates={
                    "tick": event_tick,
                    "liquidity": event_liquidity,
                    "sqrt_price_x96": event_sqrt_price_x96,
                },
                block_number=event_block
            )
        except KeyError:
            pass
        except Exception as e:
            print(f"update_v3_pools: {e}")
            print(type(e))
        else:
            # find all arbs that care about this pool
            arbs_affected = [
                arb
                for arb in degenbot_cycle_arb_helpers.values()
                for pool_obj in arb.swap_pools
                if v3_pool_helper is pool_obj
            ]
            arbs_to_check.extend(arbs_affected)
        finally:
            nonlocal processed_swaps
            processed_swaps += 1
            if VERBOSE_EVENTS and processed_swaps % 100 == 0:
                print(f"[EVENT] processed {processed_swaps} swaps")

    TOPICS = {
        w3.keccak(text="Sync(uint112,uint112)").hex(): {
            "name": "Uniswap V2: SYNC",
            "process_func": process_sync_event,
        },
        w3.keccak(text="Mint(address,address,int24,int24,uint128,uint256,uint256)").hex(): {
            "name": "Uniswap V3: MINT",
            "process_func": process_mint_event,
        },
        w3.keccak(text="Burn(address,int24,int24,uint128,uint256,uint256)").hex(): {
            "name": "Uniswap V3: BURN",
            "process_func": process_burn_event,
        },
        w3.keccak(text="Swap(address,address,int256,int256,uint160,uint128,int24)").hex(): {
            "name": "Uniswap V3: SWAP",
            "process_func": process_swap_event,
        }
    }

    async for websocket in websockets.connect(
        uri=WEBSOCKET_URI,
        ping_timeout=None,
    ):
        status_events = False
        first_event_block = 0

        try:
            await websocket.send(
                json.dumps(
                    {
                        "id": 1,
                        "method": "eth_subscribe",
                        "params": ["logs", {}],
                    }
                )
            )
            subscribe_result = json.loads(await websocket.recv())
            print(subscribe_result)

            status_events = True

            start = time.time()

            while True:
                try:
                    message = json.loads(
                        await asyncio.wait_for(
                            websocket.recv(),
                            timeout=_TIMEOUT,
                        )
                    )

                # if no event has been received in _TIMEOUT seconds, assume all
                # events have been received, reduce the list of arbs to check with
                # set(), repackage and send for processing, then clear the
                # working queue
                except asyncio.exceptions.TimeoutError as e:
                    if arbs_to_check:
                        if ARB_ONCHAIN_ENABLE:
                            asyncio.create_task(
                                process_onchain_arbs(
                                    deque(set(arbs_to_check)),
                                )
                            )
                        arbs_to_check.clear()
                    continue
                except Exception as e:
                    print(f"(watch events) websocket.recv(): {e}")
                    print(type(e))
                    break

                if not first_event_block:
                    first_event_block = int(
                        message.get("params").get("result").get("blockNumber"),
                        16,
                    )
                    print(f"First event block: {first_event_block}")

                received_events += 1
                if VERBOSE_EVENTS and received_events % 5000 == 0:
                    print(f"[EVENTS] Received {received_events} total events")

                try:
                    topic0 = (
                        message.get("params").get("result").get("topics")[0]
                    )
                except IndexError:
                    continue
                except Exception as e:
                    print(f"(event_watcher): {e}")
                    print(type(e))
                    continue

                # process the message for the associated event
                try:
                    TOPICS[topic0]["process_func"](message)
                except KeyError:
                    continue
        except Exception as e:
            print("event_watcher reconnecting")
            print(e)


# Watches the websocket for new blocks, updates the base fee for the last block, scans transactions and removes them from the pending tx queue
async def watch_new_blocks():

    print("Starting block watcher loop")

    global first_new_block
    global newest_block
    global newest_block_timestamp
    global last_base_fee
    global next_base_fee
    global status_new_blocks

    async for websocket in websockets.connect(
        uri=WEBSOCKET_URI,
        ping_timeout=None,
    ):

        # reset the first block and status every time we connect or reconnect
        status_new_blocks = False
        first_new_block = 0

        try:
            await websocket.send(
                json.dumps(
                    {
                        "id": 1,
                        "method": "eth_subscribe",
                        "params": ["newHeads"],
                    }
                )
            )
            subscribe_result = json.loads(await websocket.recv())
            print(subscribe_result)
            status_new_blocks = True

            while True:

                await asyncio.sleep(0)
                message = json.loads(await websocket.recv())

                if VERBOSE_TIMING:
                    print("starting watch_new_blocks")
                    start = time.monotonic()

                newest_block = int(
                    message.get("params").get("result").get("number"),
                    16
                )
                newest_block_timestamp = int(
                    message.get("params").get("result").get("timestamp"),
                    16
                )

                if not first_new_block:
                    first_new_block = newest_block
                    print(f"First full block: {first_new_block}")
                last_base_fee = w3.eth.fee_history(
                    1, newest_block
                ).get("baseFeePerGas")[0]
                gas_used_ratio = w3.eth.fee_history(
                    1, newest_block
                ).get("gasUsedRatio")[0]
                #TODO: monitor if the next base fee logic is valid
                next_base_fee = last_base_fee if gas_used_ratio < 1 else last_base_fee*1.05


                # remove all confirmed transactions from the all_pending_tx dict
                for hash in w3.eth.get_block(newest_block).get("transactions"):
                    all_pending_tx.pop(hash.hex(), None)

                if VERBOSE_BLOCKS:
                    print(
                        f"[{newest_block}] "
                        + f"base fee: {last_base_fee / (10 ** 9):.1f} (this) / {next_base_fee / (10 ** 9):.1f} (next) - "
                        f"(+{time.time() - newest_block_timestamp:.2f}s)"
                    )

                if VERBOSE_TIMING:
                    print(
                        f"watch_new_blocks completed in {time.monotonic() - start:0.4f}s"
                    )

        except Exception as e:
            print("watch_new_blocks reconnecting...")
            print(e)


# calculates if the arbitrages are profitable, and send them to execute_arb to execute
async def process_onchain_arbs(arbs: deque):

    arbs_submitted_this_block = []
    arbs_processed = 0

    while True:
        await asyncio.sleep(0)

        try:
            arb_helper = arbs.popleft()
        except IndexError:
            # queue is empty, break the loop
            break

        try:
            if arb_helper.auto_update(
                silent=True,
                block_number=newest_block
            ):
                arb_helper.calculate_arbitrage()
        except bot.exceptions.ArbitrageError as e:
            continue
        except Exception as e:
            print(f"process_onchain_arbs: {e}")
            print(type(e))
            continue
        else:
            arbs_processed += 1

        if VERBOSE_PROCESSING and arbs_processed:
            print(
                f"(process_onchain_arbs) processed {arbs_processed} updated arbs"
            )

        profitable_arbs = (
            arb_helper
            for arb_helper in degenbot_cycle_arb_helpers.copy().values()
            if arb_helper.gas_estimate
            if (_profit := arb_helper.best.get("profit_amount"))
            if _profit > 0
        )
        #profitable_arbs_list = [arb_helper
        #                        for arb_helper in degenbot_cycle_arb_helpers.copy().values()
        #                        if arb_helper.gas_estimate
        #                        if (_profit := arb_helper.best.get("profit_amount"))
        #                        if _profit > (0 * 10**18)]

        #print(f"Number of profitable_arbs: {len(profitable_arbs_list)}")

        best_arb_profit = 0
        best_arb = None

        while True:
            try:
                arb_helper = next(profitable_arbs)
            except StopIteration:
                break
            else:
                if arb_helper in arbs_submitted_this_block:
                    continue
                if (profit := arb_helper.best["profit_amount"]) > best_arb_profit:
                    best_arb_profit = profit
                    best_arb = arb_helper

        if best_arb:
            if VERBOSE_PROCESSING:
                print(f"Best arb: {best_arb}")
                print(f"Best arb profit: {best_arb.best['profit_amount'] / 10**18}")
            arbs_submitted_this_block.append(best_arb)
            await execute_arb(
                arb_dict=best_arb,
                #state_block=newest_block,
                #target_block=newest_block+1, #TODO: CHECK IF NEWEST_BLOCK+1 IS VALID FOR FANTOM
                arb_id=best_arb.id,
            )


# execute_arb: build the bundles and send the transaction
async def execute_arb(
        arb_dict: dict,
        #state_block: int,
        #target_block: int,
        arb_id=None,
        backrun_mempool_tx=None,
        frontrun_mempool_tx=None,
):
    global arb_simulations

    if not (arb_helper := degenbot_cycle_arb_helpers.get(arb_id)):
        return

    if VERBOSE_TIMING:
        start = time.monotonic()
        print("Starting execute_arb")

    gas_fee = arb_helper.gas_estimate * next_base_fee
    arb_profit = arb_dict.best.get("profit_amount") - gas_fee

    if arb_profit > MIN_PROFIT_FTM: # and not DRY_RUN:

        # Simulate the transaction

        print()
        if backrun_mempool_tx:
            print("*** BACKRUN ARB ***")
        elif frontrun_mempool_tx:
            print("*** FRONTRUN ARB ***")
        else:
            print("*** ONCHAIN ARB ***")
        print(f"Arb    : {arb_helper}")
        print(f"Profit : {arb_dict.best.get('profit_amount') / (10 ** 18):0.5f} FTM")
        print(f"Gas    : {gas_fee / (10 ** 18):0.5f} FTM")
        print(f"Net    : {arb_profit / (10 ** 18):0.5f} FTM")

        total_priority_fee = int(MINER_TIP * arb_profit) if (int(MINER_TIP * arb_profit) > 0) else (0.025 * 10**18) #TODO change
        priority_fee_per_gas = total_priority_fee // arb_helper.gas_estimate

        print(f"Priority fee: {total_priority_fee/(10**18):0.5f} FTM")

        tx_params = {
            "from": bot_account.address,
            "nonce": bot_account.nonce,
            "gas": arb_gas
            if (arb_gas := int(1.1 * arb_helper.gas_estimate))
            else 250_000, #TODO check for Fantom
            "chainId": brownie.chain.id,
            "maxFeePerGas": int((next_base_fee + priority_fee_per_gas) * 1.05),
            "maxPriorityFeePerGas": int(priority_fee_per_gas),
        }

        arb_payloads = arb_helper.generate_payloads(
            from_address=arb_contract.address
        )

        transactions_to_bundle = [
            w3.eth.contract(address=arb_contract.address, abi=arb_contract.abi)
            .functions.execute_payloads(arb_payloads)
            .buildTransaction(tx_params)
        ]

        bundle = []
        if backrun_mempool_tx:
            bundle.append({"signed_transaction": backrun_mempool_tx})
        for transaction in transactions_to_bundle:
            tx = eth_account.Account.from_key(
                bot_account.private_key
            ).sign_transaction(transaction)
            signed_transaction = tx.rawTransaction
            bundle.append({"signed_transaction": signed_transaction})
        if frontrun_mempool_tx:
            bundle.append({"signed_transaction": frontrun_mempool_tx})

        if backrun_mempool_tx or frontrun_mempool_tx:
            bundle_valid_blocks = 5
        else:
            bundle_valid_blocks = 1

        # Simulate the transaction:
        simulation_success = False
        try:
            for transaction in transactions_to_bundle:
                try:
                    simulated_gas_use = w3.eth.estimate_gas(transaction)
                except Exception as e:
                    print(f"(simulation failed for transaction {transaction}): {e}")
                else:
                    simulation_success = True
                    print("simulation success!")
                    arb_helper.gas_estimate = simulated_gas_use

        except Exception as e:
            print(f"(simulation outer): {e}")
            print(type(e))

        
        if simulation_success:
            gas_fee = arb_helper.gas_estimate * next_base_fee
            arb_profit = arb_dict.best.get("profit_amount") - gas_fee
            total_priority_fee = int(MINER_TIP * arb_profit) if (int(MINER_TIP * arb_profit) > 0) else (0.025 * 10**18) #TODO change
            priority_fee_per_gas = total_priority_fee // arb_helper.gas_estimate

            tx_params = {
                "from": bot_account.address,
                "nonce": bot_account.nonce,
                "gas": arb_gas if (arb_gas := int(1.1 * arb_helper.gas_estimate)) else 250_000,  # TODO check for Fantom
                "chainId": brownie.chain.id,
                "maxFeePerGas": int((next_base_fee + priority_fee_per_gas) * 1.05),
                "maxPriorityFeePerGas": int(priority_fee_per_gas),
            }

            transactions_to_bundle = [
                w3.eth.contract(address=arb_contract.address, abi=arb_contract.abi)
                .functions.execute_payloads(arb_payloads)
                .buildTransaction(tx_params)
            ]


        if simulation_success and not DRY_RUN:
            # submit the bundle to the relay, retrying up to RELAY_RETRIES times per valid block
            attempts = 0
            for i in range(bundle_valid_blocks):
                for transaction in bundle:
                    while True:
                        if attempts == RELAY_RETRIES:
                            return

                        await asyncio.sleep(0)

                        try:
                            attempts += 1
                            print("Sending the transaction")
                            signed_tx = transaction["signed_transaction"]
                            tx_hash = w3.eth.send_raw_transaction(signed_tx)
                        except Exception as e:
                            print(e)
                            await asyncio.sleep(0.25)
                        else:
                            attempts = 0
                            break

            print(f"Transaction sent. Tx hash: {tx_hash.hex()}")
            record_transaction(
                tx_hash=tx_hash.hex(),
                arb_id=arb_id,
                blocks=int(brownie.chain.height)
            )
            print("Transaction recorded")
    #else:
        #print("arb_profit did not exceed MIN_PROFIT threshold")
        #print()
        #print(f"arb_profit without gas: {arb_dict.best.get('profit_amount') / 10**18}")
        #print(f"gas_fee: {gas_fee / 10**18}")
        #print(f"arb_profit: {arb_profit / 10**18}")
        #print(f"Min profit threshold: {MIN_PROFIT_FTM / 10**18}")



# submits the sent bundles to a json file
def record_transaction(
        tx_hash: str,
        arb_id: str,
        blocks: int,
):

    SUBMITTED_BUNDLES[tx_hash] = {
        "tx_hash": tx_hash,
        "arb_id": arb_id,
        "blocks": blocks,
        "time": datetime.datetime.utcnow().timestamp()
    }

    with open("submitted_transactions.json", "w") as file:
        json.dump(SUBMITTED_BUNDLES, file, indent=2)


# tracks and updates the balance of the wrapped token on the contract
async def track_balance():

    wftm_balance = 0

    while True:

        await asyncio.sleep(AVERAGE_BLOCK_TIME)

        try:
            balance = wftm.balanceOf(arb_contract.address)
        except Exception as e:
            print(f"(track_balance): {e}")
        else:
            if balance != wftm_balance:
                print()
                print(f"Updated balance: {balance/10**18:.3f} FTM")
                print()
                wftm_balance = balance
                for arb in degenbot_cycle_arb_helpers.copy().values():
                    arb.max_input = wftm_balance


# Tasked with monitoring other coroutines, functions, objects, etc. and setting bot status variables like `status_paused`
# Other coroutines should monitor the state of `status_paused` and adjust their activity as needed
async def status_watchdog():

    global status_paused

    print("Starting status watchdog")

    while True:
        await asyncio.sleep(0)

        # our node will always be slightly delayed compared to the timestamp of the block,
        # so compare that difference on each pass through the loop
        if (
                time.monotonic() - newest_block_timestamp
                > AVERAGE_BLOCK_TIME + LATE_BLOCK_THRESHOLD
        ):
            # if the expected block is late, set the paused flag to True
            if not status_paused:
                status_paused = True
                if VERBOSE_WATCHDOG:
                    #print(f"WATCHDOG: time.time - newest_block_timestamp: {time.time() - newest_block_timestamp}")
                    #print(f"WATCHDOG: average block time: {AVERAGE_BLOCK_TIME}")
                    #print(f"WATCHDOG: late block threshold: {LATE_BLOCK_THRESHOLD}")
                    #print(f"WATCHDOG: average block time + late block threshold: {AVERAGE_BLOCK_TIME + LATE_BLOCK_THRESHOLD}")
                    print("WATCHDOG: paused (block late)")
        elif status_pool_sync_in_progress:
            if not status_paused:
                status_paused = True
                if VERBOSE_WATCHDOG:
                    print("WATCHDOG: paused (pool sync in progress)")
        else:
            if status_paused:
                status_paused = False
                if VERBOSE_WATCHDOG:
                    #print(f"WATCHDOG: time.time - newest_block_timestamp: {time.time() - newest_block_timestamp}")
                    #print(f"WATCHDOG: average block time: {AVERAGE_BLOCK_TIME}")
                    #print(f"WATCHDOG: late block threshold: {LATE_BLOCK_THRESHOLD}")
                    #print(f"WATCHDOG: average block time + late block threshold: {AVERAGE_BLOCK_TIME + LATE_BLOCK_THRESHOLD}")
                    print("WATCHDOG: unpaused")


async def measure_rpc_latency():
    block_number = newest_block
    start_time = time.time()
    while True:
        await asyncio.sleep(0)
        if newest_block > block_number:
            break
    end_time = time.time()
    return end_time - start_time


async def average_block_time():
    global AVERAGE_BLOCK_TIME
    global LATE_BLOCK_THRESHOLD
    print("starting average_block_time")
    old_block_time = AVERAGE_BLOCK_TIME
    while True:
        latency = await measure_rpc_latency()
        AVERAGE_BLOCK_TIME = latency
        LATE_BLOCK_THRESHOLD = AVERAGE_BLOCK_TIME * 0.3
        await asyncio.sleep(0.01)
        if VERBOSE_UPDATES:
            if AVERAGE_BLOCK_TIME != old_block_time:
                print(f"Block time set to {AVERAGE_BLOCK_TIME}")


if not DRY_RUN:
    print(
        "\n"
        "\n***************************************"
        "\n*** DRY RUN DISABLED - BOT IS LIVE! ***"
        "\n***************************************"
        "\n"
    )
    time.sleep(10)

# Create a reusable web3 object to communicate with the node
w3 = web3.Web3(web3.WebsocketProvider(WEBSOCKET_URI))

AVERAGE_BLOCK_TIME = 1.1 # overridden on first received block
# how many seconds behind the chain timestamp to consider the block
# "late" and disable processing until it catches up
LATE_BLOCK_THRESHOLD = 2  # overridden on first received block

os.environ["FTMSCAN_TOKEN"] = FTMSCAN_API_KEY

try:
    brownie.network.connect(BROWNIE_NETWORK)
except:
    sys.exit(
        "Could not connect! Verify your Brownie network settings using 'brownie networks list'"
    )
else:
    # swap out the brownie web3 object - workaround for the
    # `block_filter_loop` thread that Brownie starts.
    # It sometimes crashes on concurrent calls to websockets.recv()
    # and creates a zombie middleware that returns stale state data
    brownie.web3 = w3

try:
    bot_account = brownie.accounts.load(
        BROWNIE_ACCOUNT, password="kfndkjnfjkvkmkl1!!"
    )
except:
    sys.exit(
        "Could not load account! Verify your Brownie account settings using 'brownie accounts list'"
    )



arb_contract = brownie.Contract.from_abi(
    name="",
    address=ARB_CONTRACT_ADDRESS,
    abi=json.loads(
        """
        [{"stateMutability": "payable", "type": "constructor", "inputs": [], "outputs": []}, {"stateMutability": "payable", "type": "function", "name": "execute_payloads", "inputs": [{"name": "payloads", "type": "tuple[]", "components": [{"name": "target", "type": "address"}, {"name": "calldata", "type": "bytes"}, {"name": "value", "type": "uint256"}]}], "outputs": []}, {"stateMutability": "payable", "type": "function", "name": "uniswapV3SwapCallback", "inputs": [{"name": "amount0", "type": "int256"}, {"name": "amount1", "type": "int256"}, {"name": "data", "type": "bytes"}], "outputs": []}, {"stateMutability": "payable", "type": "fallback"}]
        """
    ),
)

try:
    wftm = brownie.Contract(WFTM_ADDRESS)
except Exception as e:
    print(e)
    try:
        wftm = brownie.Contract.from_explorer(WFTM_ADDRESS)
    except Exception as e:
        print(e)

# load historical submitted bundles
SUBMITTED_BUNDLES = {}
try:
    with open("submitted_bundles.json") as file:
        SUBMITTED_BUNDLES= json.load(file)
# if the file doesn't exist, create it
except FileNotFoundError:
    with open("submitted_bundles.json", "w") as file:
        json.dump(SUBMITTED_BUNDLES, file, indent=2)

# load the blacklists
BLACKLISTED_TOKENS = []
for filename in ["fantom_blacklisted_tokens.json"]:
    try:
        with open(filename) as file:
            BLACKLISTED_TOKENS.extend(json.load(file))
    except FileNotFoundError:
        with open(filename, "w") as file:
            json.dump(BLACKLISTED_TOKENS, file, indent=2)
print(f"Found {len(BLACKLISTED_TOKENS)} blacklisted tokens")

BLACKLISTED_ARBS = []
for filename in ["fantom_blacklisted_arbs.json"]:
    try:
        with open(filename) as file:
            BLACKLISTED_ARBS.extend(json.load(file))
    except FileNotFoundError:
        with open(filename, "w") as file:
            json.dump(BLACKLISTED_ARBS, file, indent=2)
print(f"Found {len(BLACKLISTED_ARBS)} blacklisted arbs")

last_base_fee = 35 * 10**9  # overridden on first received block
next_base_fee = 35 * 10**9   # overridden on first received block
newest_block = 0    # overridden on first received block
newest_block_timestamp = int(time.time())   # overridden on first received block
status_events = False
status_new_blocks = False
status_paused = True
status_pool_sync_in_progress = False
first_new_block = None
first_event_block = 0
all_pending_tx = {}
degenbot_lp_helpers = {}
degenbot_cycle_arb_helpers = {}
arb_simulations = {}


if __name__ == "__main__":
    asyncio.run(_main_async())