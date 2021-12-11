import asyncio
import copy
import math
from collections import OrderedDict
from datetime import datetime, timezone, timedelta
from itertools import islice
from typing import Optional, Iterable

import binance.exceptions
from binance import AsyncClient, BinanceSocketManager

from core.logging import logger
from settings import API_KEY, SECRET_KEY, SYMBOLS, COPY_BUY_PERCENT, SUPPORTED_CURRENCIES, TP_TARGET, \
    RECORD_SECONDS_TO_KEEP, RECORD_TARGET_PERCENT_TO_STOP_COPY_BUY, DECIMALS
from typings import EStreamType, IAggTrade, IAggTradeData, IAccountInfo, ITokenStat, IPrice, IBalance, IOrder
from utils.time_utils import date_to_timestamp

POW10 = 10 ** DECIMALS


def log_pnl(token_stats: dict[str, ITokenStat], token_stat: ITokenStat, order: IOrder) -> None:
    total_filled_cost = 0
    total_filled_quantity = 0
    for fill in order['fills']:
        total_filled_cost += float(fill['price']) * float(fill['qty'])
        total_filled_quantity += float(fill['qty'])
    if total_filled_quantity == 0:
        average_sold_price = 0
    else:
        average_sold_price = total_filled_cost / total_filled_quantity

    average_bought_price = get_average_bought_price(token_stat)
    gain_amount = average_sold_price - average_bought_price

    if average_bought_price == 0:
        gain_percent = 0
    else:
        gain_percent = 100 * (average_sold_price - average_bought_price) / average_bought_price

    new_dict = {}
    for symbol, inner_dict in token_stats.items():
        temp_dict: dict = copy.deepcopy(inner_dict)
        del temp_dict['last_order']
        new_dict[symbol] = temp_dict
    logger.info(f'Last token stats (excluding last orders): {new_dict}')
    logger.info(f'Last order: {order}')
    logger.info(f'PNL: ${gain_amount:.4f} ({gain_percent:.4f}%)')


def update_token_stat(token_stat: ITokenStat, order: IOrder) -> ITokenStat:
    token_cost = token_stat['token_cost']
    token_quantity = token_stat['token_quantity']
    total_filled_cost = token_cost
    total_filled_quantity = token_quantity
    for fill in order['fills']:
        total_filled_cost += float(fill['price']) * float(fill['qty'])
        total_filled_quantity += float(fill['qty'])
    new_token_stat = {**token_stat, 'token_cost': total_filled_cost, 'token_quantity': total_filled_quantity,
                      'last_order': order}
    logger.info(f'Updated token stat: {new_token_stat}')
    return new_token_stat


def update_sl_price(token_stat: ITokenStat, new_token_stat: ITokenStat) -> ITokenStat:
    if token_stat['token_quantity'] != 0:
        # Only update the first time when quantity is 0
        return new_token_stat
    token_price = token_stat['token_price']
    logger.info(f"Updating SL price from {token_stat['sl_price']} to {token_price}")
    new_token_stat.update({'sl_price': token_price})
    return new_token_stat


async def copy_buy_token(client: AsyncClient, data: IAggTradeData, available_currency: int) -> Optional[IOrder]:
    copy_buy_token_amount = math.floor(float(data['q']) * COPY_BUY_PERCENT * POW10) / float(POW10)
    token_price = float(data['p'])
    affordable_token_amount = math.floor(available_currency / token_price * POW10) / float(POW10)
    token_amount_to_buy = min(copy_buy_token_amount, affordable_token_amount)
    if token_amount_to_buy == 0:
        logger.info(f'Will not execute copy buy order: {token_amount_to_buy=}')
        return None
    logger.info(f'Will execute copy buy order:\n'
                f'{copy_buy_token_amount=}\n'
                f'{token_price=}\n'
                f'{affordable_token_amount=}\n'
                f'{token_amount_to_buy=}')
    order = None
    try:
        order = await client.order_market_buy(symbol=data['s'].upper(), quantity=token_amount_to_buy)
    except binance.exceptions.BinanceAPIException as e:
        logger.error(e)
        if e.code == -1013 and e.message == 'Filter failure: MIN_NOTIONAL':
            # Copy buy amount is below min amount, so we retry at full amount
            copy_buy_token_amount = math.floor(float(data['q']) * POW10) / float(POW10)
            token_amount_to_buy = min(copy_buy_token_amount, affordable_token_amount)
            logger.info(f'Retrying copy buy at full amount: {token_amount_to_buy}')
            order = await client.order_market_buy(symbol=data['s'].upper(), quantity=token_amount_to_buy)
    return order


def should_stop_copy_buy(token_stat: ITokenStat) -> bool:
    ordered_dict: Iterable = token_stat['records'].items()
    records: list[tuple[int, float]] = list(ordered_dict)
    last_record_price = records[-1][1]
    average_bought_price = get_average_bought_price(token_stat)
    if average_bought_price == 0:
        increase_in_percent = 0
    else:
        increase_in_percent = (last_record_price - average_bought_price) / average_bought_price
    stop_copy_buy = increase_in_percent >= RECORD_TARGET_PERCENT_TO_STOP_COPY_BUY
    logger.info(f'should_stop_copy_buy ({stop_copy_buy}):\n'
                f'{records=}\n'
                f'increase_in_percent >= RECORD_TARGET_PERCENT_TO_STOP_COPY_BUY; '
                f'{increase_in_percent:.4f} >= {RECORD_TARGET_PERCENT_TO_STOP_COPY_BUY};')
    return stop_copy_buy


async def get_available_currency(client: AsyncClient, currency: str) -> int:
    balance: IBalance = await client.get_asset_balance(asset=currency.upper())
    return math.floor(float(balance['free']))


def validate_currency(symbol: str) -> str:
    currency = next((currency for currency in SUPPORTED_CURRENCIES if symbol.endswith(currency.lower())), None)
    if not currency:
        error = f'Currency not supported for symbol {symbol}. Supported currencies: {SUPPORTED_CURRENCIES}'
        logger.error(error)
        raise ValueError(error)
    return currency


async def process_copy_buy(client: AsyncClient, data: IAggTradeData, token_stat: ITokenStat,
                           new_token_stat: ITokenStat) -> ITokenStat:
    token_price = token_stat['token_price']
    new_token_price = new_token_stat['token_price']
    currency = validate_currency(token_stat['symbol'])
    has_price_increased_or_maintained = new_token_price >= token_price
    available_currency = await get_available_currency(client, currency)
    stop_copy_buy = should_stop_copy_buy(new_token_stat)

    logger.info(f'process_copy_buy:\n'
                f'has_price_increased_or_maintained ({has_price_increased_or_maintained}): '
                f'new_token_price > token_price; {new_token_price} >= {token_price};\n'
                f'available_currency ({available_currency > 0}): available_currency > 0; {available_currency} > 0;\n'
                f'not stop_copy_buy ({not stop_copy_buy});')
    if has_price_increased_or_maintained and available_currency > 0 and not stop_copy_buy:
        # Copy buy some token to prepare for the pnd
        order = await copy_buy_token(client, data, available_currency)
        if order is not None:
            logger.info(f'Executed copy buy order: {order}')
            new_token_stat = update_sl_price(token_stat, new_token_stat)
            new_token_stat = update_token_stat(new_token_stat, order)
    return new_token_stat


def get_average_bought_price(token_stat: ITokenStat) -> float:
    token_cost = token_stat['token_cost']
    token_quantity = token_stat['token_quantity']
    if token_quantity == 0:
        return 0
    return token_cost / token_quantity


async def check_tp_or_sl_target(client: AsyncClient, token_stat: ITokenStat) -> Optional[IOrder]:
    token_cost = token_stat['token_cost']
    token_quantity = token_stat['token_quantity']
    logger.info(f'Check for TP or SL 1: {token_cost=:.4f} {token_quantity=:.4f}')
    if not token_cost or not token_quantity:
        return None
    token_price = token_stat['token_price']
    sl_price = token_stat['sl_price']
    average_bought_price = get_average_bought_price(token_stat)
    if average_bought_price == 0:
        price_change_percent = 0
    else:
        price_change_percent = (token_price - average_bought_price) / average_bought_price
    logger.info(f'Check for TP or SL 2: {token_price=} {average_bought_price=} {price_change_percent=:.4f} {sl_price=}')

    if price_change_percent >= TP_TARGET or token_price < sl_price:
        logger.info(f'TP or SL target hit:\n'
                    f'TP: price_change_percent >= TP_TARGET; {price_change_percent:.4f} >= {TP_TARGET};\n'
                    f'SL: token_price <= sl_price; {token_price} < {sl_price};')
        return await client.order_market_sell(symbol=token_stat['symbol'].upper(),
                                              quantity=math.floor(token_quantity * POW10) / float(POW10))
    return None


def validate_trade_data(data: IAggTradeData, token_stat: ITokenStat) -> bool:
    """Check if trade data was executed by us to avoid recursively buying in"""
    first_trade_id = data['f']
    last_order = token_stat['last_order']
    if last_order is not None:
        for fill in last_order['fills']:
            if fill['tradeId'] == first_trade_id:
                logger.info(f'Ignoring recursive trade data:\n{data}')
                return False
    return True


def update_records(token_stat: ITokenStat, data: IAggTradeData) -> ITokenStat:
    records = token_stat['records']
    records[data['T']] = float(data['p'])  # Key: trade time; value: trade price
    oldest_datetime_to_keep = datetime.now(timezone.utc) - timedelta(seconds=RECORD_SECONDS_TO_KEEP)
    oldest_timestamp_to_keep = date_to_timestamp(oldest_datetime_to_keep)
    for idx, key in enumerate(records.keys()):
        if int(key) >= oldest_timestamp_to_keep:
            sliced: Iterable = islice(records.items(), idx, len(records))
            token_stat['records'] = OrderedDict(sliced)
            return token_stat
    token_stat['records'] = OrderedDict()
    return token_stat


async def init_token_stats(client: AsyncClient) -> dict[str, ITokenStat]:
    token_stats: dict[str, ITokenStat] = {}

    token_prices: list[IPrice] = await client.get_all_tickers()
    account_info: IAccountInfo = await client.get_account()

    for symbol in SYMBOLS:
        symbol_lower = symbol.lower()
        token_stat: ITokenStat = {'symbol': symbol_lower, 'token_price': 0, 'token_cost': 0, 'token_quantity': 0,
                                  'sl_price': 0, 'last_order': None, 'records': OrderedDict()}
        token_stats[symbol_lower] = token_stat

        # Update token price and sl price
        for obj in token_prices:
            if symbol_lower == obj['symbol'].lower():
                token_price = float(obj['price'])
                token_stats[symbol].update({'token_price': token_price, 'sl_price': token_price})
                break

        # Update token cost and quantity if available
        for balance in account_info['balances']:
            if symbol.lower() == balance['asset'].lower():
                token_quantity = float(balance['free'])
                token_cost = token_quantity * token_stats[symbol]['token_price']
                token_stats[symbol].update({'token_cost': token_cost, 'token_quantity': token_quantity})
                break
    logger.info(f'Initialized token stats: {token_stats}')
    return token_stats


async def parse_agg_trade_data(client: AsyncClient, data: IAggTradeData, token_stats: Optional[dict[str, ITokenStat]]) \
        -> tuple[dict[str, ITokenStat], bool]:
    """BTCUSDT -> Token: BTC; Currency: USDT"""
    token_stats = token_stats or await init_token_stats(client)
    symbol = data['s'] = data['s'].lower()
    token_stat = update_records(token_stats[symbol], data)
    new_token_stat = {**token_stat, 'token_price': float(data['p'])}

    if not validate_trade_data(data, new_token_stat):
        return {**token_stats, symbol: new_token_stat}, False

    tp_or_sl_order = await check_tp_or_sl_target(client, new_token_stat)
    if tp_or_sl_order:
        log_pnl(token_stats, new_token_stat, tp_or_sl_order)
    else:
        new_token_stat = await process_copy_buy(client, data, token_stat, new_token_stat)
    return {**token_stats, symbol: new_token_stat}, bool(tp_or_sl_order)


async def run_multiplex_socket(client: AsyncClient) -> None:
    token_stats = None
    bsm = BinanceSocketManager(client)
    agg_trade = [f'{symbol}@aggTrade' for symbol in SYMBOLS]

    async with bsm.multiplex_socket(agg_trade) as stream:
        while True:
            res: IAggTrade = await stream.recv()
            if EStreamType.aggTrade in res['data']['e']:
                logger.info(f"Incoming data: {res['data']}")
                token_stats, has_tp_or_sl = await parse_agg_trade_data(client, res['data'], token_stats)
                if has_tp_or_sl:
                    # token_stats = await init_token_stats(client)
                    logger.info('Stopping websocket after TP / SL')
                    break


async def main() -> None:
    msg = f'starting pnd-spot-bot {datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S}\n' \
          f'{SYMBOLS=}\n{SUPPORTED_CURRENCIES=}\n{TP_TARGET=}\n{COPY_BUY_PERCENT=}'
    logger.info(msg)
    # telegram_bot.start_bot()
    # telegram_bot.send_message(message=msg)

    client = await AsyncClient.create(API_KEY, SECRET_KEY, testnet=True)
    await asyncio.gather(run_multiplex_socket(client))
    await client.close_connection()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    try:
        asyncio.ensure_future(main())
        loop.run_forever()
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()
