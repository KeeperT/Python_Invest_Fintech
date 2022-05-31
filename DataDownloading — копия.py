# Код написан на основе документации API https://tinkoff.github.io/investAPI/
# В основном используется Сервис Котировок https://tinkoff.github.io/investAPI/marketdata/
# Figi - это уникальный ID акции

import pandas as pd
import numpy as np
from tqdm import tqdm
from datetime import datetime, timedelta
import datetime as dt
from tinkoff.invest import *
import os.path
import time
from tinkoff.invest.utils import now
import logging


def get_shares_list_to_csv():
    # FUNC позволяет получить из API список всех акций
    # FUNC создаёт CSV-файл c более подробными данными, чем на выходе функции
    # На выход подаётся массив (Series) всех figi акций

    df_figi, df_shares = 'error', 'error'

    try:
        with Client(token) as client:  # обёртка
            all_shares = client.instruments.shares()  # запрашивает название всех акций и закладывает их в переменную
        df_shares = pd.DataFrame(all_shares.instruments)
        df_shares.set_index(['figi'], inplace=True)
        df_figi = df_shares.index  # для подачи на выход функции массива всех figi
        df_shares.to_csv('shares.csv', sep=';')  # выгружает dataframe в CSV
        print('✅Downloaded list of shares')

    except Exception as e:
        print('No internet connection? Reconnecting in 60 sec:')
        print(e)
        time.sleep(60)
        get_shares_list_to_csv()

    return df_figi, df_shares


def last_data_parser(figi):
    # функция позволяет получить самую позднюю дату из csv-файла c Historic_close_prices в формате datetime
    # на вход подаётся один str(figi)
    # функция используется в def one_figi_all_candles_request

    if os.path.exists('historic_close_prices.csv'):  # проврека на существование файла
        try:
            figi_last_date = pd.read_csv('historic_close_prices.csv',
                                         sep=';',
                                         index_col='Unnamed: 0')[figi]  # выделяет DataFrame для поиска в нём даты
            figi_last_date = figi_last_date.dropna().index  # выделяет из DF только массив дат
            figi_last_date = pd.to_datetime(figi_last_date, infer_datetime_format=True).max()  # выделяет последнюю дату

        except Exception as e:
            logging.exception(e)
            figi_last_date = dt.datetime(2012, 1, 1)

    else:
        figi_last_date = dt.datetime(2012, 1, 1)

    return figi_last_date


def one_figi_all_candles_request(figi, last_date, df_fin_volumes, df_fin_close_prices):
    # функция запрашивает все ОТСУТСТВУЮЩИЕ свечи по ОДНОМУ str(figi), который подаётся на вход.
    # далее парсит полученные данные (цену закрытия, объёмы)
    # и сохраняет их в 2 DataFrame df_fin_close_prices и df_fin_volumes
    # вспомогательная функция для def create_2_csv_with_historic_candles

    days = (datetime.utcnow() - last_date).days
    with Client(token) as client:
        for candle in client.get_all_candles(
                figi=figi,  # сюда должен поступать только один figi (id акции)
                from_=now() - timedelta(days=days),  # период времени определяется динамически функцией last_data_parser
                interval=CandleInterval.CANDLE_INTERVAL_DAY,  # запрашиваемая размерность японских свеч (дневная)
        ):
            data = dt.datetime(candle.time.year, candle.time.month, candle.time.day)  # из ответа API парсит дату
            close_price = f'{candle.close.units}.{candle.close.nano // 10000000}'  # из ответа API парс. цену закрытия
            volume = candle.volume  # из ответа API парсит объём торгов

            # print('Цена открытия:', candle.open.units, '.', candle.open.nano // 10000000, sep='')
            # print('Цена закрытия:', candle.close.units, '.', candle.close.nano // 10000000, sep='')
            # print('Макс. цена:', candle.high.units, '.', candle.high.nano // 10000000, sep='')
            # print('Мин. цена:', candle.low.units, '.', candle.low.nano // 10000000, sep='')
            # print('Объём:', candle.volume, 'сделок')
            # print('')

            if os.path.exists('historic_close_prices.csv'):  # проверяет существование файла
                df_fin_close_prices.at[data, figi] = close_price  # если данных нет, записывает новые
                df_fin_volumes.at[data, figi] = volume  # если данных нет, записывает новые

            else:
                df_fin_close_prices.at[data, figi] = close_price
                df_fin_volumes.at[data, figi] = volume


def create_2_csv_with_historic_candles():
    # FUNC позволяет создать два CSV-файла с historic_close_prices и historic_volumes
    # на вход подаётся path, df_all_figi
    # на выходе два DF с historic_close_prices и historic_volumes

    # ниже подготовка входных данных для функций
    if os.path.exists('historic_close_prices.csv'):  # проверка существует ли файл
        df_fin_close_prices = pd.read_csv('historic_close_prices.csv', sep=';', parse_dates=[0], index_col=0)
        df_fin_volumes = pd.read_csv('historic_volumes.csv', sep=';', parse_dates=[0], index_col=0)
    else:
        df_fin_close_prices = pd.DataFrame()  # пустой DF, если файла нет
        df_fin_volumes = pd.DataFrame()  # пустой DF, если файла нет

    for i in tqdm(range(len(df_all_figi)), desc='Downloading historic candles'):
        figi = df_all_figi[i]
        last_date = last_data_parser(figi)
        # выше подготовка входных данных для функций

        if (datetime.utcnow() - last_date).days != 0:  # проверка: не запрашиваем ли существующие в CSV данные
            try:
                one_figi_all_candles_request(figi, last_date, df_fin_volumes, df_fin_close_prices)
                time.sleep(0.61)  # не более 100 запросов API в минуту
            except Exception as e:
                print(f'Wait 60sec to download more candles, resource exhausted:')
                print(e)
                time.sleep(60)
                try:
                    one_figi_all_candles_request(figi, last_date, df_fin_volumes, df_fin_close_prices)
                    time.sleep(0.601)  # не более 100 запросов API в минуту
                except Exception as e:
                    logging.error('Failed downloading candles twice', exc_info=e)

    df_fin_close_prices = df_fin_close_prices.sort_index()  # сортируем DF по датам по возрастанию
    df_fin_close_prices.to_csv('historic_close_prices.csv', sep=';')
    df_fin_volumes = df_fin_volumes.sort_index()  # сортируем DF по датам по возрастанию
    df_fin_volumes.to_csv('historic_volumes.csv', sep=';')
    print('✅Successfully downloaded and saved historic candles')

    return df_fin_close_prices, df_fin_volumes


def get_all_lasts():
    # FUNC поучает на вход массив figi
    # на выходе dataframe со всеми ластами соответствующих figi

    with Client(token) as client:
        request_lasts = client.market_data.get_last_prices(
            figi=(df_all_figi.tolist())).last_prices  # запрос данных из API. Отвечает массивом?

    df = pd.DataFrame(columns=['figi', 'last_price', 'datetime'])
    for n in request_lasts:
        last_price = f"{n.price.units}.{n.price.nano // 10000000}"  # парсит last из ответа API
        date_time = dt.datetime(n.time.year, n.time.month, n.time.day, n.time.hour, n.time.minute, n.time.second)
        figi = n.figi  # получает figi из ответа API
        df.loc[len(df.index)] = [figi, last_price, date_time]  # сохраняет данные в DF
    df.set_index('figi', inplace=True)  # индексирует DF по figi

    return df


def calc_std():
    # FUNC получает на вход массив figi
    # на выходе DataFrame с подсчётами "стандартного отклонения" для каждого figi

    df_fin_close_prices = pd.read_csv('historic_close_prices.csv', sep=';', index_col=0)
    df_price_std = pd.DataFrame()  # пустой DF
    for i in df_all_figi:
        df = df_fin_close_prices[i].dropna()  # получаем для каждого figi его DF с close_prices без пустых ячеек
        std = df.tail(std_period).pct_change().std().round(3)  # считаем стандартное отклонение
        df_price_std.loc[i, "std"] = std  # сохраняем стандартное отклонение в DF
    df_price_std.to_csv('std.csv', sep=';')
    print('✅Calc of STD done')

    return df_price_std


def calc_sma():
    # FUNC получает на вход массив figi
    # на выходе DataFrame с подсчётами "скользящего среднего" для каждого figi

    df_fin_close_prices = pd.read_csv('historic_close_prices.csv', sep=';', index_col=0)
    df_sma_final = pd.DataFrame()  # пустой DF
    df_sma2 = pd.DataFrame()  # пустой DF
    df_amount_of_sma = pd.DataFrame(columns=['amount_of_sma_rows'])  # пустой DF

    for x in tqdm(range(len(df_all_figi)), desc='Calculating_historic_SMA'):
        i = df_all_figi[x]
        df = df_fin_close_prices[i].dropna()  # получаем для каждого figi его DF с close_prices без пустых ячеек

        df_sma_short = df.rolling(period_of_short_sma - 1).mean().dropna().round(3)  # скользяшки за коротк. период
        df_sma_long = df.rolling(period_of_long_sma - 1).mean().dropna().round(3)  # скользяшки за длинный период

        df_ma = pd.concat([df_sma_short, df_sma_long], axis=1)  # объединяем короткие и длинные "скользяшки"
        df_ma.columns = [f'{i}.short', f'{i}.long']  # именуем столбцы корректно
        df_sma_final = pd.merge(df_sma2,
                                df_ma,
                                left_index=True,
                                right_index=True,
                                how='outer')  # добавляем данные к итоговому DataFrame df_sma_final
        df_sma2 = df_sma_final  # сохраняем итоговый DF в переменную, чтобы можно было добавить данные след. циклом

        df_amount_of_sma.loc[i] = [df_sma_long.size]

    df_amount_of_sma.to_csv('amount_sma.csv', sep=';')
    df_sma_final.sort_index()
    df_sma_final.to_csv('sma.csv', sep=';')
    print('✅Calc of SMA done')

    return df_amount_of_sma, df_sma_final


def sma_cross(actual_short_sma,
              actual_long_sma,
              df_previous_sma_saving,
              figi,
              last_price,
              df_signals,
              ):
    # функция считает, пересекаются ли скользяшки, а далее формирует сигнал
    # вспомогательная функция для def calc_one_signal

    # из DF с SMA берем определенные по figi (SMA предшествуют актуальным)
    previous_short_sma_2 = df_previous_sma_saving.loc[figi].previous_short_sma
    previous_long_sma_2 = df_previous_sma_saving.loc[figi].previous_long_sma

    # математическая проверка на совпадение с условиями сигнала
    crossing_buy = ((actual_short_sma > actual_long_sma) & (previous_short_sma_2 < previous_long_sma_2) & (
            last_price > actual_long_sma))
    crossing_sell = ((actual_short_sma < actual_long_sma) & (previous_short_sma_2 > previous_long_sma_2) & (
            last_price < actual_long_sma))

    ticker = df_all_shares.loc[figi].ticker

    # если условие выполняется, то записываем данные в CSV
    if crossing_sell and df_signals.loc[figi].sell_flag != 1:
        df_one_signal = pd.DataFrame([[figi, ticker, datetime.now(), last_price, 1, 0, 'SMA']],
                                     columns=['figi',
                                              'ticker',
                                              'datetime',
                                              'last_price',
                                              'sell_flag',
                                              'buy_flag',
                                              'strategy_id'])
        df_one_signal.set_index('figi')
        df_signals.append(df_one_signal)
    if crossing_buy and df_signals.loc[figi].buy_flag != 1:
        df_one_signal = pd.DataFrame([[figi, ticker, datetime.now(), last_price, 0, 1, 'SMA']],
                                     columns=['figi',
                                              'ticker'
                                              'datetime',
                                              'last_price',
                                              'sell_flag',
                                              'buy_flag',
                                              'strategy_id'])
        df_one_signal.set_index('figi')
        df_signals.append(df_one_signal)


def calc_signal_sma(df_all_lasts, df_previous_sma_saving, n, df_signals):
    # FUNC позволяет распарсить CSV 'SMA' и получить исторические SMA, далее по ластам считает актуальные SMA ежеминутно
    # просчитывает все figi
    # На входе df_all_figi, df_all_historic_sma
    # На выходе актуальные SMA_short, SMA_long, а также SMA_short, SMA_long за минуту назад
    # вспомогательная функция для def calc_signals

    df_all_historic_sma = pd.read_csv('sma.csv', sep=';', index_col=0)
    for i in df_all_historic_sma.columns[::2]:

        # ниже получаем данные о исторических SMA из CSV
        i = i[:12]  # считываем figi без лишних элементов
        df_historic_short_sma = df_all_historic_sma[f'{i}.short'].dropna()  # подготовка DF с short_SMA по figi

        if df_historic_short_sma.size != 0:  # проверка на пустой DF
            historic_short_sma = df_historic_short_sma.loc[
                df_historic_short_sma.index.max()]  # закладываем в переменную последнюю короткую SMA
        else:
            historic_short_sma = False

        df_historic_long_sma = df_all_historic_sma[f'{i}.long'].dropna()  # подготовка DF с long_SMA по figi

        if df_historic_long_sma.size != 0:  # проверка на пустой DF
            historic_long_sma = df_historic_long_sma.loc[
                df_historic_long_sma.index.max()]  # закладываем в переменную последнюю длинную SMA
        else:
            historic_long_sma = False

        # ниже получаем актуальные данные о last_price из df_all_lasts и считаем актуальные SMA
        a = datetime.utcnow().year == df_all_lasts.loc[i].datetime.year
        b = datetime.utcnow().month == df_all_lasts.loc[i].datetime.month
        c = datetime.utcnow().day == df_all_lasts.loc[i].datetime.day
        d = datetime.utcnow().hour == df_all_lasts.loc[i].datetime.hour
        e = datetime.utcnow().minute == df_all_lasts.loc[i].datetime.minute
        f = datetime.utcnow().minute - 1 == df_all_lasts.loc[i].datetime.minute
        g = datetime.utcnow().minute - 2 == df_all_lasts.loc[i].datetime.minute
        h = e or f or g
        if a and b and c and d and h:

            last_price = float(df_all_lasts.loc[i].last_price)
            if n == 0:
                previous_short_sma = (
                        (historic_short_sma * (period_of_short_sma - 1) + last_price) / period_of_short_sma).round(3)
                previous_long_sma = (
                        (historic_long_sma * (period_of_long_sma - 1) + last_price) / period_of_long_sma).round(3)
                df_previous_sma_saving.loc[i] = [previous_short_sma, previous_long_sma]

            else:

                actual_short_sma = (
                        (historic_short_sma * (period_of_short_sma - 1) + last_price) / period_of_short_sma).round(3)
                actual_long_sma = (
                        (historic_long_sma * (period_of_long_sma - 1) + last_price) / period_of_long_sma).round(3)
                df_previous_sma_saving.loc[i] = [actual_short_sma,
                                                 actual_long_sma]  # актуальные сигналы становятся прошлыми

                sma_cross(actual_short_sma, actual_long_sma, df_previous_sma_saving, i, last_price, df_signals)


def calc_and_save_actual_signals_sma():
    # функция позволяет циклически считать сигналы на основе постоянно обновляющихся last_prices

    df_previous_sma_saving = pd.DataFrame(index=df_all_figi, columns=['previous_short_sma', 'previous_long_sma'])
    for n in tqdm(range(999999), desc='Calculating_actual_signals_sma'):
        df_all_lasts = get_all_lasts()
        if not os.path.exists('Actual_signals_SMA.csv'):  # TODO тест переноса из цикла наружу
            df_signals = pd.DataFrame(
                columns=['figi', "datetime", 'last_price', 'sell_flag', 'buy_flag', 'strategy_id'])
            df_signals.set_index('figi')
        else:
            df_signals = pd.read_csv('Actual_signals_SMA.csv', sep=';', index_col=0)
        calc_signal_sma(df_all_lasts, df_previous_sma_saving, n, df_signals)
        df_signals.to_csv('Actual_signals_SMA.csv', sep=';')
        time.sleep(15)


def historic_sma_cross(historic_short_sma,
                       historic_long_sma,
                       previous_historic_short_sma,
                       previous_historic_long_sma,
                       figi,
                       historic_last_price,
                       date,
                       df_historic_signals_sma,
                       index_of_row,
                       x):
    # функция считает, пересекаются ли скользяшки, а далее формирует сигнал

    crossing_buy = ((historic_short_sma > historic_long_sma) & (
            previous_historic_short_sma < previous_historic_long_sma) & (historic_last_price > historic_long_sma))
    crossing_sell = ((historic_short_sma < historic_long_sma) & (
            previous_historic_short_sma > previous_historic_long_sma) & (historic_last_price < historic_long_sma))

    # формирование сигнала, запись в DF
    ticker = df_all_shares.ticker[x]
    if crossing_sell:
        df_historic_signals_sma.loc[f'{figi}{index_of_row}'] = [figi, ticker, date, historic_last_price, 1, 0, 'sma', 0]
    if crossing_buy:
        df_historic_signals_sma.loc[f'{figi}{index_of_row}'] = [figi, ticker, date, historic_last_price, 0, 1, 'sma', 0]


def calc_one_historic_signal_sma(i,
                                 df_fin_close_prices,
                                 df_all_historic_sma,
                                 amount_of_rows,
                                 df_historic_signals_sma,
                                 x):
    for index_of_row in range(-1, -amount_of_rows, -1):

        # ниже получаем данные о исторических SMA из CSV
        figi = i[:12]

        df_historic_short_sma = df_all_historic_sma[f'{figi}.short'].dropna()  # подготовка DF с short_SMA по figi
        if df_historic_short_sma.size != 0:  # проверка на пустой DF
            historic_short_sma = df_historic_short_sma.loc[df_historic_short_sma.index[index_of_row]]
            # TODO тест принтом +151 в минус (ошибка?)
            previous_historic_short_sma = df_historic_short_sma.loc[df_historic_short_sma.index[index_of_row - 1]]
            # TODO тест принтом +150 в минус (ошибка?)
        else:
            historic_short_sma = False

        df_historic_long_sma = df_all_historic_sma[f'{figi}.long'].dropna()  # подготовка DF с long_SMA по figi
        if df_historic_long_sma.size != 0:  # проверка на пустой DF
            historic_long_sma = df_historic_long_sma.loc[df_historic_long_sma.index[index_of_row]]
            # TODO тест принтом на дату (ошибка?)
            previous_historic_long_sma = df_historic_long_sma.loc[df_historic_long_sma.index[index_of_row - 1]]
            # TODO тест принтом на дату (ошибка?)
        else:
            historic_long_sma = False

        historic_last_price = df_fin_close_prices[figi][index_of_row + 1]
        date = df_historic_long_sma.index[index_of_row]
        historic_sma_cross(historic_short_sma,
                           historic_long_sma,
                           previous_historic_short_sma,
                           previous_historic_long_sma,
                           figi,
                           historic_last_price,
                           date,
                           df_historic_signals_sma,
                           index_of_row,
                           x)


def calc_historic_signals_sma():
    # функция позволяет циклически считать сигналы на основе постоянно обновляющихся last_prices

    df_fin_close_prices = pd.read_csv('historic_close_prices.csv', sep=';', index_col=0, parse_dates=[0])
    # df_previous_sma_saving = pd.DataFrame(index=df_all_figi, columns=['previous_short_sma', 'previous_long_sma'])
    # TODO удалить? test
    df_all_historic_sma = pd.read_csv('sma.csv', sep=';', index_col=0)
    df_amount_of_sma = pd.read_csv('amount_sma.csv', sep=';', index_col=0, parse_dates=[0])
    df_historic_signals_sma = pd.DataFrame(columns=['figi',
                                                    'ticker',
                                                    'datetime',
                                                    'last_price',
                                                    'sell_flag',
                                                    'buy_flag',
                                                    'strategy_id',
                                                    'profit'])

    for x in tqdm(range(len(df_all_figi)), desc='Calculating_historic_signals_sma'):
        i = df_all_historic_sma.columns[::2][x]
        amount_of_rows = df_amount_of_sma.amount_of_sma_rows[i[:12]]
        calc_one_historic_signal_sma(i,
                                     df_fin_close_prices,
                                     df_all_historic_sma,
                                     amount_of_rows,
                                     df_historic_signals_sma,
                                     x)

    df_historic_signals_sma.reset_index(drop=True, inplace=True)
    df_historic_signals_sma.set_index('figi', inplace=True)
    df_historic_signals_sma.sort_values(by='datetime', inplace=True)
    df_historic_signals_sma.to_csv('historic_signals_sma.csv', sep=';')
    print('✅Historic_signals_are_saved')


def calc_profit_sma():
    # функция считает доходность от использования сигналов

    df_signals = pd.read_csv('historic_signals_sma.csv', sep=';', index_col=0)
    df_profit = pd.DataFrame(columns=['figi', 'profit'])
    profit = 'error'

    for x in tqdm(range(len(df_all_figi)), desc='Calc_profit_of_sma_signals'):
        figi = df_all_figi[x]
        profit = (100 * (df_signals.loc[figi[:12]].tail(2).last_price.sort_values().pct_change()[1])).round(2)
        df_profit.loc[figi] = profit

    df_profit.to_csv('historic_profit_sma.csv', sep=';')
    print('✅Calculation_of_profit_is_done')

    return profit


def calc_historic_rsi_signals():
    df_fin_close_prices = pd.read_csv('historic_close_prices.csv', sep=';', parse_dates=[0], index_col=0)
    df_historic_signals_rsi = pd.DataFrame(columns=['figi',
                                                    'ticker',
                                                    'last_price',
                                                    'buy_flag',
                                                    'sell_flag',
                                                    'rsi_float',
                                                    'date'])  # пустой DF

    # расчет по формуле RSI
    delta = df_fin_close_prices.diff()
    up = delta.clip(lower=0)
    down = -1 * delta.clip(upper=0)
    ema_up = up.ewm(com=14, adjust=False).mean()  # 14 - значение для экспоненциальной скользяшки
    ema_down = down.ewm(com=14, adjust=False).mean()  # 14 - значение для экспоненциальной скользяшки
    rs = ema_up / ema_down
    rsi = (100 - (100 / (1 + rs))).round(2)
    rsi.to_csv('historic_rsi_data.csv', sep=';')  # TODO допилить расчет только недостающих данных

    for x in tqdm(range(len(df_all_figi)), desc='Calculating_historic_rsi_signals'):
        figi = df_all_figi[x]
        upper_rsi = np.nanpercentile(rsi[figi], 95)  # верхняя граница RSI, значение 95 отсеивает RSI примерно выше 70
        lower_rsi = np.nanpercentile(rsi[figi], 2.5)  # нижняя граница RSI, значение 2.5 отсеивает RSI примерно ниже 30
        for y in range(len(rsi[figi])):  # проверка DF rsi на условие upper_rsi, lower_rsi
            rsi_float = rsi[figi][y]
            date = rsi.index[y]
            last_price = df_fin_close_prices[figi][date]

            if rsi_float >= upper_rsi:
                ticker = df_all_shares.ticker[x]
                df_historic_signals_rsi.loc[f'{figi}-{y}'] = [figi, ticker, last_price, 0, 1, rsi_float, date]

            if rsi_float <= lower_rsi:
                ticker = df_all_shares.ticker[x]
                df_historic_signals_rsi.loc[f'{figi}-{y}'] = [figi, ticker, last_price, 1, 0, rsi_float, date]

    df_historic_signals_rsi.reset_index(drop=True, inplace=True)
    df_historic_signals_rsi.set_index('figi', inplace=True)
    df_historic_signals_rsi.sort_values(by='date', inplace=True)
    df_historic_signals_rsi.to_csv('historic_signals_rsi.csv', sep=';')


# первоначальные настройки
token = os.environ['INVEST_TOKEN']  # персональный токен

# настройка стратегии (здесь можно ничего не трогать)
std_period = 20  # дней (обязатльено меньше sma_short_period)
period_of_short_sma = 50  # дней (обязатльено меньше sma_long_period)
period_of_long_sma = 200  # дней

# подготовка исторических данных
df_all_figi, df_all_shares = get_shares_list_to_csv()
create_2_csv_with_historic_candles()

# расчет исторических индикаторов
calc_std()
calc_sma()
print('✅All data saving complete')

# подготовка исторических сигналов
calc_historic_signals_sma()
calc_profit_sma()
calc_historic_rsi_signals()

# подготовка реальных сигналов
# calc_and_save_actual_signals()
