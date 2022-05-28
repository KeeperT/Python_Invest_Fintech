# Код написан на основе документации API https://tinkoff.github.io/investAPI/
# В основном используется Сервис Котировок https://tinkoff.github.io/investAPI/marketdata/
# Figi - это уникальный ID акции

import pandas as pd
from tqdm import trange, tqdm
from datetime import datetime, timedelta
import datetime as dt
from tinkoff.invest import *
from tinkoff.invest.services import Services
import numpy
import pathlib
import os.path
import time
from tinkoff.invest.utils import now
import logging


def get_shares_list_to_csv():
    # FUNC позволяет получить из API список всех акций
    # FUNC создаёт CSV-файл c более подробными данными, чем на выходе функции
    # На выход подаётся массив (Series) всех figi акций

    try:
        with Client(token) as client:  # обёртка
            all_shares = client.instruments.shares()  # запрашивает название всех акций и закладывает их в переменную
        df_all_shares = pd.DataFrame(all_shares.instruments)  # создаёт pandas-dataframe
        df_all_shares.reindex(['figi'])  # задаёт столбец 'figi', как индексный
        df_all_figi = df_all_shares['figi']  # для подачи на выход функции получает массив всех figi
        df_all_shares.to_csv('shares.csv', sep=';')  # выгружает dataframe в CSV
        return df_all_figi

    except:
        print('No internet connection? Reconnecting in 60 sec...')
        time.sleep(60)
        get_shares_list_to_csv()


def last_data_parser(figi):
    # функция позволяет получить самую позднюю дату из csv-файла c Historic_close_prices в формате datetime
    # на вход подаётся один str(figi)
    # функция используется в def one_figi_all_candles_request

    if os.path.exists('Historic_close_prices.csv'):  # проврека на существование файла
        try:
            figi_last_date = pd.read_csv('Historic_close_prices.csv',
                                         sep=';',
                                         index_col='Unnamed: 0')[figi]  # выделяет DataFrame для поиска в нём даты
            figi_last_date = figi_last_date.dropna().index  # выделяет из DF только массив дат
            figi_last_date = pd.to_datetime(figi_last_date, infer_datetime_format=True).max()  # выделяет последнюю дату

        except Exception as e:
            logging.exception(e)
            figi_last_date = dt.datetime(2020, 1, 1)

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

            if os.path.exists('Historic_close_prices.csv'):  # проверяет существование файла
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
    if os.path.exists('Historic_close_prices.csv'):  # проверка существует ли файл
        df_fin_close_prices = pd.read_csv('Historic_close_prices.csv', sep=';', parse_dates=[0], index_col=0)
        df_fin_volumes = pd.read_csv('Historic_volumes.csv', sep=';', parse_dates=[0], index_col=0)
    else:
        df_fin_close_prices = pd.DataFrame()  # пустой DF, если файла нет
        df_fin_volumes = pd.DataFrame()  # пустой DF, если файла нет

    for i in tqdm(range(len(df_all_figi))):
        figi = df_all_figi[i]
        last_date = last_data_parser(figi)
        # выше подготовка входных данных для функций

        if (datetime.utcnow() - last_date).days != 0:  # проверка: не запрашиваем ли существующие в CSV данные
            try:
                one_figi_all_candles_request(figi, last_date, df_fin_volumes, df_fin_close_prices)
                time.sleep(0.601)  # не более 100 запросов API в минуту
            except:
                print('Wait 60sec to download more candles, resource exhausted')
                time.sleep(60)
                try:
                    one_figi_all_candles_request(figi, last_date, df_fin_volumes, df_fin_close_prices)
                    time.sleep(0.601)  # не более 100 запросов API в минуту
                except Exception as e:
                    logging.error('Failed downloading candles twice', exc_info=e)

    df_fin_close_prices = df_fin_close_prices.sort_index()  # сортируем DF по датам по возрастанию
    df_fin_close_prices.to_csv('Historic_close_prices.csv', sep=';')
    df_fin_volumes = df_fin_volumes.sort_index()  # сортируем DF по датам по возрастанию
    df_fin_volumes.to_csv('Historic_volumes.csv', sep=';')

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

    df_fin_close_prices = pd.read_csv('Historic_close_prices.csv', sep=';', index_col=0)
    df_price_std = pd.DataFrame()  # пустой DF
    for i in df_all_figi:
        df = df_fin_close_prices[i].dropna()  # получаем для каждого figi его DF с close_prices без пустых ячеек
        std = df.tail(std_period).pct_change().std().round(3)  # считаем стандартное отклонение
        df_price_std.loc[i, "std"] = std  # сохраняем стандартное отклонение в DF
    df_price_std.to_csv('STD.csv', sep=';')

    return df_price_std


def calc_sma():
    # FUNC получает на вход массив figi
    # на выходе DataFrame с подсчётами "скользящего среднего" для каждого figi

    df_fin_close_prices = pd.read_csv('Historic_close_prices.csv', sep=';', index_col=0)
    df_sma_final = pd.DataFrame()  # пустой DF
    df_sma2 = pd.DataFrame()  # пустой DF
    df_amount_of_sma = pd.DataFrame(columns=['amount_of_sma_rows'])  # пустой DF

    for i in df_all_figi:
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

    df_amount_of_sma.to_csv('amount_SMA.csv', sep=';')
    df_sma_final.sort_index()
    df_sma_final.to_csv('SMA.csv', sep=';')

    return df_amount_of_sma, df_sma_final


def sma_cross(actual_short_sma,
              actual_long_sma,
              df_previous_sma_saving,
              figi,
              last_price,
              df_signals):
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

    # если условие выполняется, то записываем данные в CSV
    if crossing_sell and df_signals.loc[figi].sell_flag != 1:
        df_one_signal = pd.DataFrame([[figi, datetime.now(), last_price, 1, 0, 'SMA']],
                                     columns=['figi',
                                              'datetime',
                                              'last_price',
                                              'sell_flag',
                                              'buy_flag',
                                              'strategy_id'])
        df_one_signal.set_index('figi')
        df_signals.append(df_one_signal)
    if crossing_buy and df_signals.loc[figi].buy_flag != 1:
        df_one_signal = pd.DataFrame([[figi, datetime.now(), last_price, 0, 1, 'SMA']],
                                     columns=['figi',
                                              'datetime',
                                              'last_price',
                                              'sell_flag',
                                              'buy_flag',
                                              'strategy_id'])
        df_one_signal.set_index('figi')
        df_signals.append(df_one_signal)


def calc_one_signal(df_all_lasts, df_previous_sma_saving, n, df_signals):
    # FUNC позволяет распарсить CSV 'SMA' и получить исторические SMA, далее по ластам считает актуальные SMA ежеминутно
    # просчитывает все figi
    # На входе df_all_figi, df_all_historic_sma
    # На выходе актуальные SMA_short, SMA_long, а также SMA_short, SMA_long за минуту назад
    # вспомогательная функция для def calc_signals

    df_all_historic_sma = pd.read_csv('SMA.csv', sep=';', index_col=0)
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
                df_previous_sma_saving.loc[i] = [actual_short_sma, actual_long_sma]

                sma_cross(actual_short_sma,
                          actual_long_sma,
                          df_previous_sma_saving,
                          i,
                          last_price,
                          df_signals)


def calc_and_save_actual_signals():
    # функция позволяет циклически считать сигналы на основе постоянно обновляющихся last_prices

    df_previous_sma_saving = pd.DataFrame(index=df_all_figi, columns=['previous_short_sma', 'previous_long_sma'])
    for n in tqdm(range(999999), desc='calculating signals'):
        df_all_lasts = get_all_lasts()
        if not os.path.exists('Actual_signals_SMA.csv'):
            df_signals = pd.DataFrame(
                columns=['figi', "datetime", 'last_price', 'sell_flag', 'buy_flag', 'strategy_id'])
            df_signals.set_index('figi')
        else:
            df_signals = pd.read_csv('Actual_signals_SMA.csv', sep=';', index_col=0)
        calc_one_signal(df_all_lasts, df_previous_sma_saving, n, df_signals)
        df_signals.to_csv('Actual_signals_SMA.csv', sep=';')
        time.sleep(15)


def historic_sma_cross(historic_short_sma,
                       historic_long_sma,
                       previous_historic_short_sma,
                       previous_historic_long_sma,
                       figi,
                       historic_last_price,
                       date,
                       df_historic_signals,
                       index_of_row):
    # функция считает, пересекаются ли скользяшки, а далее формирует сигнал

    crossing_buy = ((historic_short_sma > historic_long_sma) & (
            previous_historic_short_sma < previous_historic_long_sma) & (historic_last_price > historic_long_sma))
    crossing_sell = ((historic_short_sma < historic_long_sma) & (
            previous_historic_short_sma > previous_historic_long_sma) & (historic_last_price < historic_long_sma))

    if crossing_sell:
        df_historic_signals.loc[f'{figi}{index_of_row}'] = [figi, date, historic_last_price, 1, 0, 'SMA']
    if crossing_buy:
        df_historic_signals.loc[f'{figi}{index_of_row}'] = [figi, date, historic_last_price, 0, 1, 'SMA']


def calc_one_historic_signal(i,
                             df_fin_close_prices,
                             df_all_historic_sma,
                             amount_of_rows,
                             df_historic_signals):
    for index_of_row in range(-1, -amount_of_rows, -1):

        # ниже получаем данные о исторических SMA из CSV
        figi = i[:12]

        df_historic_short_sma = df_all_historic_sma[f'{figi}.short'].dropna()  # подготовка DF с short_SMA по figi
        if df_historic_short_sma.size != 0:  # проверка на пустой DF
            historic_short_sma = df_historic_short_sma.loc[df_historic_short_sma.index[index_of_row]]
            previous_historic_short_sma = df_historic_short_sma.loc[df_historic_short_sma.index[index_of_row - 1]]
            # print('historic_short_sma:', historic_short_sma, 'date:', df_historic_short_sma.index[number + 151])
            # print('previous_historic_short_sma', previous_historic_short_sma, 'date:', df_historic_short_sma.index[number + 150])
        else:
            historic_short_sma = False

        df_historic_long_sma = df_all_historic_sma[f'{figi}.long'].dropna()  # подготовка DF с long_SMA по figi
        if df_historic_long_sma.size != 0:  # проверка на пустой DF
            historic_long_sma = df_historic_long_sma.loc[df_historic_long_sma.index[index_of_row]]
            previous_historic_long_sma = df_historic_long_sma.loc[df_historic_long_sma.index[index_of_row - 1]]
            # print('historic_long_sma', historic_long_sma, 'date:', df_historic_long_sma.index[number + 1])
            # print('previous_historic_long_sma', previous_historic_long_sma, 'date:', df_historic_long_sma.index[number])
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
                           df_historic_signals,
                           index_of_row)


def calc_historic_signals():
    # функция позволяет циклически считать сигналы на основе постоянно обновляющихся last_prices

    df_fin_close_prices = pd.read_csv('Historic_close_prices.csv', sep=';', index_col=0, parse_dates=[0])
    # df_previous_sma_saving = pd.DataFrame(index=df_all_figi, columns=['previous_short_sma', 'previous_long_sma'])
    # TODO удалить
    df_all_historic_sma = pd.read_csv('SMA.csv', sep=';', index_col=0)
    df_amount_of_sma = pd.read_csv('amount_SMA.csv', sep=';', index_col=0, parse_dates=[0])
    df_historic_signals = pd.DataFrame(columns=['figi',
                                                'datetime',
                                                'last_price',
                                                'sell_flag',
                                                'buy_flag',
                                                'strategy_id'])
    for x in tqdm(range(len(df_all_historic_sma.columns[::2])), desc='calculating_historic_signals'):
        i = df_all_historic_sma.columns[::2][x]
        amount_of_rows = df_amount_of_sma.amount_of_sma_rows[i[:12]]
        calc_one_historic_signal(i,
                                 df_fin_close_prices,
                                 df_all_historic_sma,
                                 amount_of_rows,
                                 df_historic_signals)
    df_historic_signals.set_index('figi')
    df_historic_signals.droplevel(0, axis='columns')
    df_historic_signals.to_csv('Historic_signals_SMA.csv', sep=';')


# TODO допилить функцию
def calc_profit():
    pd.read_csv('Historic_signals_SMA.csv', sep=';', index_col=0)


# первоначальные настройки
token = os.environ['INVEST_TOKEN']  # персональный токен

# настройка стратегии (здесь можно ничего не трогать)
std_period = 20  # дней (обязатльено меньше sma_short_period)
period_of_short_sma = 50  # дней (обязатльено меньше sma_long_period)
period_of_long_sma = 200  # дней

# подготовка исторических данных
df_all_figi = get_shares_list_to_csv()
print('Downloading historic candles:')
# create_2_csv_with_historic_candles()
print('Downloading success')

# подготовка исторических индикаторов
# calc_std()
print('Calc of STD done')
print('Calc of SMA starts:')
# calc_sma()
print('Calc of SMA done')
print('Data saving complete')

# подготовка исторических сигналов
calc_historic_signals()
print('Historic_signals_are_saved')

# подготовка реальных сигналов
# calc_and_save_actual_signals()
