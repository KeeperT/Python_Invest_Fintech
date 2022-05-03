import pandas as pd
from tqdm import trange, tqdm
import time
from datetime import datetime, timedelta
import datetime as dt
from tinkoff.invest import *
from tinkoff.invest.services import Services
import numpy
import csv
import pathlib
import os


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


def sma_cross(actual_short_sma,
                 actual_long_sma,
                 df_previous_sma_saving,
                 figi,
                 last_price):
    # функция считает, пересекаются ли скользяшки, а далее формирует сигнал

    previous_short_sma_2 = df_previous_sma_saving.loc[figi].previous_short_sma
    previous_long_sma_2 = df_previous_sma_saving.loc[figi].previous_long_sma
    crossing_buy = ((actual_short_sma > actual_long_sma) & (previous_short_sma_2 < previous_long_sma_2) & (last_price > actual_long_sma))
    crossing_sell = ((actual_short_sma < actual_long_sma) & (previous_short_sma_2 > previous_long_sma_2) & (last_price < actual_long_sma))
    if crossing_sell:
        print(figi, 'SELL')
    if crossing_buy:
        print(figi, 'BUY')


def calc_one_signal(df_all_lasts, df_previous_sma_saving, n):
    # FUNC позволяет распарсить CSV 'SMA' и получить исторические SMA, далее по ластам считает актуальные SMA ежеминутно
    # На входе df_all_figi, df_all_historic_sma
    # На выходе актуальные SMA_short, SMA_long, а также SMA_short, SMA_long за минуту назад

    df_all_historic_sma = pd.read_csv('SMA.csv', sep=';', index_col=0)
    for i in df_all_historic_sma.columns[::2]:

        # ниже получаем данные о исторических SMA из CSV
        i = i[:12]
        df_historic_short_sma = df_all_historic_sma[f'{i}.short'].dropna()  # подготовка DF с short_SMA по figi

        if df_historic_short_sma.size != 0:  # проверка на пустой DF
            historic_short_sma = df_historic_short_sma.loc[df_historic_short_sma.index.max()]
        else:
            historic_short_sma = False

        df_historic_long_sma = df_all_historic_sma[f'{i}.long'].dropna()  # подготовка DF с long_SMA по figi

        if df_historic_long_sma.size != 0:  # проверка на пустой DF
            historic_long_sma = df_historic_long_sma.loc[df_historic_long_sma.index.max()]
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
                previous_short_sma = ((historic_short_sma * (period_of_short_sma - 1) + last_price) / period_of_short_sma).round(3)
                previous_long_sma = ((historic_long_sma * (sma_long_period - 1) + last_price) / sma_long_period).round(3)
                df_previous_sma_saving.loc[i] = [previous_short_sma, previous_long_sma]

            else:

                actual_short_sma = ((historic_short_sma * (period_of_short_sma - 1) + last_price) / period_of_short_sma).round(3)
                actual_long_sma = ((historic_long_sma * (sma_long_period - 1) + last_price) / sma_long_period).round(3)
                df_previous_sma_saving.loc[i] = [actual_short_sma, actual_long_sma]

                sma_cross(actual_short_sma,
                          actual_long_sma,
                          df_previous_sma_saving,
                          i,
                          last_price)


def calc_signals():
    # функция позволяет циклически считать сигналы на основе постоянно обновляющихся last_prices

    df_previous_sma_saving = pd.DataFrame(index=df_all_figi, columns=['previous_short_sma', 'previous_long_sma'])
    for n in tqdm(range(999999), desc='calculating signals'):
        df_all_lasts = get_all_lasts()
        calc_one_signal(df_all_lasts, df_previous_sma_saving, n)
        time.sleep(15)

# изначальная настройка
token = os.environ['INVEST_TOKEN']
df_all_figi = pd.read_csv('shares.csv', sep=';')['figi']  # формирование маcсива figi-акций
period_of_short_sma = 50  # дней (обязатльено меньше sma_long_period)
sma_long_period = 200  # дней

# подсчёт данных
calc_signals()
