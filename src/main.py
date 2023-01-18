import time
import polars as pl
import pandas as pd 
import numpy as np 
import seaborn as sns
import streamlit as st
import matplotlib.pyplot as plt
sns.set_theme()
plt.style.use("seaborn-dark-palette")

from utils.preprocess import generate_df, prepare_atm, prepare_ewallet, prepare_qris, prepare_edc
from utils.detect import predict_fraud

st.set_page_config(layout="wide")
st.title("Fraud Detection System (DEMO) :mag:")
st.text("Due to load time, only 5.000.000 rows will be processed.")

PAGE1 = "Transaction (Sample Data [LAMA])"
PAGE2 = "Transaction (Sample Data [BARU])"
# PAGE3 = "Transaction (Upload File)"

#page = st.sidebar.selectbox('Select page', [PAGE1, PAGE2, PAGE3])
page = st.sidebar.selectbox('Select page', [PAGE1, PAGE2])

if page == PAGE1:
    st.subheader("Load Sample Data")
    load_button = st.button("Load Data and Perform Predictions")
    data_path = '/data/fact_transaksi_echannel_jakone_mobile_2022.csv'

if page == PAGE2:
    st.subheader("Load Sample Data")
    load_button = st.button("Load Data and Perform Predictions")
    data_path = '/data/fact_transaksi_echannel_jakone_mobile.csv'
        
# elif page == PAGE3:
#     st.subheader("Load Data (File)")
#     data_path = st.file_uploader("Upload ATM Transaction Records Here", accept_multiple_files=False)
#     load_button = st.button("Load Data and Perform Predictions")

if load_button:
    # Step 1: load data
    with st.spinner('Loading Data, please wait a moment...'): 
        df_trx = generate_df(data_path)

    n = len(df_trx) if df_trx is not None else 0
    min_date = df_trx.select(pl.col('waktutrx').min())
    max_date = df_trx.select(pl.col('waktutrx').max())

    st.subheader(f":airplane_departure: Tanggal mulai transaksi : {min_date[0,0]}")
    st.subheader(f":airplane_arriving: Tanggal akhir transaksi : {max_date[0,0]}")
    st.subheader(f":white_check_mark: Data Loaded! ({n} entries found)")
    st.dataframe(df_trx.head(10).to_pandas())

    # Case ATM
    if page==PAGE1:
        st.title(':desktop_computer:1. Channel : ATM')
        st.dataframe(df_trx.filter(pl.col('channel')=='ATM').head().to_pandas())
        # Step 2: preprocessing data
        with st.spinner('Preprocessing data, please wait a moment...'):
            df_atm_withdraw = prepare_atm(df_trx, channel='ATM', trx_type='Tarik Tunai', duration='10m')
            st.subheader(':loudspeaker: A. Skenario: Penarikan uang > 4 kali di beberapa ATM dalam jangka waktu 10 menit')
            st.dataframe(df_atm_withdraw.filter(pl.col('jumlah_terminal')>4).to_pandas())

            df_atm_inquiry = prepare_atm(df_trx, channel='ATM', trx_type='Informasi Saldo', duration='1d')
            st.subheader(':loudspeaker: B. Skenario: Inquiry saldo > 5 kali jangka waktu 1 hari')
            st.dataframe(df_atm_inquiry.filter(pl.col('jumlah_transaksi')>5).to_pandas())

            df_atm_transfer = prepare_atm(df_trx, channel='ATM', trx_type='Transfer', duration='1d')
            st.subheader(':loudspeaker: C. Skenario: Transfer uang > 4 kali ke bank berbeda jangka waktu 1 hari')
            st.dataframe(df_atm_transfer.filter(pl.col('jumlah_bank_tujuan')>4).to_pandas())
        
        st.header(':credit_card:2. Channel : EDC ')
        st.dataframe(df_trx.filter(pl.col('channel')=='EDC').head().to_pandas())
        # Step 2: preprocessing data
        with st.spinner('Preprocessing data, please wait a moment...'):
            df_edc_trx = prepare_atm(df_trx, channel='EDC', trx_type='Pembelian', duration='10m')
            st.subheader(':loudspeaker: A. Skenario: Jumlah transaksi > 5 kali dalam jangka waktu 10 menit')
            st.dataframe(df_edc_trx.filter(pl.col('jumlah_transaksi')>5).to_pandas())
            
            # df_edc_inquiry = prepare_edc(df_trx, channel='EDC', trx_type='Informasi Saldo', duration='1d')
            # st.subheader(':loudspeaker: B. Skenario: Inquiry saldo > 5 kali jangka waktu 1 hari')
            # st.dataframe(df_edc_inquiry.filter(pl.col('jumlah_transaksi')>0).to_pandas())
        

    elif page==PAGE2:
        st.header(':pager:1. Channel : Jakone Mobile')
        st.dataframe(df_trx.filter(pl.col('channel')=='Jakone Mobile').head().to_pandas())
        # Step 2: preprocessing data
        with st.spinner('Preprocessing data, please wait a moment...'):
            df_ewallet = prepare_ewallet(df_trx, 
                                        channel='Jakone Mobile', 
                                        trx_type='Billpayment', 
                                        duration='10m',
                                        ewallet_only=True)
            st.subheader(':loudspeaker: A. Skenario: Top up ke no tujuan selain no hp sumber secara berturut turut')
            st.dataframe(df_ewallet.filter(pl.col('jumlah_no_hp_tujuan')>3).to_pandas())

            st.subheader(':loudspeaker: B. Skenario: Total nominal transaksi > Rp 10.000.000')
            st.dataframe(df_ewallet.filter(pl.col('total_transaksi')>10000000).to_pandas())


        st.header(':camera:2. Channel : QRIS')
        st.dataframe(df_trx.filter(pl.col('jenistrx')=='Qris').head().to_pandas())
        with st.spinner('Preprocessing data, please wait a moment...'):
            df_qris = prepare_qris(df_trx, 
                                   channel='Jakone Mobile', 
                                   trx_type='Qris', 
                                   duration='10m')
            st.subheader(':loudspeaker: A. Skenario: Transaksi dilakukan secara berturut-turut')
            st.dataframe(df_qris.filter(pl.col('jumlah_transaksi')>5).to_pandas())