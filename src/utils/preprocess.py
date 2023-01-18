import polars as pl
import pandas as pd

def generate_df(file_path, n_limit=5000000):
    df = pl.read_csv(file_path, sep='$', n_rows=n_limit, ignore_errors=True)

    df = df.with_column((pl.col('tgltrx').cast(str) + ' ' + 
                        pl.col('jamtrx').cast(str)).str.strptime(pl.Datetime, fmt="%Y-%m-%d %H:%M:%S").cast(pl.Datetime).alias('waktutrx'))
    df = df.sort(by=['waktutrx'])

    return df

def prepare_atm(df_, channel, trx_type, duration='10m'):
    df = df_.clone()
    if channel == 'Jakone Mobile':
        features = ['waktutrx', 'channel', 'respon', 'jenistrx', 'tipetrx', 
                    'sourceoffund', 'kdbanksumber', 'destinationaccount', 'kdbanktujuan', 
                    'nilai', 'namasumber', 'namatujuan', 
                    'src_bank_name', 'dst_bank_name', 'transaction_status', 'error_desc', 'device_info']
    else:
        features = ['waktutrx', 'channel', 'respon', 'jenistrx', 'tipetrx', 'norek',
                    'sourceoffund', 'kdbanksumber', 'deskripsi', 'terminal', 'jamtrx',
                    'nama_terminal', 'destinationaccount', 'kdbanktujuan', 
                    'nilai', 'namasumber', 'namatujuan']

    df = df.select(features)
    df = df.filter(pl.col('channel') == channel)

    df = df.filter(pl.col('jenistrx')==trx_type).groupby_dynamic('waktutrx', every=duration, closed='both', truncate=False, by='sourceoffund').agg([
        pl.col('deskripsi'),
        pl.col('tipetrx'),
        pl.col('jamtrx'),
        pl.col('kdbanktujuan').list().alias('kode_bank_tujuan'),
        pl.col('nama_terminal').count().alias('jumlah_terminal'),
        pl.col('respon').count().alias('jumlah_transaksi'),
        pl.col('kdbanktujuan').n_unique().alias('jumlah_bank_tujuan'),
        pl.col('nilai').sum().alias('total_transaksi')
    ]).sort(by=['waktutrx'])

    return df

def prepare_ewallet(df_, channel, trx_type, duration='10m', ewallet_only=False):
    df = df_.clone()
    if channel == 'Jakone Mobile':
        features = ['waktutrx', 'jamtrx', 'channel', 'respon', 'jenistrx', 'tipetrx', 
                    'sourceoffund', 'kdbanksumber', 'destinationaccount', 'kdbanktujuan', 
                    'nilai', 'namasumber', 'namatujuan', 'deskripsi',
                    'src_bank_name', 'dst_bank_name', 'transaction_status', 'error_desc', 'device_info']
    else:
        features = ['waktutrx', 'channel', 'respon', 'jenistrx', 'tipetrx', 'norek',
                    'sourceoffund', 'kdbanksumber', 'deskripsi', 'terminal',
                    'nama_terminal', 'destinationaccount', 'kdbanktujuan', 
                    'nilai', 'namasumber', 'namatujuan']

    df = df.select(features)
    df = df.filter(pl.col('channel') == channel)

    if ewallet_only:
        df = df.filter(((pl.col('tipetrx')=='Topup_Ovo') | (pl.col('tipetrx')=='Topup_Gopay')) & (pl.col('respon')=='Berhasil'))
        df = df.with_column(pl.col('deskripsi').str.replace('Top Up ', ''))

    df = df.filter(pl.col('jenistrx')==trx_type).groupby_dynamic('waktutrx', every=duration, closed='both', truncate=False, by='namasumber').agg([
        pl.col('deskripsi'),
        pl.col('jamtrx'),
        pl.col('tipetrx'),
        pl.col('respon'),
        pl.col('deskripsi').n_unique().alias('jumlah_no_hp_tujuan'),
        pl.col('respon').count().alias('jumlah_transaksi'),
        pl.col('nilai').sum().alias('total_transaksi')
    ]).sort(by=['waktutrx'])

    return df

def prepare_qris(df_, channel, trx_type, duration='10m'):
    df = df_.clone()
    if channel == 'Jakone Mobile':
        features = ['waktutrx', 'jamtrx','channel', 'respon', 'jenistrx', 'tipetrx', 
                    'sourceoffund', 'kdbanksumber', 'destinationaccount', 'kdbanktujuan', 
                    'nilai', 'namasumber', 'namatujuan', 'deskripsi',
                    'src_bank_name', 'dst_bank_name', 'transaction_status', 'error_desc', 'device_info']
    else:
        features = ['waktutrx', 'channel', 'respon', 'jenistrx', 'tipetrx', 'norek',
                    'sourceoffund', 'kdbanksumber', 'deskripsi', 'terminal',
                    'nama_terminal', 'destinationaccount', 'kdbanktujuan', 
                    'nilai', 'namasumber', 'namatujuan']

    df = df.select(features)
    df = df.filter((pl.col('channel') == channel) & (pl.col('respon')=='Berhasil'))

    df = df.filter(pl.col('jenistrx')==trx_type).groupby_dynamic('waktutrx', every=duration, closed='both', truncate=False, by='namasumber').agg([
        pl.col('namatujuan'),
        pl.col('jamtrx'),
        pl.col('respon'),
        pl.col('respon').count().alias('jumlah_transaksi'),
        pl.col('nilai').sum().alias('total_transaksi')
    ]).sort(by=['waktutrx'])

    return df

def prepare_edc(df_, channel, trx_type, duration='10m'):
    df = df_.clone()
    if channel == 'Jakone Mobile':
        features = ['waktutrx', 'jamtrx','channel', 'respon', 'jenistrx', 'tipetrx', 
                    'sourceoffund', 'kdbanksumber', 'destinationaccount', 'kdbanktujuan', 
                    'nilai', 'namasumber', 'namatujuan', 'deskripsi',
                    'src_bank_name', 'dst_bank_name', 'transaction_status', 'error_desc', 'device_info']
    else:
        features = ['waktutrx', 'channel', 'respon', 'jenistrx', 'tipetrx', 'norek',
                    'sourceoffund', 'kdbanksumber', 'deskripsi', 'terminal',
                    'nama_terminal', 'destinationaccount', 'kdbanktujuan', 
                    'nilai', 'namasumber', 'namatujuan', 'jamtrx']

    df = df.select(features)
    df = df.filter((pl.col('channel') == channel) & (pl.col('respon')=='Berhasil'))

    df = df.filter(pl.col('jenistrx')==trx_type).groupby_dynamic('waktutrx', every=duration, closed='both', truncate=False, by='namasumber').agg([
        pl.col('namatujuan'),
        pl.col('jamtrx'),
        pl.col('respon'),
        pl.col('respon').count().alias('jumlah_transaksi'),
        pl.col('nilai').sum().alias('total_transaksi')
    ]).sort(by=['waktutrx'])

    return df