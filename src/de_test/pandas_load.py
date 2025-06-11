import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
from argparse import ArgumentParser


class TaxPayersColumnsSchema:
    FIO = 'ФИО'
    PHONE = 'Телефон'
    SNILS = 'Snils'
    BDAY = 'Дата рождения'
    INN = 'INN'
    EMAIL = 'Email'
    ADDRESS = 'Адрес'

    @staticmethod
    def get_types_map():
        return {
            TaxPayersColumnsSchema.FIO: "string",
            TaxPayersColumnsSchema.PHONE: np.uint64, 
            TaxPayersColumnsSchema.SNILS: np.uint64,
            TaxPayersColumnsSchema.INN: np.uint64,
            TaxPayersColumnsSchema.EMAIL: "string",
            TaxPayersColumnsSchema.ADDRESS: "string",
            TaxPayersColumnsSchema.BDAY: "datetime64[ns]"
        }
    
    @staticmethod
    def get_column_names_map():
        return {
            TaxPayersColumnsSchema.FIO: "name",
            TaxPayersColumnsSchema.PHONE: "phoneNumber",
            TaxPayersColumnsSchema.SNILS: "snils",
            TaxPayersColumnsSchema.INN: "inn",
            TaxPayersColumnsSchema.EMAIL: "email",
            TaxPayersColumnsSchema.ADDRESS: "address",
            TaxPayersColumnsSchema.BDAY: "birthday"
        }
    
def write_parquet_in_chunks(data_iterator, file_path):
    writer = None
    for i, chunk in enumerate(data_iterator):
        chunk = preprocess_chunk(chunk)
        table = pa.Table.from_pandas(chunk)
        if not bool(i):
            writer = pq.ParquetWriter(file_path, table.schema, compression='SNAPPY')
        writer.write_table(table)

    if writer:
        writer.close()

def chunk_data_types_preprocess(chunk:pd.DataFrame):
    chunk[TaxPayersColumnsSchema.PHONE] = chunk[TaxPayersColumnsSchema.PHONE]\
        .astype('string')\
        .str.extract('(\d+)', expand=False)\
        .fillna(value='0')
    # chunk = chunk[chunk[TaxPayersColumnsSchema.PHONE].str.len() < 11] 
    chunk[TaxPayersColumnsSchema.BDAY] = pd.to_datetime(chunk[TaxPayersColumnsSchema.BDAY], errors='coerce')
    chunk[TaxPayersColumnsSchema.INN] = chunk[TaxPayersColumnsSchema.INN]\
        .astype('string')\
        .str.extract('(\d+)', expand=False)\
        .fillna(value='0')
    chunk[TaxPayersColumnsSchema.SNILS] = chunk[TaxPayersColumnsSchema.SNILS]\
        .astype('string')\
        .str.extract('(\d+)', expand=False)\
        .fillna(value='0')
    return chunk

def chunk_process_name(chunk:pd.DataFrame):
    chunk[['fname', 'mname', 'lname']] = chunk[TaxPayersColumnsSchema.FIO].str.split(' ', expand=True)
    return chunk

def chunk_email_normalization(chunk:pd.DataFrame):
    chunk[TaxPayersColumnsSchema.EMAIL] = chunk[TaxPayersColumnsSchema.EMAIL].str.lower()
    return chunk


def preprocess_chunk(chunk:pd.DataFrame):
    typeMap = TaxPayersColumnsSchema.get_types_map()
    # Drop garbage column 
    # Fills float nans in phone column with 0 to support int type. 
    # Drop any symbols from INN and SNILS
    # Filter phones, inn, snils regarding to its length
    # Manually process dates because of out of range error
    chunk = chunk.drop(columns=['Unnamed: 0'])
    chunk = chunk_data_types_preprocess(chunk)
    chunk = chunk_process_name(chunk)
    chunk = chunk_email_normalization(chunk)
    chunk = chunk.query(
        f'''
            {TaxPayersColumnsSchema.PHONE}.str.len() < 11 and\
            {TaxPayersColumnsSchema.INN}.str.len() < 11 and\
            {TaxPayersColumnsSchema.SNILS}.str.len() < 12
        '''
    )
    chunk = chunk.astype(typeMap)
    chunk = chunk[[
        'fname', 'mname', 'lname',
        TaxPayersColumnsSchema.BDAY,
        TaxPayersColumnsSchema.PHONE,
        TaxPayersColumnsSchema.SNILS,
        TaxPayersColumnsSchema.INN,
        TaxPayersColumnsSchema.EMAIL,
        TaxPayersColumnsSchema.ADDRESS 
    ]] # Reorder columns
    chunk = chunk.rename(TaxPayersColumnsSchema.get_column_names_map(), axis='columns')
    chunk = chunk.reset_index(inplace=False)
    return chunk

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("-i", "--inp", dest="filename",
                        help="Input txt file")
    parser.add_argument("-o", "--out", dest="outfile", default='./data/output.parquet',
                        help="Specifies output file for script")
    args = parser.parse_args()

    filename = args.filename
    chunksize = 10**5
    chunks = pd.read_csv(filename, on_bad_lines='skip', chunksize=chunksize)

    write_parquet_in_chunks(chunks, args.outfile)