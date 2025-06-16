import sqlite3
import pandas as pd
import logging
import os
from ingestion_db import ingest_db


# Get absolute path of current script
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Ensure logs directory exists
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

# Full log file path
LOG_FILE = os.path.join(LOG_DIR, "get_vendor_summary.log")

# Create logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Prevent adding handlers multiple times (important in Jupyter or imports)
if not logger.handlers:
    file_handler = logging.FileHandler(LOG_FILE, mode='a')
    file_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

# Optional: Also log to console for quick debugging
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

logger.info(f"Logging to {LOG_FILE}")


def create_vendor_summary(conn):
    '''this function will merge the different tables to get the overall vendor summary and adding new columns in the resultant data'''
    vendor_sales_summary = pd.read_sql_query("""WITH FreightSummary AS(
                                            SELECT VendorNumber, SUM(Freight) AS FreightCost
                                            FROM vendor_invoice
                                            GROUP BY VendorNumber),
                                            PurchaseSummary AS (
                                            SELECT p.VendorNumber, p.VendorName, p.Brand, p.Description, p. PurchasePrice,
                                            pp.Price as ActualPrice, pp.Volume, SUM(p.Quantity) AS TotalPurchaseQuantity,
                                            SUM(p.Dollars) AS TotalPurchaseDollars
                                            FROM purchases p JOIN purchase_prices pp ON p.Brand=pp.Brand
                                            WHERE p.PurchasePrice>0
                                            GROUP BY p.VendorNumber, p.VendorName, p.Brand, p.Description, p. PurchasePrice,
                                            pp.Price, pp.Volume),
                                            SalesSummary AS(
                                            SELECT VendorNo, Brand, SUM(SalesQuantity) AS TotalSalesQuantity, 
                                            SUM(SalesDollars) AS TotalSalesDollars, SUM(SalesPrice) AS TotalSalesPrice,
                                            SUM(ExciseTax) AS TotalExciseTax 
                                            FROM sales
                                            GROUP BY VendorNo, Brand
                                            )
                                            
                                            SELECT ps.VendorNumber, ps.VendorName, ps.Brand, ps.Description, ps.PurchasePrice,
                                            ps.ActualPrice, ps.Volume, ps.TotalPurchaseQuantity, ps.TotalPurchaseDollars,
                                            ss.TotalSalesQuantity, ss.TotalSalesDollars, ss.TotalSalesPrice, ss.TotalExciseTax,
                                            fs.FreightCost
                                            FROM PurchaseSummary ps
                                            LEFT JOIN SalesSummary ss 
                                                ON ps.VendorNumber=ss.VendorNo AND ps.Brand=ss.Brand
                                            LEFT JOIN FreightSummary fs
                                                ON ps.VendorNumber=fs.VendorNumber
                                            ORDER BY ps.TotalPurchaseDollars DESC""",conn)
    return vendor_sales_summary

def clean_data(df):
    '''this function will clean the data'''
    # changing datatype to float 
    df['Volume'] = df['Volume'].astype('float64')
    
    # filling missing value with 0
    df.fillna(0, inplace=True)
    
    # removing spaces from categorical columns
    df['VendorName'] = df['VendorName'].str.strip()
    df['Description'] = df['Description'].str.strip()
    
    # creating new columns for better analysis
    df['GrossProfit'] = df['TotalSalesDollars'] - df['TotalPurchaseDollars']
    df['ProfitMargin'] = (df['GrossProfit'] / df['TotalSalesDollars']) * 100
    df['StockTurnover'] = df['TotalSalesQuantity'] / df['TotalPurchaseQuantity']
    df['SalesToPurchaseRatio'] = df['TotalSalesDollars'] / df['TotalPurchaseDollars']
    
    return df


if __name__=='__main__':
    #creating database connection
    conn=sqlite3.connect('inventory.db')
    
    logger.info('Creating Vendor Summary Table....')
    summary_df=create_vendor_summary(conn)
    logger.info(summary_df.head())
    
    logger.info('Cleaning Data....')
    clean_df=clean_data(summary_df)
    logger.info(clean_df.head())
    
    logger.info('Ingesting Data....')
    ingest_db(clean_df,'vendor_sales_summary',conn)
    logger.info('Complete')