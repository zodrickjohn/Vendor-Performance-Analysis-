import pandas as pd
import sqlite3
import logging
from ingestion_db import ingest_db

# Dedicated logger for this script 
logger = logging.getLogger("vendor_summary")
logger.setLevel(logging.DEBUG)

file_handler = logging.FileHandler("logs/get_vendor_summary.log", mode="a", encoding='utf-8')
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)

# Avoid adding multiple handlers if already added
if not logger.hasHandlers():
    logger.addHandler(file_handler)


def create_vendor_summary(conn):
    """Merge different tables to get overall vendor summary with new analytical columns."""
    vendor_sales_summary = pd.read_sql_query("""
    WITH freightsummary AS (
        SELECT VendorNumber, SUM(Freight) AS FreightCost
        FROM vendor_invoice
        GROUP BY VendorNumber
    ),
    purchasesummary AS (
        SELECT
            p.VendorNumber,
            p.VendorName,
            p.Brand,
            p.Description,
            p.PurchasePrice,
            pp.Price AS ActualPrice,
            pp.Volume,
            SUM(p.Quantity) AS TotalPurchaseQuantity,
            SUM(p.Dollars) AS TotalPurchaseDollars
        FROM purchases p
        JOIN purchase_prices pp ON p.Brand = pp.Brand
        WHERE p.PurchasePrice > 0
        GROUP BY p.VendorNumber, p.VendorName, p.Brand, p.Description,
                 p.PurchasePrice, pp.Price, pp.Volume
    ),
    salessummary AS (
        SELECT 
            VendorNo,
            Brand,
            SUM(SalesQuantity) AS TotalSalesQuantity,
            SUM(SalesDollars) AS TotalSalesDollars,
            SUM(SalesPrice) AS TotalSalesPrice,
            SUM(ExciseTax) AS TotalExciseTax
        FROM sales
        GROUP BY VendorNo, Brand
    )
    SELECT 
    ps.VendorNumber,
    ps.VendorName,
    ps.Brand,
    ps.Description,
    ps.PurchasePrice,
    ps.ActualPrice,
    ps.Volume,
    ps.TotalPurchaseQuantity,
    ps.TotalPurchaseDollars,
    ss.TotalSalesQuantity,
    ss.TotalSalesDollars,
    ss.TotalSalesPrice,
    ss.TotalExciseTax,
    fs.FreightCost
    FROM purchasesummary ps
    LEFT JOIN salessummary ss ON ps.VendorNumber = ss.VendorNo AND ps.Brand = ss.Brand
    LEFT JOIN freightsummary fs ON ps.VendorNumber = fs.VendorNumber
    ORDER BY ps.TotalPurchaseDollars DESC""", conn)

    return vendor_sales_summary


def clean_data(df):
    """Clean and enhance data for better analysis."""
    df['Volume'] = df['Volume'].astype('float')
    df.fillna(0, inplace=True)

    df['VendorName'] = df['VendorName'].str.strip()
    df['Description'] = df['Description'].str.strip()

    df['GrossProfit'] = df['TotalSalesDollars'] - df['TotalPurchaseDollars']
    df['ProfitMargin'] = (df['GrossProfit'] / df['TotalSalesDollars']) * 100
    df['StockTurnover'] = df['TotalSalesQuantity'] / df['TotalPurchaseQuantity']
    df['SalesPurchaseRatio'] = df['TotalSalesDollars'] / df['TotalPurchaseDollars']

    return df


if __name__ == '__main__':
    conn = sqlite3.connect('inventory.db')

    logger.info('Creating Vendor Summary Table...')
    summary_df = create_vendor_summary(conn)
    logger.info('\n%s', summary_df.head())

    logger.info('Cleaning Data...')
    clean_df = clean_data(summary_df)
    logger.info('\n%s', clean_df.head())

    logger.info('Ingesting data...')
    ingest_db(clean_df, 'vendor_sales_summary', conn)
    logger.info('Completed')

    clean_df.to_csv(r'C:\Users\Zodrick John\Downloads\vendor_sales_summary.csv', index=False)
    logger.info('CSV Exported to Downloads folder.')