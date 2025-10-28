import pandas as pd
import os
import glob
import re
from openpyxl import load_workbook
from openpyxl.utils.dataframe import dataframe_to_rows

# 📁 Folder tempat semua file Excel
folder_path = r'D:\DataFromPrincipal\DataEBLO\merge'
final_output = os.path.join(folder_path, 'EBLO.xlsx')

# ================= HEADER TEMPLATE PER MARKETPLACE =================
headers = {
    "SHOPEE": [
        "NO", "BRAND", "No. Pesanan", "Status Pesanan", "Status Pembatalan/ Pengembalian",
        "No. Resi", "Opsi Pengiriman", "Antar ke counter/ pick-up",
        "Pesanan Harus Dikirimkan Sebelum (Menghindari keterlambatan)",
        "Waktu Pengiriman Diatur", "Waktu Pesanan Dibuat", "Waktu Pembayaran Dilakukan",
        "SKU Induk", "Nama Produk", "Nomor Referensi SKU", "Nama Variasi",
        "Harga Sebelum Diskon", "Harga Setelah Diskon", "Jumlah", "Total Harga Produk",
        "Total Diskon", "Diskon Dari Penjual", "Diskon Dari Shopee", "Berat Produk",
        "Jumlah Produk di Pesan", "Total Berat", "Voucher Ditanggung Penjual",
        "Cashback Koin", "Voucher Ditanggung Shopee", "Paket Diskon",
        "Paket Diskon (Diskon dari Shopee)", "Paket Diskon (Diskon dari Penjual)",
        "Potongan Koin Shopee", "Diskon Kartu Kredit", "Ongkos Kirim Dibayar oleh Pembeli",
        "Estimasi Potongan Biaya Pengiriman", "Ongkos Kirim Pengembalian Barang",
        "Total Pembayaran", "Perkiraan Ongkos Kirim", "Catatan dari Pembeli", "Catatan",
        "Username (Pembeli)", "Nama Penerima", "No. Telepon", "Alamat Pengiriman",
        "Kota/Kabupaten", "Provinsi", "Waktu Pesanan Selesai"
    ],
    "TOKPED": [
        "Count", "BRAND", "Invoice", "Payment Date", "Order Status", "Product ID", "Product Name",
        "Quantity", "Stock Keeping Unit (SKU)", "Notes", "Price (Rp)", "Discount Amount (Rp)",
        "Subsidi Amount (Rp)", "Harga Jual (Rp)", "Customer Name", "Customer Phone", "Recipient",
        "Recipient Number", "Recipient Address", "Courier", "Shipping Price + fee (Rp)",
        "Insurance (Rp)", "Total Shipping Fee (Rp)", "Total Amount (Rp)", "AWB", "Jenis Layanan",
        "Bebas Ongkir", "Warehouse Origin", "Campaign Name"
    ],
    "TIKTOK": [
        "NO", "Nama Brand", "Order ID", "Tracking ID", "Cancellation Request", "Product Name",
        "Seller SKU", "Variation", "Quantity", "Paid Time", "Delivery Option", "Buyer Message",
        "Buyer Username", "Recipient", "Phone #", "Zipcode", "Country", "Province",
        "Regency and City", "Districts", "Villages", "Detail Address",
        "Unit Price", "Order Amount", "Payment Method", "Weight(kg)",
        "Product Category", "Purchase Channel"
    ],
    "LAZADA": [
        "NO", "Nama Brand", "Order Item Id", "Lazada Id", "Seller SKU", "Lazada SKU", "Created at",
        "Updated at", "Order Number", "Invoice Required", "Customer Name", "Customer Email",
        "National Registration Number", "Shipping Name", "Shipping Address", "Shipping Address2",
        "Shipping Address3", "Shipping Address4", "Shipping Address5", "Shipping Phone Number",
        "Shipping Phone Number2", "Shipping City", "Shipping Postcode", "Shipping Country",
        "Shipping Region", "Billing Name", "Billing Address", "Billing Address2",
        "Billing Address3", "Billing Address4", "Billing Address5", "Billing Phone Number",
        "Billing Phone Number2", "Billing City", "Billing Country", "Payment Method", "Paid Price",
        "Unit Price", "Shipping Fee", "Wallet Credits", "Item Name", "Variation",
        "CD Shipping Provider", "Shipping Provider", "Shipment Type Name",
        "Shipping Provider Type", "CD Tracking Code", "Tracking Code", "Tracking URL",
        "Shipping Provider (first mile)", "Tracking Code (first mile)", "Tracking URL (first mile)",
        "Promised shipping time", "Premium", "Status", "Cancel / Return Initiator", "Reason",
        "Reason Detail", "Editor", "Bundle ID", "Bundle Discount", "Refund Amount"
    ],
    "BLIBLI": [
        "NO", "Nama Brand", "No. Order", "No. Order Item", "No. Paket", "No. Awb",
        "Tanggal Order", "Nama Pemesan", "No. Tlp", "Kode SKU Blibli", "SKU Merchant",
        "SKU", "Nama Produk", "Total Barang", "Servis Logistik", "Kode Merchant",
        "Order Status", "Alamat", "Kota", "Provinsi", "Harga item pesanan",
        "Total harga item pesanan", "Total", "Catatan produk", "Tanggal pengiriman"
    ]
}

# ================= GABUNGKAN FILES =================
files = glob.glob(os.path.join(folder_path, '*.xlsx'))
combined_sheets = {}

for file_path in files:
    if os.path.basename(file_path).lower() in ['eblo.xlsx', 'eblo_clean.xlsx', 'eblo_clean_with_format.xlsx']:
        continue

    brand = os.path.splitext(os.path.basename(file_path))[0].split(' ')[0]
    print(f"\nProcessing: {brand}")
    xls = pd.ExcelFile(file_path)

    for sheet_name in xls.sheet_names:
        clean_name = sheet_name.strip().upper()

        # Normalisasi nama sheet
        if clean_name in ["TOKPED NEW", "TOKOPEDIA NEW", "TOKOPEDIA"]:
            clean_name = "TIKTOK"
        elif clean_name in ["LAZADAA", "LAZADA ", "LAZADA"]:
            clean_name = "LAZADA"
        elif clean_name in ["BLIBLI", "BLIBLII", "BLIBLI ", "BLIBLIII"]:
            clean_name = "BLIBLI"

        try:
            df = pd.read_excel(xls, sheet_name=sheet_name, dtype=str, header=None)
            df.dropna(how='all', inplace=True)
            if df.empty:
                continue

            header_row = df.notna().sum(axis=1).idxmax()
            df.columns = df.iloc[header_row].astype(str).str.strip()
            df = df.iloc[header_row + 1:].copy()
            df.dropna(axis=1, how='all', inplace=True)
            df.dropna(how='all', inplace=True)

            key_col = {
                'SHOPEE': 'No. Pesanan',
                'TOKPED': 'Invoice',
                'TIKTOK': 'Order ID',
                'LAZADA': 'Order Item Id',
                'BLIBLI': 'No. Order'
            }.get(clean_name)

            if key_col and key_col in df.columns:
                df = df[df[key_col].notna() & (df[key_col].astype(str).str.strip() != '')]

        
            if clean_name in headers:
                header_template = headers[clean_name]
                df = df.reindex(columns=header_template)

            # Gabung
            if clean_name not in combined_sheets:
                combined_sheets[clean_name] = df
            else:
                combined_sheets[clean_name] = pd.concat([combined_sheets[clean_name], df], ignore_index=True)

            print(f"✅ {sheet_name} dari {brand} berhasil ({len(df)} baris)")

        except Exception as e:
            print(f"⚠️ Gagal baca sheet {sheet_name} dari {brand}: {e}")

# ================= SIMPAN FILE GABUNGAN TANPA FORMAT =================
if combined_sheets:
    with pd.ExcelWriter(final_output, engine='openpyxl') as writer:
        for sheet_name, df in combined_sheets.items():
            df.to_excel(writer, sheet_name=sheet_name[:31], index=False)
    print(f"\n✅ File gabungan tanpa format disimpan ke: {final_output}")
else:
    print("\n⚠️ Tidak ada data yang digabungkan.")
    exit()

# ================= SALIN FORMAT DARI FILE TEMPLATE PERTAMA =================
template_file = files[0]
wb_template = load_workbook(template_file)
wb_clean = load_workbook(clean_file)

for sheet_name in wb_clean.sheetnames:
    ws_clean = wb_clean[sheet_name]

    # Gunakan format sheet yang paling mirip dari template
    ws_template = wb_template.active
    if sheet_name in wb_template.sheetnames:
        ws_template = wb_template[sheet_name]

    # Terapkan lebar kolom dan style header
    for col_idx, col in enumerate(ws_template.iter_cols(min_row=1, max_row=1), start=1):
        cell = col[0]
        if cell.has_style and col_idx <= ws_clean.max_column:
            ws_clean.cell(row=1, column=col_idx)._style = cell._style
        # Lebar kolom
        col_letter = cell.column_letter
        if col_letter in ws_template.column_dimensions:
            ws_clean.column_dimensions[col_letter].width = ws_template.column_dimensions[col_letter].width

wb_clean.save(final_output)
print(f"🎨 Format berhasil disalin dari template → {final_output}")
