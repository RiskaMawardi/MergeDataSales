import pandas as pd
import os
import glob


folder_path = r'D:\DataFromPrincipal\DataEBLO\merge'
output_file = os.path.join(folder_path, 'eblo.xlsx')

headers = {
    "SHOPEE": [
        "NO", "BRAND", "No# Pesanan", "Status Pesanan", "Status Pembatalan/ Pengembalian",
        "No Resi", "Opsi Pengiriman", "Antar ke counter/ pick-up",
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
        "Username (Pembeli)", "Nama Penerima", "No# Telepon", "Alamat Pengiriman",
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
        "Total before platform Subsidy", "Order Amount", "Payment Method", "Weight(kg)",
        "Product Category", "Purchase Channel"
    ],
    "LAZADA": [
        "NO", "BRAND", "Order Item Id", "Lazada Id", "Seller SKU", "Lazada SKU", "Created at",
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
    ]
}

files = glob.glob(os.path.join(folder_path, '*.xlsx'))
combined_sheets = {}

for file_path in files:
    if os.path.basename(file_path).lower() == 'eblo.xlsx':
        continue 

    brand = os.path.splitext(os.path.basename(file_path))[0].split(' ')[0]
    print(f"\nProcessing: {brand}")
    xls = pd.ExcelFile(file_path)

    for sheet_name in xls.sheet_names:
        clean_name = sheet_name.strip().upper()

        if clean_name in ["TOKPED NEW", "TOKOPEDIA NEW", "TOKOPEDIA"]:
            clean_name = "TIKTOK"

        if clean_name in ["LAZADAA"]:
            clean_name = "LAZADA"
        try:
            df = pd.read_excel(xls, sheet_name=sheet_name, dtype=str, header=None)
            df.dropna(how='all', inplace=True)
            if df.empty:
                print(f"⚠️ Sheet {sheet_name} dari {brand} kosong, dilewati.")
                continue

            header_row = df.notna().sum(axis=1).idxmax()
            df.columns = df.iloc[header_row].astype(str).str.strip()
            df = df.iloc[header_row + 1:].copy()

            df.dropna(axis=1, how='all', inplace=True)
            df.dropna(how='all', inplace=True)

            if clean_name == 'SHOPEE' and 'No. Pesanan' in df.columns:
                before = len(df)
                df = df[df['No. Pesanan'].notna() & (df['No. Pesanan'].astype(str).str.strip() != '')]
                after = len(df)
                if before != after:
                    print(f"➡️ {before - after} baris tanpa 'No. Pesanan' dihapus dari {brand} (SHOPEE).")

            elif clean_name == 'TIKTOK' and 'Order ID' in df.columns:
                before = len(df)
                df = df[df['Order ID'].notna() & (df['Order ID'].astype(str).str.strip() != '')]
                after = len(df)
                if before != after:
                    print(f"➡️ {before - after} baris tanpa 'Order ID' dihapus dari {brand} ({clean_name}).")

            elif clean_name == 'LAZADA' and 'Order Item Id' in df.columns:
                before = len(df)
                df = df[df['Order Item Id'].notna() & (df['Order Item Id'].astype(str).str.strip() != '')]
                after = len(df)
                if before != after:
                    print(f"➡️ {before - after} baris tanpa 'Order Item Id' dihapus dari {brand} (LAZADA).")

            elif clean_name == 'TOKPED' and 'Invoice' in df.columns:
                before = len(df)
                df = df[df['Invoice'].notna() & (df['Invoice'].astype(str).str.strip() != '')]
                after = len(df)
                if before != after:
                    print(f"➡️ {before - after} baris tanpa 'Invoice' dihapus dari {brand} (TOKPED).")


            if df.empty:
                print(f"⚠️ {sheet_name} dari {brand} kosong setelah filter, dilewati.")
                continue

            if clean_name in headers:
                header_template = headers[clean_name]
                df = df.reindex(columns=header_template)

            if clean_name not in combined_sheets:
                combined_sheets[clean_name] = df
            else:
                combined_sheets[clean_name] = pd.concat([combined_sheets[clean_name], df], ignore_index=True)

        except Exception as e:
            print(f"⚠️ Gagal baca sheet {sheet_name} dari {brand}: {e}")

if combined_sheets:
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        for sheet_name, df in combined_sheets.items():
            df.to_excel(writer, sheet_name=sheet_name[:31], index=False)
    print(f"\n✅ File gabungan berhasil dibuat: {output_file}")
else:
    print("\n⚠️ Tidak ada data untuk digabungkan.")
