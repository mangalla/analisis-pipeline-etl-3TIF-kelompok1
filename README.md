# Analisis Pengaruh Format File dan Volume Data terhadap Waktu Eksekusi dan Penggunaan Memori Pipeline ETL Berbasis Python

Projek Statistika dan Probabilitas · Kelompok 1 · Kelas 3 TI F
Program Studi D4 Teknik Informatika · Politeknik Caltex Riau · 2026

| Nama | NIM |
| --- | --- |
| Elvina Fadhila Ramadani | 2455301219 |
| Muhammad Nawfal Ramadhan | 2455301229 |
| Ramos Hutahaean | 2455301235 |

## Ringkasan

Eksperimen faktorial penuh 3 format file (CSV, JSON, Parquet) × 5 volume data (10.000–1.000.000 baris) × 2 library (Pandas, Polars) × 10 ulangan = 300 run pipeline ETL. Setiap run mencatat waktu baca, transformasi, tulis, total waktu, memori puncak, dan throughput. Data dianalisis dengan statistik deskriptif, uji asumsi, ANOVA tiga arah, Kruskal-Wallis, Tukey HSD, Mann-Whitney U, serta korelasi dan regresi log-log.

## Isi repository

| File / folder | Keterangan |
| --- | --- |
| `jalankan_eksperimen.py` | Script utama: membuat data input, 30 run pemanasan, 300 run berurutan acak (seed 42), dan file Excel |
| `run_one.py` | Satu run pipeline ETL (Pandas/Polars) beserta pengukuran waktu dan memori; dipanggil oleh script utama |
| `requirements.txt` | Daftar library eksperimen |
| `hasil_mentah.csv` | Data asli 300 run sesuai urutan eksekusi + stempel waktu |
| `3TIF_01_PipelineETL.xlsx` | Dataset yang dikumpulkan (sheet Data, Keterangan_Variabel, Info_Dataset) |
| `lingkungan.json`, `identitas_eksperimen.json` | Spesifikasi laptop dan identitas pelaksana |
| `log_eksperimen.txt`, `log_pemanasan.csv` | Buku log eksperimen dan 30 run pemanasan |
| `hasil dari semua uji coba/` | Script analisis (`analisis_data.py`), tabel hasil uji, grafik, laporan, dan lampiran |

Folder `data_input/` dan `data_output/` tidak diunggah karena ukurannya lebih dari 1 GB. Isinya dibuat ulang secara identik oleh `jalankan_eksperimen.py` (seed 42).

## Cara menjalankan

```bash
python -m pip install -r requirements.txt
python jalankan_eksperimen.py --uji     # uji coba singkat, tidak masuk data
python jalankan_eksperimen.py           # eksperimen penuh: 30 pemanasan + 300 run + Excel
```

Analisis statistik (butuh tambahan `scipy`, `statsmodels`, `matplotlib`):

```bash
cd "hasil dari semua uji coba"
python analisis_data.py
```

## Lingkungan eksperimen

Lenovo LOQ · Intel Core i5-13450HX (10 inti fisik, 16 inti logis) · RAM 11,7 GB · Windows 11 · Python 3.13.15 · NumPy 2.5.3 · Pandas 3.0.6 · Polars 1.44.2 · PyArrow 25.0.1
