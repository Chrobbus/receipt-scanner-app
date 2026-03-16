# Receipt Scanner App

A simple Streamlit app that lets you upload or photograph a receipt, uses OCR to read the text, and extracts the total (and other amounts) into a table.

## What you need

- **Python 3.9 or newer** — check with: `python --version`
- **Tesseract OCR** — the `pytesseract` package is a wrapper; Tesseract itself must be installed on your system.

### Install Tesseract on Windows

1. Download the installer: [UB-Mannheim tesseract](https://github.com/UB-Mannheim/tesseract/wiki).
2. Run the installer and note the install path (e.g. `C:\Program Files\Tesseract-OCR`).
3. Add that folder to your system PATH, or in `app.py` set the path before running OCR:
   ```python
   pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'
   ```

## Setup

1. Open a terminal in the project folder:

   ```bash
   cd C:\Users\Daniel\receipt-scanner-app
   ```

2. Create and activate a virtual environment (recommended):

   ```bash
   python -m venv venv
   venv\Scripts\activate
   ```

3. Install Python dependencies:

   ```bash
   pip install -r requirements.txt
   ```

   Or install the main packages directly: `pip install streamlit pytesseract pillow`

## Run the app

From the same folder (with `venv` activated if you use it):

```bash
streamlit run app.py
```

Your browser should open to something like `http://localhost:8501`. If it doesn’t, open that URL yourself.

## How to use

1. Choose **Upload an image** or **Take a photo**.
2. Provide a clear image of the receipt (good light, minimal glare).
3. Wait for OCR to run; the app will show:
   - The receipt image
   - A table of extracted amounts (with the total highlighted)
   - Optional: raw OCR text in an expandable section

## Tips

- For best results, use a high-contrast image (e.g. dark text on light background).
- If the total isn’t detected, check the “Raw text from receipt” section to see what OCR read.
