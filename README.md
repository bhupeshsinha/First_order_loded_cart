# 📋 Farmer Cart Upload Automation  
![Built with Google Apps Script](https://img.shields.io/badge/Built%20with-Google%20Apps%20Script-4285F4?logo=google&logoColor=white)  
![Status: Production Ready](https://img.shields.io/badge/Status-Production%20Ready-brightgreen)  
![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)

---

## 🚀 Overview

A simple, reliable Google Apps Script to **upload farmer cart data** from Google Sheets to an external API.

- Reads farmer and product data.
- Builds dynamic cart payloads.
- Sends `PUT` API requests.
- Tracks upload status and API responses.
- One-click upload via a custom Google Sheets menu.

---

## 📂 Sheet Structure

| Sheet Name         | Purpose                                                                          |
| ------------------ | -------------------------------------------------------------------------------- |
| `Extract 1`        | Master data: all farmer cart details.                                             |
| `ToUploadFarmers`  | Farmers pending upload (`farmerId` list).                                         |
| `FarmerCartStatus` | (Auto-created) Log sheet: upload results, product count, and API responses.       |

---

## 🛠 How It Works

1. **Custom Menu**: Adds a `Upload Cart` menu → one-click upload.
2. **Mapping**: Matches farmers between `Extract 1` and `ToUploadFarmers`.
3. **Status Management**:  
   - Already uploaded? → Skipped.  
   - Not found? → Marked as "Not Found".  
   - Successfully uploaded? → Marked as "Uploaded".
4. **Logging**: All actions logged in `FarmerCartStatus` sheet.

---

## 📡 API Request Details

- **Method**: `PUT`
- **Headers**:
  - `source`: Product source (dynamic).
  - `x-authorization-token`: Your API token.
- **Payload Format**:
  ```json
  {
    "farmerId": "string",
    "source": "string",
    "products": [
      {
        "productName": "string",
        "skuCode": "string",
        "quantity": number,
        "appliedOffer": "string",
        "isAdding": true
      }
    ]
  }
  ```
- **Example Endpoint**: `https://httpbin.org/put` (for safe testing)

---

## 🎯 Why This Approach?

- **Non-technical Friendly**: One menu click uploads everything.
- **Safe & Idempotent**: Skips already-uploaded farmers automatically.
- **Transparent**: Full upload logs with API responses.
- **Fast**: Optimized for minimal reads/writes inside the sheet.

---

## ⚙️ How to Set Up

1. Open your Google Sheet.
2. Go to **Extensions → Apps Script**.
3. Paste the provided script.
4. Reload the Google Sheet.
5. Use the new **Upload Cart** menu.

---

## 📈 Future Enhancements (Optional)

- [ ] Scheduled automatic uploads (via time-driven triggers).
- [ ] Retry on failure with exponential backoff.
- [ ] Email notifications after upload completion.
- [ ] External configuration file (for auth token, API URL).

---

## 📝 License

This project is licensed under the **MIT License**.  
Feel free to use, modify, and share it!

---

> Made with ❤️ using Google Apps Script
