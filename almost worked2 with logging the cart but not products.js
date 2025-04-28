function onOpen() {
  const ui = SpreadsheetApp.getUi();
  ui.createMenu('Upload Cart')
    .addItem('Upload Now', 'uploadSelectedFarmerCarts')
    .addItem('Admin: Update AuthToken', 'updateAuthToken')
    .addToUi();
}

function uploadSelectedFarmerCarts() {
  const ui = SpreadsheetApp.getUi();
  const props = PropertiesService.getScriptProperties();
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const todayDate = new Date().toDateString();

  const authToken = props.getProperty("authToken");
  const lastCheckedDate = props.getProperty("lastCheckedDate");

  if (lastCheckedDate !== todayDate) {
    ui.alert('❌ First upload attempt of the day.\n\nPlease ask the admin to update the authToken manually.');
    props.setProperty("lastCheckedDate", todayDate);
    return;
  }

  if (!authToken) {
    ui.alert('❌ AuthToken not updated yet.\n\nPlease ask admin to update it before uploading.');
    return;
  }

  const masterSheet = ss.getSheetByName("Extract 1");
  const toUploadSheet = ss.getSheetByName("ToUploadFarmers");
  const uploadedSheetName = "UploadedFarmers"; // NEW mapping sheet
  let uploadedSheet = ss.getSheetByName(uploadedSheetName);
  const statusSheet = ss.getSheetByName("FarmerCartStatus"); // For logging status

  if (!masterSheet || !toUploadSheet || !statusSheet) {
    ui.alert('❌ Required sheets not found.');
    return;
  }

  // Create UploadedFarmers sheet if not exist
  if (!uploadedSheet) {
    uploadedSheet = ss.insertSheet(uploadedSheetName);
    uploadedSheet.appendRow(["farmerId"]);
  }

  const uploadedData = uploadedSheet.getDataRange().getValues();
  const uploadedFarmerIds = new Set(uploadedData.slice(1).map(row => row[0].toString())); // Ignore header

  const masterData = masterSheet.getDataRange().getValues();
  const masterHeaders = masterData[0];
  const masterRows = masterData.slice(1);

  const toUploadData = toUploadSheet.getDataRange().getValues();
  const toUploadHeaders = toUploadData[0];
  const toUploadRows = toUploadData.slice(1);

  // Build farmerId -> cart mapping
  const masterIndex = {};
  masterRows.forEach(row => {
    const farmerId = row[masterHeaders.indexOf("farmerId")];
    if (farmerId) {
      if (!masterIndex[farmerId]) {
        masterIndex[farmerId] = [];
      }
      masterIndex[farmerId].push(row);
    }
  });

  // Find column indices
  const farmerIdColIndex = toUploadHeaders.indexOf("farmerId");
  let statusColIndex = toUploadHeaders.indexOf("cartUploadStatus");

  if (statusColIndex === -1) {
    toUploadHeaders.push("cartUploadStatus");
    toUploadSheet.getRange(1, 1, 1, toUploadHeaders.length).setValues([toUploadHeaders]);
    statusColIndex = toUploadHeaders.length - 1;
  }

  const newlyUploaded = [];

  // Process each farmerId
  toUploadRows.forEach((row, i) => {
    const farmerId = row[farmerIdColIndex];
    if (!farmerId) return;

    if (uploadedFarmerIds.has(farmerId.toString())) {
      toUploadSheet.getRange(i + 2, statusColIndex + 1).setValue("Already Uploaded");
      
      // Log the status in 'FarmerCartStatus' sheet
      statusSheet.appendRow([farmerId, "Already Uploaded", "", new Date()]);
      return;
    }

    const cartRows = masterIndex[farmerId];
    if (!cartRows || cartRows.length === 0) {
      toUploadSheet.getRange(i + 2, statusColIndex + 1).setValue("Not Found");
      
      // Log the status in 'FarmerCartStatus' sheet
      statusSheet.appendRow([farmerId, "Not Found", "", new Date()]);
      return;
    }

    const source = cartRows[0][masterHeaders.indexOf("source")];

    const cartData = {
      farmerId: farmerId,
      source: source,
      products: cartRows.map(r => ({
        productName: r[masterHeaders.indexOf("productName")],
        skuCode: r[masterHeaders.indexOf("skuCode")],
        quantity: parseInt(r[masterHeaders.indexOf("quantity")], 10),
        appliedOffer: r[masterHeaders.indexOf("appliedOffer")] || "",
        isAdding: true
      }))
    };

    const options = {
      method: "put",
      contentType: "application/json",
      headers: {
        "source": source,
        "x-authorization-token": authToken
      },
      payload: JSON.stringify(cartData),
      muteHttpExceptions: true
    };

    try {
      const res = UrlFetchApp.fetch("https://httpbin.org/put", options); // Replace with real API URL
      const status = res.getResponseCode();

      if (status === 200) {
        toUploadSheet.getRange(i + 2, statusColIndex + 1).setValue("Uploaded");
        newlyUploaded.push(farmerId);
        
        // Log the successful upload status in 'FarmerCartStatus' sheet
        const productDetails = cartData.products.map(p => `${p.productName} (${p.quantity})`).join(", ");
        statusSheet.appendRow([farmerId, "Uploaded", productDetails, new Date()]);
      } else {
        toUploadSheet.getRange(i + 2, statusColIndex + 1).setValue(`Failed ${status}`);
        
        // Log the failure status in 'FarmerCartStatus' sheet
        statusSheet.appendRow([farmerId, `Failed ${status}`, "", new Date()]);
      }
    } catch (e) {
      toUploadSheet.getRange(i + 2, statusColIndex + 1).setValue("Error");
      
      // Log the error status in 'FarmerCartStatus' sheet
      statusSheet.appendRow([farmerId, "Error", "", new Date()]);
    }
  });

  // Update UploadedFarmers sheet
  if (newlyUploaded.length > 0) {
    const lastRow = uploadedSheet.getLastRow();
    const newEntries = newlyUploaded.map(fid => [fid]);
    uploadedSheet.getRange(lastRow + 1, 1, newEntries.length, 1).setValues(newEntries);
  }

  ui.alert('✅ Upload completed!');
}

function updateAuthToken() {
  const props = PropertiesService.getScriptProperties();
  const ui = SpreadsheetApp.getUi();

  const response = ui.prompt('Admin', 'Enter new authToken:', ui.ButtonSet.OK_CANCEL);
  if (response.getSelectedButton() === ui.Button.OK) {
    const newToken = response.getResponseText();
    props.setProperty("authToken", newToken);
    ui.alert('✅ AuthToken updated successfully.');
  } else {
    ui.alert('❌ AuthToken update canceled.');
  }
}
