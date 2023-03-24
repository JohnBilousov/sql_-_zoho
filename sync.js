// See the documentation for the tool:
//https://docs.google.com/document/d/1lEAadKGFtvXSMScVkRb4khX0MeIAv8o8dX3CbpEr2cU/edit?usp=sharing

const mysql = require("mysql");
const ZCRMRestClient = require("zcrmsdk");

// Define the central schema for the sync
// It already consists all fields listed in the documentation for the tool
// This schema can be defined as a JSON file for the general use of this package
//
const fields = [
  "email",
  "name",
  "surname",
  "language",
  "specializations",
  "comment",
  "speak_languages",
  "email_verified",
  "photo",
  "phone_call_availability",
  "alto_arrival_date",
  "job_experience",
  "date_of_birth",
  "curriculum",
  "preferred_contact_time_str",
  "phone",
  "nationality",
  "current_location",
  "interests",
]; // full list of all fields we need for sync (NOT UPDATED IN THE MAPPINGS)

// Define the mapping from each data source to the central schema
const mappings = {
  mysql: {
    email: "email",
    name: "name",
    updated_at: "updated_at", // updated_at and timestamp to apply the latest version of the field across all data sources
    timestamp: "updated_at", // the current issue here is that we actually may need the updated timestamp on each individual field
  },
  zoho: {
    email: "Email",
    name: "Last_Name",
    language: "Language",
    updated_at: "Modified_Time",
    timestamp: "Modified_Time",
  },
};

// Define the MySQL connection configuration
const mysqlConfig = {
  host: "localhost",
  user: "user",
  password: "password",
  database: "database",
};

// Define the Zoho CRM authentication configuration
const zohoConfig = {
  client_id: "client_id",
  client_secret: "client_secret",
  redirect_uri: "https://redirect-uri.com", // ??
  user_identifier: "user_identifier",
  refresh_token: "refresh_token",
};

// Connect to MySQL
const mysqlConnection = mysql.createConnection(mysqlConfig);
mysqlConnection.connect();

// Connect to Zoho CRM
ZCRMRestClient.initialize(zohoConfig);

function getLatestFromMySQL(email, field) {
  return new Promise((resolve, reject) => {
    const query = `SELECT ${field}, updated_at FROM users WHERE email = ? ORDER BY updated_at DESC LIMIT 1`;
    mysqlConnection.query(query, [email], (error, results) => {
      if (error) {
        reject(error);
      } else {
        resolve({
          value: results[0][field],
          timestamp: results[0]["updated_at"],
        });
      }
    });
  });
}

// Define a function to get the latest version of a field from Zoho CRM
function getLatestFromZoho(email, field) {
  return new Promise((resolve, reject) => {
    const input = {
      module: "Contacts",
      params: {
        email: email,
      },
    };
    ZCRMRestClient.API.GET(input)
      .then((response) => {
        const record = response.body.data[0];
        if (record) {
          resolve({
            value: record[field],
            timestamp: record["Modified_Time"],
          });
        } else {
          resolve(null);
        }
      })
      .catch((error) => {
        reject(error);
      });
  });
}

// Define a function to update a field in MySQL
function updateMySQL(email, field, value, timestamp) {
  const query = `INSERT INTO users (email, ${field}, updated_at) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE ${field} = ?, updated_at = ?`;
  const values = [email, value, timestamp, value, timestamp];
  mysqlConnection.query(query, values, (error) => {
    if (error) {
      console.error(error);
    }
  });
}

// Define a function to update a field in Zoho CRM
function updateZoho(email, field, value, timestamp) {
  const input = {
    module: "Contacts",
    body: [
      {
        Email: email,
        [field]: value,
        Modified_Time: timestamp,
      },
    ],
  };
  ZCRMRestClient.API.UPSERT(input)
    .then((response) => {
      console.log(`Zoho CRM: Updated ${field} for ${email}`);
    })
    .catch((error) => {
      console.error(error);
    });
}

// This is a syncer

// Define the main sync function
async function sync(email) {
  console.log(`Syncing data for ${email}`);

  // Get the latest version of each field from each data source
  const latest = {};
  // Define the main sync function
  for (const field of fields) {
    const value = latest[field];
    if (value !== null) {
      let latestTimestamp = null;
      let latestValue = null;

      // Check the MySQL timestamp
      if (mappings.mysql[field]) {
        const mysqlTimestamp = value.updated_at;
        if (!latestTimestamp || mysqlTimestamp > latestTimestamp) {
          latestTimestamp = mysqlTimestamp;
          latestValue = value.value;
        }
      }

      // Check the Zoho CRM timestamp
      if (mappings.zoho[field]) {
        const zohoTimestamp = new Date(value.updated_at).getTime();
        if (!latestTimestamp || zohoTimestamp > latestTimestamp) {
          latestTimestamp = zohoTimestamp;
          latestValue = value.value;
        }
      }

      // Update the data source with the latest value
      if (mappings.mysql[field]) {
        updateMySQL(email, mappings.mysql[field], latestValue);
      }
      if (mappings.zoho[field]) {
        updateZoho(email, mappings.zoho[field], latestValue);
      }
    }
  }
}

// Test the sync function
sync("example@example.com");
