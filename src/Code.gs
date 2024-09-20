// Define the connector configuration
function getConfig(request) {
  return {
    configParams: [
      {
        type: 'SELECT_SINGLE',
        name: 'dbtRegion',
        displayName: 'DBT Region',
        options: [
          { label: 'Global', value: 'https://semantic-layer.cloud.getdbt.com/api/graphql' },
          { label: 'EMEA', value: 'https://semantic-layer.emea.dbt.com/api/graphql' }
        ]
      },
      {
        type: 'TEXTINPUT',
        name: 'dbtEnvironmentId',
        displayName: 'dbt Environment ID',
        helpText: 'Enter your dbt Environment ID'
      },
      {
        type: 'TEXTINPUT',
        name: 'dbtAuthToken',
        displayName: 'dbt Auth Token',
        helpText: 'Enter your dbt Auth Token'
      }
    ]
  };
}

// Define the authentication method
function getAuthType() {
  return {
    type: 'KEY'
  };
}

// Check if the 3rd-party service credentials are valid
function isAuthValid() {
  return true;
}

// Check if the current user is an admin user
function isAdminUser() {
  return false;
}

// Fetch metrics and dimensions from the dbt Semantic Layer
function fetchMetricsAndDimensions(dbtRegion, dbtEnvironmentId, dbtAuthToken) {
  var url = dbtRegion;
  var query = `
    query GetMetrics($environmentId: BigInt!) {
      metrics(environmentId: $environmentId) {
        description
        name
        queryableGranularities
        type
        requiresMetricTime
        dimensions {
          description
          name
          type
        }
      }
    }
  `;

  var payload = {
    query: query,
    variables: {
      environmentId: parseInt(dbtEnvironmentId)
    }
  };

  var options = {
    method: 'post',
    contentType: 'application/json',
    headers: {
      'Authorization': 'Token ' + dbtAuthToken
    },
    payload: JSON.stringify(payload)
  };

  try {
    var response = UrlFetchApp.fetch(url, options);
    var json = JSON.parse(response.getContentText());
    if (json.errors) {
      Logger.log('Errors: ' + JSON.stringify(json.errors));
      throw new Error('Error fetching metrics and dimensions: ' + JSON.stringify(json.errors));
    }

    var metrics = json.data.metrics;
    var dimensionsMap = {};
    metrics.forEach(function (metric) {
      metric.dimensions.forEach(function (dimension) {
        if (!dimensionsMap[dimension.name]) {
          dimensionsMap[dimension.name] = dimension;
        }
      });
    });

    var dimensions = Object.values(dimensionsMap);
    return { metrics: metrics, dimensions: dimensions };
  } catch (e) {
    Logger.log('Fetch metrics and dimensions error: ' + e.message);
    throw e;
  }
}

// Build the fields using fetched metrics and dimensions
function getFields(dbtRegion, dbtEnvironmentId, dbtAuthToken) {
  var data = fetchMetricsAndDimensions(dbtRegion, dbtEnvironmentId, dbtAuthToken);
  var fields = DataStudioApp.createCommunityConnector().getFields();
  var types = DataStudioApp.createCommunityConnector().FieldType;

  data.metrics.forEach(function (metric) {
    fields.newMetric()
      .setId(metric.name.toUpperCase()) // Convert to uppercase to match data keys
      .setName(metric.name)
      .setDescription(metric.description || '')
      .setType(types.NUMBER);
  });

  data.dimensions.forEach(function (dimension) {
    var dimensionType = types.TEXT;
    if (dimension.name.toLowerCase().includes('date') || dimension.name.toLowerCase().includes('time')) {
      dimensionType = types.YEAR_MONTH_DAY;
    }
    fields.newDimension()
      .setId(dimension.name.toUpperCase()) // Convert to uppercase to match data keys
      .setName(dimension.name)
      .setDescription(dimension.description || '')
      .setType(dimensionType);
  });

  return fields;
}

// Define the schema
function getSchema(request) {
  var dbtRegion = request.configParams.dbtRegion;
  var dbtEnvironmentId = request.configParams.dbtEnvironmentId;
  var dbtAuthToken = request.configParams.dbtAuthToken;
  var fields = getFields(dbtRegion, dbtEnvironmentId, dbtAuthToken);

  return { schema: fields.build() };
}

// Fetch and process the data
function getData(request) {
  var dbtRegion = request.configParams.dbtRegion;
  var dbtEnvironmentId = request.configParams.dbtEnvironmentId;
  var dbtAuthToken = request.configParams.dbtAuthToken;

  var requestedFieldIds = request.fields.map(function (field) { return field.name; });
  var fields = getFields(dbtRegion, dbtEnvironmentId, dbtAuthToken);
  var requestedFields = fields.forIds(requestedFieldIds);

  var metrics = requestedFields.asArray().filter(function (field) {
    return field.getType() === DataStudioApp.createCommunityConnector().FieldType.NUMBER;
  }).map(function (metric) {
    return { name: metric.getId() };
  });

  var groupBy = requestedFields.asArray().filter(function (field) {
    return field.getType() !== DataStudioApp.createCommunityConnector().FieldType.NUMBER;
  }).map(function (dimension) {
    return { name: dimension.getId() };
  });

  if (metrics.length === 0) {
    throw new Error('At least one metric must be selected.');
  }

  var queryId = createQuery(dbtRegion, dbtEnvironmentId, dbtAuthToken, metrics, groupBy);
  Logger.log('Query ID: ' + queryId);

  var queryResults = pollForResults(dbtRegion, dbtEnvironmentId, dbtAuthToken, queryId);
  Logger.log('Query Results: ' + JSON.stringify(queryResults));

  var rows = [];
  if (queryResults && queryResults.length > 0) {
    rows = queryResults.map(function (row) {
      return {
        values: requestedFieldIds.map(function (id) {
          return row[id];
        })
      };
    });
  }

  return {
    schema: requestedFields.build(),
    rows: rows
  };
}

// Create a query in the dbt Semantic Layer
function createQuery(dbtRegion, dbtEnvironmentId, dbtAuthToken, metrics, groupBy) {
  var url = dbtRegion;
  var query = `
    mutation CreateQuery($environmentId: BigInt!, $metrics: [MetricInput!]!, $groupBy: [GroupByInput!]) {
      createQuery(
        environmentId: $environmentId,
        metrics: $metrics,
        groupBy: $groupBy
      ) {
        queryId
      }
    }
  `;

  var payload = {
    query: query,
    variables: {
      environmentId: parseInt(dbtEnvironmentId),
      metrics: metrics,
      groupBy: groupBy
    }
  };

  var options = {
    method: 'post',
    contentType: 'application/json',
    headers: {
      'Authorization': 'Token ' + dbtAuthToken
    },
    payload: JSON.stringify(payload)
  };

  Logger.log('Create Query GraphQL: ' + JSON.stringify(payload));
  Logger.log('Create Query Headers: ' + JSON.stringify(options.headers));

  try {
    var response = UrlFetchApp.fetch(url, options);
    var responseText = response.getContentText();
    Logger.log('Create Query Response: ' + responseText);
    var json = JSON.parse(responseText);

    if (json.errors) {
      Logger.log('Errors: ' + JSON.stringify(json.errors));
      throw new Error('Error creating query: ' + JSON.stringify(json.errors));
    }

    return json.data.createQuery.queryId;
  } catch (e) {
    Logger.log('Create query error: ' + e.message);
    throw e;
  }
}

// Poll for query results
function pollForResults(dbtRegion, dbtEnvironmentId, dbtAuthToken, queryId) {
  var maxAttempts = 10;
  var pollInterval = 4000; // 4 seconds

  for (var attempt = 0; attempt < maxAttempts; attempt++) {
    Logger.log('Polling attempt ' + (attempt + 1) + ' of ' + maxAttempts);
    var result = fetchQueryResults(dbtRegion, dbtEnvironmentId, dbtAuthToken, queryId);

    if (result.status === 'SUCCESSFUL') {
      Logger.log('Query successful. Returning data.');
      return result.data;
    } else if (result.status === 'FAILED') {
      Logger.log('Query failed: ' + result.error);
      throw new Error('Query failed: ' + result.error);
    } else if (['QUEUED', 'RUNNING', 'COMPILED'].includes(result.status)) {
      Logger.log('Query still processing. Status: ' + result.status + '. Waiting before next attempt...');
      Utilities.sleep(pollInterval);
    } else {
      Logger.log('Unexpected query status: ' + result.status);
      throw new Error('Unexpected query status: ' + result.status);
    }
  }

  Logger.log('Query timed out after ' + maxAttempts + ' attempts');
  throw new Error('Query timed out after ' + maxAttempts + ' attempts');
}

function fetchQueryResults(dbtRegion, dbtEnvironmentId, dbtAuthToken, queryId) {
  var url = dbtRegion;
  var query = `
    query GetResults($environmentId: BigInt!, $queryId: String!) {
      query(environmentId: $environmentId, queryId: $queryId) {
        jsonResult
        error
        queryId
        sql
        status
      }
    }
  `;

  var payload = {
    query: query,
    variables: {
      environmentId: parseInt(dbtEnvironmentId),
      queryId: queryId
    }
  };

  var options = {
    method: 'post',
    contentType: 'application/json',
    headers: {
      'Authorization': 'Token ' + dbtAuthToken
    },
    payload: JSON.stringify(payload)
  };

  Logger.log('Fetch Query Results GraphQL: ' + JSON.stringify(payload));

  try {
    var response = UrlFetchApp.fetch(url, options);
    var responseText = response.getContentText();
    Logger.log('Fetch Query Results Response: ' + responseText);
    var json = JSON.parse(responseText);

    if (json.errors) {
      Logger.log('Errors: ' + JSON.stringify(json.errors));
      throw new Error('Error fetching query results: ' + JSON.stringify(json.errors));
    }

    if (json.data.query.error) {
      Logger.log('Query error: ' + json.data.query.error);
      throw new Error('Query error: ' + json.data.query.error);
    }

    var status = json.data.query.status;
    var jsonResult = json.data.query.jsonResult;

    if (status === 'SUCCESSFUL' && jsonResult) {
      // Decode the base64 encoded jsonResult
      var decodedBytes = Utilities.base64Decode(jsonResult);
      var decodedString = "";
      for (var i = 0; i < decodedBytes.length; i++) {
        decodedString += String.fromCharCode(decodedBytes[i]);
      }
      var parsedResult = JSON.parse(decodedString);
      Logger.log('Decoded and parsed result: ' + JSON.stringify(parsedResult));

      return {
        status: status,
        data: parsedResult.data,
        error: null
      };
    } else {
      Logger.log('Query not yet complete. Status: ' + status);
      return {
        status: status,
        data: null,
        error: json.data.query.error
      };
    }
  } catch (e) {
    Logger.log('Fetch query results error: ' + e.message);
    throw e;
  }
}
