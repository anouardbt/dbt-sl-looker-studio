import base64
import pyarrow as pa
import pyarrow.ipc as ipc
import pandas as pd
import json

def to_arrow_table(byte_string: str) -> pd.DataFrame:
    # Decode the Base64 string and open as an Arrow stream
    with ipc.open_stream(base64.b64decode(byte_string)) as reader:
        arrow_table = pa.Table.from_batches(reader, reader.schema)
    
    # Convert to a Pandas DataFrame
    return arrow_table.to_pandas()

def arrow_to_json(request):
    # Parse the incoming request as JSON
    request_json = request.get_json(silent=True)
    arrow_result = request_json.get('arrowResult')

    if not arrow_result:
        return json.dumps({"error": "No arrowResult found."})

    try:
        # Convert the arrow result to a Pandas DataFrame
        df = to_arrow_table(arrow_result)
        
        # Convert the DataFrame to a JSON-friendly format
        return df.to_json(orient='records')
    except Exception as e:
        return json.dumps({"error": str(e)})
