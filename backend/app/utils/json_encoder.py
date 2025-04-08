# backend/app/utils/json_encoder.py
import re
import json
import math
from typing import Any

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj: Any) -> Any:
        if isinstance(obj, float):
            if math.isnan(obj):
                return None  # Convert NaN to null
            elif math.isinf(obj) and obj > 0:
                return "Infinity"
            elif math.isinf(obj) and obj < 0:
                return "-Infinity"
        return super().default(obj)

def custom_json_dumps(obj: Any) -> str:
    json_str = json.dumps(obj, cls=CustomJSONEncoder)
    json_str = re.sub(r':\s*NaN\s*([,}])', r': null\1', json_str)
    
    return json_str