import math
import numpy as np
import pandas as pd
from elasticsearch import Elasticsearch

def index_combined_to_es():
    df = pd.read_parquet("/home/axoud/datalake/formatted/combined/20250610/cinema_combined.parquet")
    df = df.where(pd.notnull(df), None)

    es = Elasticsearch("http://localhost:9200")

    for _, row in df.iterrows():
        doc = row.to_dict()

        for key, value in doc.items():
            # Timestamp → ISO 8601
            if isinstance(value, pd.Timestamp):
                doc[key] = value.isoformat()
            # NaN → None
            elif isinstance(value, float) and math.isnan(value):
                doc[key] = None
            # NumPy floats and ints → native Python
            elif isinstance(value, (np.floating, np.integer)):
                doc[key] = value.item()

        es.index(index="cinema_combined", document=doc)
