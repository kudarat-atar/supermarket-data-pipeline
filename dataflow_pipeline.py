
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText
from apache_beam.io.gcp.bigquery import WriteToBigQuery
import csv
from datetime import datetime

PROJECT_ID = "project_id"
DATASET = "sales_dw"
INPUT_FILE = "gs://bucket_name/supermarket_analysis.csv"

class ParseCSV(beam.DoFn):
    def process(self, element):
        reader = csv.DictReader([element])
        for row in reader:
            yield row

def clean_row(row):
    try:
        return {
            "Invoice ID": row["Invoice ID"],
            "Branch": row["Branch"],
            "City": row["City"],
            "Customer type": row["Customer type"],
            "Gender": row["Gender"],
            "Product line": row["Product line"],
            "Payment": row["Payment"],
            "Date": datetime.strptime(row["Date"], "%m/%d/%Y").date().isoformat(),
            "Time": row["Time"],
            "Quantity": int(row["Quantity"]),
            "Unit price": float(row["Unit price"]),
            "Tax 5%": float(row["Tax 5%"]),
            "Sales": float(row["Sales"]),
        }
    except Exception:
        return None

def run():
    pipeline_options = PipelineOptions(
        runner="DataflowRunner",
        project=PROJECT_ID,
        temp_location="gs://bucket_name/temp",
        region="us-central1",
        save_main_session=True,
    )

    with beam.Pipeline(options=pipeline_options) as p:

        raw_data = (
            p
            | "Read CSV" >> ReadFromText(INPUT_FILE, skip_header_lines=1)
            | "Parse CSV" >> beam.ParDo(ParseCSV())
            | "Clean Rows" >> beam.Map(clean_row)
            | "Filter Nulls" >> beam.Filter(lambda x: x is not None)
        )

        # -------- Dimension Customer --------
        dim_customer = (
            raw_data
            | "Customer Fields" >> beam.Map(lambda r: (
                r["Customer type"], r["Gender"], r["Payment"]
            ))
            | "Distinct Customers" >> beam.Distinct()
            | beam.transforms.util.WithKeys(lambda x: None)
            | beam.Values()
            | beam.transforms.combiners.ToList()
            | beam.FlatMap(lambda rows: [
                {
                    "customer_id": i + 1,
                    "customer_type": r[0],
                    "gender": r[1],
                    "payment": r[2],
                }
                for i, r in enumerate(rows)
            ])
        )

        dim_customer | "Write dim_customer" >> WriteToBigQuery(
            f"{PROJECT_ID}:{DATASET}.dim_customer",
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_IF_NEEDED"
        )

        # -------- Dimension Product --------
        dim_product = (
            raw_data
            | "Product Fields" >> beam.Map(lambda r: (
                r["Branch"], r["City"], r["Product line"]
            ))
            | "Distinct Products" >> beam.Distinct()
            | beam.transforms.util.WithKeys(lambda x: None)
            | beam.Values()
            | beam.transforms.combiners.ToList()
            | beam.FlatMap(lambda rows: [
                {
                    "product_id": i + 1,
                    "branch": r[0],
                    "city": r[1],
                    "product_line": r[2],
                }
                for i, r in enumerate(rows)
            ])
        )

        dim_product | "Write dim_product" >> WriteToBigQuery(
            f"{PROJECT_ID}:{DATASET}.dim_product",
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_IF_NEEDED"
        )

        # -------- Fact Table --------
        fact_sales = (
            raw_data
            | "Prepare Fact" >> beam.Map(lambda r: {
                "sale_id": int(r["Invoice ID"].replace("-", "")[:9]),
                "product_id": None,
                "customer_id": None,
                "date": r["Date"],
                "time": r["Time"],
                "quantity": r["Quantity"],
                "unit_price": r["Unit price"],
                "tax": r["Tax 5%"],
                "sales": r["Sales"],
            })
        )

        fact_sales | "Write fact_sales" >> WriteToBigQuery(
            f"{PROJECT_ID}:{DATASET}.fact_sales",
            write_disposition="WRITE_APPEND",
            create_disposition="CREATE_IF_NEEDED"
        )

if __name__ == "__main__":
    run()
