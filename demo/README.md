# Local Demo

This folder contains a fully local end-to-end demo pipeline for `DP Flow`.

## Assets

- `scripts/generate_orders.py`: creates raw input data
- `scripts/transform_orders.py`: produces a curated dataset
- `scripts/build_report.py`: writes a JSON summary and markdown report
- `scripts/validate_outputs.py`: verifies the demo outputs
- `data/`: generated raw and curated data
- `output/`: generated reports

## Pipeline Config

The runnable pipeline config lives in:

- `pipelines/local_demo_pipeline.yaml`

## Run

```bash
python3 -m dataplatform.cli.main run pipelines/local_demo_pipeline.yaml
```
