# Notes for Developers

## Running the Pipeline

One way to test the pipeline during development is to run the demo_tiny dataset. You can do that locally using the `singularity` profile:

```bash
nextflow run main.nf -profile singularity --input ./assets/samplesheet.csv --outdir ./output --spark_cluster=true --spark_workers=2 --spark_worker_cores=4 --spark_gb_per_core=15
```

You can do the same thing by running the test profile with `-stub`:

```bash
nextflow run main.nf -profile singularity,test_full --outdir ./output
```

Use the `test` profile with `-stub` if you just want to make sure the pipeline structure works, without testing the actual image processing tools:

```bash
nextflow run main.nf -profile singularity,test --outdir ./output -stub
```

## Running Tests

Run the automated test like this:

```bash
PROFILE=singularity nextflow run ./tests/subworkflows/local/spark/cluster/main.nf -entry test_spark_cluster -c ./tests/config/nextflow.config
```

## Before pushing

Run these tools before pushing:

```bash
nf-core lint
prettier -w .
black .
```
