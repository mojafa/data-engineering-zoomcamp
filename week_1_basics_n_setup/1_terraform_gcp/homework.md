## Week 1 Homework

In this homework we'll prepare the environment by creating resources in GCP with Terraform.

In your VM on GCP install Terraform. Copy the files from the course repo
[here](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_1_basics_n_setup/1_terraform_gcp/terraform) to your VM.

Modify the files as necessary to create a GCP Bucket and Big Query Dataset.


## Question 1. Creating Resources

After updating the main.tf and variable.tf files run:

```
terraform apply
```
```
var.project
  Your GCP Project ID

  Enter a value: smart-grin-376216

google_storage_bucket.data-lake-bucket: Refreshing state... [id=dtc_data_lake_smart-grin-376216]
google_bigquery_dataset.dataset: Refreshing state... [id=projects/smart-grin-376216/datasets/trips_data_all]

No changes. Your infrastructure matches the configuration.

Terraform has compared your real infrastructure against your configuration and found no differences, so no changes are needed.

Apply complete! Resources: 0 added, 0 changed, 0 destroyed.
```

Paste the output of this command into the homework submission form.


## Submitting the solutions

* Form for submitting: [form](https://forms.gle/S57Xs3HL9nB3YTzj9)
* You can submit your homework multiple times. In this case, only the last submission will be used.

Deadline: 26 January (Thursday), 22:00 CET
