# Custom Airflow Plugins

This repository contains some custom airflow plugins which provide useful operators not included in the main airflow release

## Installation
To use a plugin, all you need to do is copy the plugin file to ${AIRFLOW_HOME}/plugins on your airflow servers. Any specific instructions will be included in the comments in the plugin code

## Plugins

### GCS Check Plugin
This provides an operator that checks if a file exists on a GCS bucket. The result is pushed to an xcom variable so it can be used by other tasks

### SFTP to GCS Plugin
This provides an operator that downloads a file from an SFTP server and then uploads it to a GCS bucket

### Slack Webhook Plugin
This provides an operator that sends a message to slack using slack incoming webhooks. This can be used as an alternative to the slack operator bundled with airflow

## Licence
All code is licenced under the Apache licence. Please see [licence](LICENCE.md)
