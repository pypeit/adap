# adap

The files in this project run the PypeIt software package in a cloud instance.
- the data are stored in AWS.
- kubernetes manages the data processing.
- the cloud provider is called nautilus, and the yaml files that control jobs there are in nautilus_jobs
- data access often uses rclone to move files from one host to another (say from the AWS to the cloud computing.)
- config files contain information for PypeIt as well as rclone
- data are downloaded using a serious of scripts in the download_lib directory
