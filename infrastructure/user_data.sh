#!/bin/bash

set -ex
set -o pipefail

exec > >(tee /var/log/user_data.log | logger -t user_data) 2>&1
echo BEGIN
BEGINTIME=$(date +%s)
date "+%Y-%m-%d %H:%M:%S"

yum update -y
amazon-linux-extras install -y epel
yum-config-manager --enable epel*
yum install -y git python3 gcc postgresql-devel python3-devel.x86_64 tor

cat << EOT >> /etc/tor/torrc
SOCKSPort 9050
ControlPort 9051
EOT

echo "export TOR_PASSWORD=ind*Ica69" >> /home/ec2-user/.bashrc
echo "export AWS_REGION=us-east-1" >> /home/ec2-user/.bashrc
echo "HashedControlPassword $(tor --hash-password "$TOR_PASSWORD" | grep --color=never 16:[A-Z0-9])" >> /etc/tor/torrc
systemctl restart tor

pip3 install bs4 torrequest awscli boto3

mkdir /home/ec2-user/.browsing
aws s3 sync s3://jschnab-test-bucket/browser_config/ /home/ec2-user/.browsing/

cat << EOF > /home/ec2-user/.browsing/browser.conf
[logging]
log_file = /home/ec2-user/.browsing/browser.log

[harvest]
archive_dir = /home/ec2-user/real-estate-scraping/nytimes/archives
key_prefix = real-estate/nytimes/harvest

[extract]
csv_path = /home/ec2-user/real-estate-scraping/nytimes/rentals.csv
csv_header = listing_type,property_type,burrough,neighborhood,address,zip,price,description,amenities,common_charges,monthly_taxes,days_listed,size,year_built,bedrooms,bathrooms,half_bathrooms,rooms,representative,agency

[sqs]
queue_url = https://sqs.us-east-1.amazonaws.com/941435572757/toparse.fifo

[s3]
bucket = jschnab-test-bucket
EOF

date "+%Y-%m-%d %H:%M:%S"
ENDTIME=$(date +%s)
echo "deployment took $((ENDTIME - BEGINTIME)) seconds"
echo END
